"""调度器：端点级隔离的投递运行时。

- 每个启用端点一个 Runner：独立调度线程、独立并行令牌、独立退避状态；
  某租户的接收方超时/限流/下线，只会让该 Runner 暂停，其他端点不受任何影响。
- 同一 object_key 严格保序（队头阻塞），不同 object_key 在并行度内并发。
- Supervisor 负责发现端点、回收过期租约、把 API 的事件入队实时踢给 Runner。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .db import Store
from .sender import backoff_delay, build_request, send

log = logging.getLogger("whub.dispatcher")


class DynamicLimit:
    """可热更新容量的信号量（租户可随时调整并行上限，立即生效）。"""

    def __init__(self, n: int):
        self._cv = threading.Condition()
        self._permits = n

    @property
    def permits(self) -> int:
        with self._cv:
            return self._permits

    def set(self, n: int) -> None:
        with self._cv:
            self._permits = n
            self._cv.notify_all()

    def try_acquire(self) -> bool:
        with self._cv:
            if self._permits > 0:
                self._permits -= 1
                return True
            return False

    def acquire(self) -> None:
        with self._cv:
            while self._permits <= 0:
                self._cv.wait()
            self._permits -= 1

    def release(self) -> None:
        with self._cv:
            self._permits += 1
            self._cv.notify_all()


class EndpointRunner:
    def __init__(self, endpoint_id: str, store: Store, pool: ThreadPoolExecutor,
                 cfg: "HubConfig"):
        self.eid = endpoint_id
        self.store = store
        self.pool = pool
        self.cfg = cfg
        ep = store.endpoint(endpoint_id)
        self.limit = DynamicLimit(ep["parallelism"] if ep else 1)
        self.wake = threading.Event()
        self.stop_event = threading.Event()
        # 已领取但未终态的投递数（含退避等待中的）；用于 claim 预算，
        # 避免同一 key 的后续投递被重复领取。
        self.active = 0
        # 当前持有并行令牌的发送数（退避睡眠会先让出令牌）。
        self.flying = 0
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self._loop, name=f"runner-{endpoint_id[:12]}",
                                       daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake.set()

    def kick(self) -> None:
        self.wake.set()

    # -- 调度循环 ------------------------------------------------------

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._tick()
            except Exception:
                log.exception("runner %s tick failed", self.eid)
            self.wake.wait(timeout=self.cfg.poll_interval)
            self.wake.clear()

    def _tick(self) -> None:
        ep = self.store.endpoint(self.eid)
        if ep is None or ep["disabled"]:
            return
        self.limit.set(ep["parallelism"])  # 热更新并行度
        with self._lock:
            # 领取预算 = 并行度 -（在飞 + 等待退避中），避免重复领取同一 key
            want = max(ep["parallelism"] - self.active, 0)
        tokens = 0
        for _ in range(want):
            if self.limit.try_acquire():
                tokens += 1
            else:
                break
        if not tokens:
            return
        rows = self.store.claim_due(self.eid, tokens)
        unused = tokens - len(rows)
        for _ in range(unused):
            self.limit.release()
        for row in rows:
            with self._lock:
                self.active += 1
                self.flying += 1
            self.pool.submit(self._run_with_token, row["id"])

    # -- 发送 ----------------------------------------------------------

    def _send_owned(self, delivery_id: str, reacquired: bool = False) -> None:
        """持有令牌的一次发送。令牌只覆盖“实际 HTTP 调用”，退避睡眠时让出。

        返回 True 表示计数已移交退避定时器（active 仍保留），调用方 finally
        不得再动计数/令牌；False 表示本次调用已终态结算。"""
        row = self.store.delivery(delivery_id)
        if row is None or row["status"] != "inflight":
            return False  # 已被 replay/skip 等人工操作改变路径
        if not reacquired and row["not_before"] > time.time():
            self._wait_then_send(delivery_id,
                                 max(row["not_before"] - time.time(), 0))
            return True
        ver = self.store.delivery_secret(self.eid, row["sig_version"])
        ev = self.store.event(row["event_id"])
        if ver is None or ev is None:
            self.store.mark_dead(delivery_id, 0, "version or event missing")
            return False
        req = build_request(
            row["target_url"], delivery_id=delivery_id,
            event_id=row["event_id"], endpoint_id=self.eid,
            kid=row["kid"], object_key=row["object_key"], seq=row["seq"],
            payload=ev["payload"], secret=ver["secret"])
        result = send(req, timeout=self.cfg.http_timeout)
        if result.ok:
            self.store.mark_succeeded(delivery_id, result.code or 200)
            self.kick()
            return False
        if result.retryable:
            delay = backoff_delay(
                row["fail_count"] + 1, base=self.cfg.backoff_base,
                cap=self.cfg.backoff_cap, retry_after=result.retry_after)
            self.store.mark_retry(
                delivery_id, result.code,
                result.error or f"HTTP {result.code}", time.time() + delay)
            log.info("endpoint %s delivery %s retryable (%s), backoff %.2fs",
                     self.eid, delivery_id, result.code or result.error, delay)
            self._wait_then_send(delivery_id, delay)
            return True
        self.store.mark_dead(
            delivery_id, result.code or 0,
            result.error or f"HTTP {result.code}")
        log.warning("endpoint %s delivery %s dead: %s",
                    self.eid, delivery_id, result.code)
        self.kick()
        return False

    def _run_with_token(self, delivery_id: str, reacquired: bool = False) -> None:
        """工作线程入口：包住一次发送的计数与令牌生命周期。"""
        suspended = False
        try:
            suspended = self._send_owned(delivery_id, reacquired)
        except Exception:
            log.exception("send %s crashed", delivery_id)
            # 意外异常按临时失败退避，避免 release 后被立即反复领取形成风暴
            self.store.mark_retry(delivery_id, None, "worker crash",
                                  time.time() + min(self.cfg.backoff_cap, 1.0))
            self._wait_then_send(delivery_id, min(self.cfg.backoff_cap, 1.0))
            suspended = True
        finally:
            if not suspended:
                with self._lock:
                    self.flying -= 1
                    self.active -= 1
                self.limit.release()

    def _wait_then_send(self, delivery_id: str, delay: float) -> None:
        """让出“当前这次调用”的令牌与在飞计数（active 保留），定时重试。

        投递始终是 inflight，调度器不会重复领取；而 claim_due 的端点级退避
        会挡住该端点其他对象（接收方正在限流/下线，不打无意义的请求）。"""
        with self._lock:
            self.flying -= 1
        self.limit.release()
        t = threading.Timer(delay, self._after_backoff, args=(delivery_id,))
        t.daemon = True
        t.start()

    def _after_backoff(self, delivery_id: str) -> None:
        # 退避结束：claim_due 要求所有未完成投递都到点，多个定时器形成自然闸口：
        # 先抢到令牌的先发送，其余随后放行，仍由端点并行度统一约束。
        self.limit.acquire()
        with self._lock:
            self.flying += 1
        self.pool.submit(self._run_with_token, delivery_id, True)


class Supervisor:
    def __init__(self, store: Store, cfg: "HubConfig"):
        self.store = store
        self.cfg = cfg
        self.pool = ThreadPoolExecutor(max_workers=cfg.max_workers,
                                       thread_name_prefix="whub-send")
        self.runners: dict[str, EndpointRunner] = {}
        self._lock = threading.Lock()
        self.stop_event = threading.Event()
        self.threads = [
            threading.Thread(target=self._discover, name="whub-discover",
                             daemon=True),
            threading.Thread(target=self._reap, name="whub-reaper", daemon=True),
        ]

    def start(self) -> None:
        # 启动即回收上个进程遗留的孤儿 inflight（不等租约到期）
        try:
            n = self.store.reap_expired_leases(started_before=time.time())
            if n:
                log.warning("recovered %d in-flight deliveries from previous run", n)
        except Exception:
            log.exception("startup lease recovery failed")
        for t in self.threads:
            t.start()

    def stop(self) -> None:
        self.stop_event.set()
        with self._lock:
            for r in self.runners.values():
                r.stop()
        self.pool.shutdown(wait=False)

    def kick(self, endpoint_id: str) -> None:
        """唤醒端点 runner；若端点刚创建、尚未被 discover 发现，则立即启动它，
        消除“建端点后立刻投事件”的启动竞态。"""
        with self._lock:
            r = self.runners.get(endpoint_id)
            if r is None and endpoint_id in self.store.active_endpoint_ids():
                r = EndpointRunner(endpoint_id, self.store, self.pool, self.cfg)
                self.runners[endpoint_id] = r
                r.start()
                log.info("lazily started runner for endpoint %s", endpoint_id)
        if r:
            r.kick()

    def runner(self, endpoint_id: str) -> EndpointRunner | None:
        with self._lock:
            return self.runners.get(endpoint_id)

    def _discover(self) -> None:
        while not self.stop_event.wait(timeout=self.cfg.discover_interval):
            try:
                wanted = set(self.store.active_endpoint_ids())
                with self._lock:
                    have = set(self.runners)
                    for eid in wanted - have:
                        r = EndpointRunner(eid, self.store, self.pool, self.cfg)
                        self.runners[eid] = r
                        r.start()
                        log.info("started runner for endpoint %s", eid)
                    for eid in have - wanted:
                        self.runners.pop(eid).stop()
                        log.info("stopped runner for endpoint %s", eid)
            except Exception:
                log.exception("discover loop failed")

    def _reap(self) -> None:
        while not self.stop_event.wait(timeout=self.cfg.reap_interval):
            try:
                n = self.store.reap_expired_leases()
                if n:
                    log.warning("reaped %d expired in-flight deliveries", n)
                    with self._lock:
                        for r in self.runners.values():
                            r.kick()
            except Exception:
                log.exception("reap loop failed")
