"""投递 worker：lease 的持有者与执行者，本身不做任何跨进程裁决。

所有权状态的唯一事实来源是 durable store（:mod:`whub.engine` 中的事务）。
本模块里的 ``owned``/``busy_lanes`` 只是缓存：

* 任何缓存与数据库不一致时，fence 条件（owner+epoch+fence_id）会让写操作
  在事务中影响 0 行并抛 :class:`StaleEpoch`，worker 随即丢弃缓存；
* **每次出站 HTTP 之前**都要过一次 :meth:`StoreClient.touch` 写事务：
  store 拒绝写入（outage）或 fence 已陈旧时绝不产生出站副作用；
* 进程内线程/锁只用于本进程的并发发送与缓存记账，禁止用于跨进程仲裁。
"""
from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .client import DirectClient, HttpStore, StoreUnavailable, StaleEpoch, LeaseNotOwned
from .config import HubConfig
from .engine import StoreError
from .sender import backoff_delay, build_request, send
from .util import new_id

log = logging.getLogger("whub.worker")


class LeaseToken:
    __slots__ = ("epoch", "fence", "expires_at")

    def __init__(self, epoch: int, fence: str, expires_at: Optional[float]):
        self.epoch = epoch
        self.fence = fence
        self.expires_at = expires_at


class Worker:
    def __init__(self, worker_id: str, store, cfg: HubConfig,
                 host: str = "", port: int = 0):
        self.wid = worker_id
        self.store = store
        self.cfg = cfg
        self.host, self.port = host, port
        self.incarnation = ""
        # 缓存：lane_id -> LeaseToken（仅用于少读库；裁决以 DB 为准）
        self.tokens: dict[str, LeaseToken] = {}
        self.busy_lanes: set[str] = set()       # 本进程内已提交 claim/在途
        self._lock = threading.RLock()
        self.stop_event = threading.Event()
        # STW 注入：非 0 时主循环在该 store 时钟前原地睡眠（不续期）
        self.freeze_until = 0.0
        # 自动 rebalance 仅在启动后短暂进行（帮助新成员渐进分流），
        # 稳态后由控制面显式触发；忙 lane 在引擎层永不参与搬运。
        self.rebalance_until = 0.0
        self.pool = ThreadPoolExecutor(max_workers=cfg.max_workers,
                                       thread_name_prefix=f"send-{worker_id}")
        self.wake = threading.Event()
        # 进程内观测计数（权威计数仍以 store counters 为准）
        self.local = {"sent_ok": 0, "sent_retry": 0, "sent_dead": 0,
                      "touch_blocked": 0, "store_unavailable": 0}
        self._threads: list[threading.Thread] = []

    # ---- 生命周期 ----------------------------------------------------

    def start(self) -> None:
        info = self.store.rpc("register_worker", worker_id=self.wid,
                              host=self.host, port=self.port)
        self.incarnation = info["incarnation"]
        try:
            self.rebalance_until = self.store.rpc("now") + max(
                self.cfg.lease_ttl * 1.5, 3.0)
        except StoreError:
            self.rebalance_until = time.time() + 5
        log.info("worker %s registered incarnation=%s", self.wid,
                 self.incarnation[:14])
        self._threads = [
            self._thread("hb", self._heartbeat_loop,
                         self.cfg.heartbeat_interval),
            self._thread("renew", self._renew_loop,
                         max(self.cfg.renew_interval, 0.1)),
            self._thread("sweep", self._sweep_loop,
                         max(self.cfg.sweep_interval, 0.1)),
            self._thread("dispatch", self._dispatch_loop, 0.1),
        ]
        for t in self._threads:
            t.start()
        self.wake.set()

    def stop(self, graceful: bool = True) -> None:
        self.stop_event.set()
        self.wake.set()
        if graceful and self.incarnation:
            try:
                self.store.rpc("deregister_worker", worker_id=self.wid,
                               incarnation=self.incarnation,
                               ttl=self.cfg.lease_ttl)
            except StoreError:
                log.exception("deregister failed; leases will expire")
        for t in self._threads:
            t.join(timeout=3)
        self.pool.shutdown(wait=False)

    def _thread(self, name: str, target, interval: float) -> threading.Thread:
        return threading.Thread(target=target, name=f"{self.wid}-{name}",
                                daemon=True)

    # ---- 故障注入：stop-the-world ------------------------------------

    def freeze(self, seconds: float) -> float:
        """让 worker 主循环整体暂停（不续期、不发送），用 store 时钟算解除点。"""
        until = self.store.rpc("now") + seconds
        self.freeze_until = until
        self.wake.set()
        return until

    def _maybe_frozen(self) -> None:
        if self.freeze_until:
            now = self.store.rpc("now")
            if now < self.freeze_until:
                log.warning("worker %s FROZEN for %.2fs (stop-the-world)",
                            self.wid, self.freeze_until - now)
                # 分段睡，便于退出及时
                while not self.stop_event.is_set():
                    n = self.store.rpc("now")
                    if n >= self.freeze_until:
                        break
                    time.sleep(min(0.2, self.freeze_until - n))
                log.warning("worker %s RESUMED after freeze", self.wid)
            self.freeze_until = 0.0

    # ---- 心跳（仅存活登记） ------------------------------------------

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.wait(
                self.cfg.heartbeat_interval + self.cfg.jitter()):
            self._maybe_frozen()
            try:
                self.store.rpc("heartbeat", worker_id=self.wid,
                               incarnation=self.incarnation)
            except StoreUnavailable:
                self.local["store_unavailable"] += 1
            except StoreError:
                # incarnation 被新进程顶替：本进程彻底退出仲裁
                log.error("worker %s superseded; heartbeats rejected", self.wid)
                self.stop_event.set()
                return

    # ---- 租约续期 ----------------------------------------------------

    def _renew_loop(self) -> None:
        while not self.stop_event.wait(
                self.cfg.renew_interval + self.cfg.jitter()):
            self._maybe_frozen()
            self._renew_all()

    def _drain_state(self) -> tuple[bool, Optional[float]]:
        w = self.store.rpc("worker", worker_id=self.wid)
        if not w:
            return False, None
        return bool(w["draining"]), w["drain_until"]

    def _renew_all(self) -> None:
        with self._lock:
            lane_ids = list(self.tokens)
        if not lane_ids:
            return
        draining, until = self._drain_state()
        now = self.store.rpc("now")
        for lane_id in lane_ids:
            tok = self.tokens.get(lane_id)
            if tok is None:
                continue
            # drain 过了收尾期限：不再续期，让租约自然过期被其他成员接手
            if draining and until is not None and now >= until:
                log.info("drain deadline passed; letting %s expire", lane_id)
                with self._lock:
                    self.tokens.pop(lane_id, None)
                continue
            try:
                r = self.store.rpc(
                    "renew", lane_id=lane_id, worker_id=self.wid,
                    epoch=tok.epoch, fence=tok.fence,
                    incarnation=self.incarnation, ttl=self.cfg.lease_ttl)
                tok.expires_at = r["expires_at"]
            except StaleEpoch:
                log.warning("renew %s rejected (stale epoch/fence); dropping",
                            lane_id)
                with self._lock:
                    self.tokens.pop(lane_id, None)
                    self.busy_lanes.discard(lane_id)
            except StoreUnavailable:
                self.local["store_unavailable"] += 1
                # store 不可写：什么也不做，绝不能凭内存继续发送
            except LeaseNotOwned:
                with self._lock:
                    self.tokens.pop(lane_id, None)
                    self.busy_lanes.discard(lane_id)

    # ---- acquire / steal 扫描 ----------------------------------------

    def _sweep_loop(self) -> None:
        while not self.stop_event.wait(
                self.cfg.sweep_interval + self.cfg.jitter()):
            self._maybe_frozen()
            try:
                self._sweep_once()
            except StoreUnavailable:
                self.local["store_unavailable"] += 1
            except Exception:
                log.exception("sweep failed")

    def _sweep_once(self) -> None:
        draining, _ = self._drain_state()
        if draining:
            budget = 0  # drain 模式停止接受新 lane
        else:
            budget = self.cfg.acquire_budget
        r = self.store.rpc(
            "sweep", worker_id=self.wid, incarnation=self.incarnation,
            ttl=self.cfg.lease_ttl, acquire_budget=budget)
        gained = list(r["acquired"]) + [s["lane_id"] for s in r["stolen"]]
        # drain 空闲 handoff 若直接交给自己，也纳入缓存
        gained += [h["lane_id"] for h in r["drain_handoffs"]
                   if h.get("to") == self.wid]
        gained = list(dict.fromkeys(gained))
        if gained:
            self._adopt(gained)
        # 成员加入后短时间内允许受 cap 约束的渐进再均衡（只搬空闲 lane）
        if not draining and self.store.rpc("now") < self.rebalance_until:
            try:
                rb = self.store.rpc(
                    "rebalance", budget=self.cfg.rebalance_budget,
                    trigger="join", ttl=self.cfg.lease_ttl)
                moved_to = [m["lane_id"] for m in rb["moved"]
                            if m["to"] == self.wid]
                if moved_to:
                    self._adopt(moved_to)
            except StoreUnavailable:
                self.local["store_unavailable"] += 1

    def _adopt(self, lane_ids: list[str]) -> None:
        new_tokens = []
        for lane_id in lane_ids:
            lane = self.store.rpc("lane", lane_id=lane_id)
            if lane and lane["owner_id"] == self.wid:
                with self._lock:
                    self.tokens.setdefault(
                        lane_id, LeaseToken(lane["lease_epoch"],
                                           lane["fence_id"],
                                           lane["expires_at"]))
                new_tokens.append(lane_id)
        if new_tokens:
            log.info("worker %s adopted %d lanes: %s", self.wid,
                     len(new_tokens), new_tokens[:6])
            self.wake.set()

    # ---- 投递调度 ----------------------------------------------------

    def _dispatch_loop(self) -> None:
        while not self.stop_event.is_set():
            self.wake.wait(timeout=0.2)
            self.wake.clear()
            self._maybe_frozen()
            try:
                self._schedule()
            except StoreUnavailable:
                self.local["store_unavailable"] += 1
                # store 不可写时停止 claim/发送，等待恢复后重新竞争
            except StaleEpoch:
                pass
            except Exception:
                log.exception("dispatch failed")

    def _schedule(self) -> None:
        with self._lock:
            candidates = [lid for lid in self.tokens if lid not in self.busy_lanes]
        for lane_id in candidates:
            tok = self.tokens.get(lane_id)
            if tok is None:
                continue
            with self._lock:
                if lane_id in self.busy_lanes:
                    continue
                # claim 本身是事务，会再校验端点并行度/fence；这里只做进程内预算
                if len(self.busy_lanes) >= self.cfg.max_workers:
                    return
                self.busy_lanes.add(lane_id)
            try:
                job = self.store.rpc(
                    "claim", lane_id=lane_id, worker_id=self.wid,
                    epoch=tok.epoch, fence=tok.fence)
            except StaleEpoch:
                with self._lock:
                    self.tokens.pop(lane_id, None)
                    self.busy_lanes.discard(lane_id)
                continue
            except StoreUnavailable:
                with self._lock:
                    self.busy_lanes.discard(lane_id)
                self.local["store_unavailable"] += 1
                continue
            if not job:
                with self._lock:
                    self.busy_lanes.discard(lane_id)
                continue
            self.pool.submit(self._deliver, lane_id, job, tok.epoch, tok.fence)

    # ---- 一次发送（严格 fence + 不可变快照） --------------------------

    def _deliver(self, lane_id: str, job: dict, epoch: int, fence: str) -> None:
        done = False
        try:
            # 出站前 fence 探针：写事务。store outage 或 fence 陈旧 => 绝不发送。
            try:
                self.store.rpc("touch", lane_id=lane_id, worker_id=self.wid,
                               epoch=epoch, fence=fence,
                               ttl=self.cfg.lease_ttl)
            except (StaleEpoch, LeaseNotOwned):
                self.local["touch_blocked"] += 1
                log.warning("suppress outbound for %s: fence stale", job["id"])
                return
            except StoreUnavailable:
                self.local["touch_blocked"] += 1
                self.local["store_unavailable"] += 1
                log.warning("suppress outbound for %s: store unavailable",
                            job["id"])
                # job 在 DB 仍为 leased，租约过期后由继任者重发
                return

            ver = self.store.rpc("delivery_secret", eid=job["endpoint_id"],
                                 version=job["sig_version"])
            ev = self.store.rpc("event", event_id=job["event_id"])
            if ver is None or ev is None:
                self.store.rpc("complete_dead", job_id=job["id"],
                               worker_id=self.wid, epoch=epoch, fence=fence,
                               code=0, error="version or event missing")
                return
            req = build_request(
                job["target_url"], delivery_id=job["id"],
                event_id=job["event_id"], endpoint_id=job["endpoint_id"],
                kid=job["kid"], object_key=self._object_key(job, ev),
                seq=job["seq"], payload=ev["payload"],
                secret=ver["secret"], sig_version=job["sig_version"])
            result = send(req, timeout=self.cfg.http_timeout)
            done = self._settle(lane_id, job, epoch, fence, result)
        except StaleEpoch:
            log.warning("job %s result dropped: stale fence (new owner wins)",
                        job["id"])
        except StoreUnavailable:
            self.local["store_unavailable"] += 1
            log.warning("job %s settle blocked: store outage", job["id"])
        except Exception:
            log.exception("deliver %s crashed", job["id"])
        finally:
            with self._lock:
                self.busy_lanes.discard(lane_id)
            # lane 仍归我所有且仍有队头：立刻驱动下一条（序号连续推进）
            if done and lane_id in self.tokens:
                self.wake.set()

    @staticmethod
    def _object_key(job: dict, ev: dict) -> str:
        return ev.get("object_key") or f"lane-{job['lane_id']}"

    def _settle(self, lane_id, job, epoch, fence, result) -> bool:
        """回写结果。三类终态/退避都携带 identical fence；返回是否已结算。"""
        if result.ok:
            self.store.rpc("complete_success", job_id=job["id"],
                           worker_id=self.wid, epoch=epoch, fence=fence,
                           code=result.code or 200)
            self.local["sent_ok"] += 1
            return True
        if result.retryable:
            store_now = self.store.rpc("now")
            delay = backoff_delay(
                job["fail_count"] + 1, base=self.cfg.backoff_base,
                cap=self.cfg.backoff_cap, retry_after=result.retry_after)
            self.store.rpc("complete_retry", job_id=job["id"],
                           worker_id=self.wid, epoch=epoch, fence=fence,
                           code=result.code,
                           error=result.error or f"HTTP {result.code}",
                           not_before=store_now + delay)
            self.local["sent_retry"] += 1
            log.info("job %s retryable (%s) not_before +%.2fs",
                     job["id"], result.code or result.error, delay)
            return True
        self.store.rpc("complete_dead", job_id=job["id"],
                       worker_id=self.wid, epoch=epoch, fence=fence,
                       code=result.code or 0,
                       error=result.error or f"HTTP {result.code}")
        self.local["sent_dead"] += 1
        return True

    # ---- 测试注入：显式用旧 fence 做三类陈旧写 ------------------------

    def stale_attempts(self, lane_id: str, epoch: int, fence: str,
                       job_id: Optional[str] = None) -> dict:
        """恢复的旧 worker 用陈旧 fence 分别尝试 renew/claim/complete。

        三类操作都必须被数据库拒绝（影响 0 行 -> StaleEpoch），
        且新 owner 已写入的状态保持不变。"""
        out: dict[str, str] = {}

        def attempt(op: str, fn):
            try:
                fn()
                out[op] = "ACCEPTED(!!)"
            except StaleEpoch:
                out[op] = "stale_epoch"
            except LeaseNotOwned:
                out[op] = "lease_not_owned"
            except StoreUnavailable:
                out[op] = "store_unavailable"

        attempt("renew", lambda: self.store.rpc(
            "renew", lane_id=lane_id, worker_id=self.wid, epoch=epoch,
            fence=fence, incarnation=self.incarnation,
            ttl=self.cfg.lease_ttl))
        attempt("claim", lambda: self.store.rpc(
            "claim", lane_id=lane_id, worker_id=self.wid, epoch=epoch,
            fence=fence))
        if job_id:
            attempt("complete", lambda: self.store.rpc(
                "complete_success", job_id=job_id, worker_id=self.wid,
                epoch=epoch, fence=fence, code=200))
        else:
            out["complete"] = "skipped(no job_id)"
        return out

    # ---- 运维视图 ----------------------------------------------------

    def view(self) -> dict:
        with self._lock:
            owned = len(self.tokens)
            active = len(self.busy_lanes)
        return {"worker_id": self.wid, "incarnation": self.incarnation,
                "owned": owned, "active": active, **self.local}
