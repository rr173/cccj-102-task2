"""跨进程投递运行时：lane lease worker + DB 选举的 rebalance 协调器。

跨进程仲裁完全在 :mod:`whub.db` 的即时事务里完成；本模块只负责：

- ``LaneRunner``：每拥有一条 lane 一个调度循环，claim/发送/退避，
  **每次出站 HTTP 前先过一道 fence 续期写**——store 拒绝写入（outage）或
  epoch 已易主时绝不产生外部副作用；
- ``Worker``：lease 管理器（心跳/续期/竞争公共池/TTL steal/drain 收尾），
  以数据库行 ``lane_leases`` 为准做 reconcile，本地不保存任何权威所有权；
- ``Coordinator``：经 ``coord_state`` 单例选举出的 leader，按 cap 渐进
  再均衡，并兜底回收孤儿 inflight。

禁止：线程锁/PID 文件/内存表不参与任何跨进程决策（进程内 RLock 仅为
sqlite3 连接的线程安全约束）。
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .db import (Store, StaleEpoch, LeaseNotOwned, StoreUnavailable,
                 new_fence)
from .sender import backoff_delay, build_request, send

log = logging.getLogger("whub.dispatcher")

DRAIN_REASONS = ("drain_handoff", "drain_release", "drain_expiry_release")


class DynamicLimit:
    """可热更新容量的信号量（parallelism PATCH 后立即生效）。"""

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


class LaneRunner:
    """单条 lane（端点）的调度循环；仅在本 worker 持有有效 fence 时存活。"""

    def __init__(self, worker: "Worker", lane_id: str, epoch: int,
                 fence: str):
        self.worker = worker
        self.store: Store = worker.store
        self.cfg = worker.cfg
        self.lane_id = lane_id
        self.epoch = epoch
        self.fence = fence
        ep = self.store.endpoint(lane_id)
        self.limit = DynamicLimit(ep["parallelism"] if ep else 1)
        self.wake = threading.Event()
        self.stop_event = threading.Event()
        self.frozen = threading.Event()   # drain deadline 后：只续期，不再调度/发送
        self.active = 0   # 已领取未终态（含退避等待）
        self.flying = 0   # 占着 HTTP 令牌的数量
        self._cnt_lock = threading.Lock()
        self.thread = threading.Thread(
            target=self._loop, name=f"lane-{lane_id[:10]}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake.set()

    def kick(self) -> None:
        self.wake.set()

    def creds(self) -> tuple[int, str] | None:
        """向 worker 查询当前权威 fence（来自数据库 reconcile）。"""
        return self.worker.lane_creds(self.lane_id)

    # -- 调度循环 ------------------------------------------------------

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._tick()
            except StaleEpoch:
                log.info("lane %s runner stopping: stale fence", self.lane_id)
                return
            except StoreUnavailable:
                self.worker.mark_store_down()
            except Exception:
                log.exception("lane %s tick failed", self.lane_id)
            self.wake.wait(timeout=self.cfg.poll_interval)
            self.wake.clear()

    def _tick(self) -> None:
        if self.frozen.is_set():
            return  # drain deadline 已到：lease 照常续期，但不再 claim/发送
        creds = self.creds()
        if creds is None:
            raise StaleEpoch("lane no longer owned", lane_id=self.lane_id)
        self.epoch, self.fence = creds
        ep = self.store.endpoint(self.lane_id)
        if ep is None:
            return
        self.limit.set(ep["parallelism"])
        with self._cnt_lock:
            want = max(ep["parallelism"] - self.active, 0)
        tokens = 0
        for _ in range(want):
            if self.limit.try_acquire():
                tokens += 1
            else:
                break
        if not tokens:
            return
        rows = self.store.claim_due(self.lane_id, tokens, self.worker.worker_id,
                                    self.epoch, self.fence)
        for _ in range(tokens - len(rows)):
            self.limit.release()
        for row in rows:
            with self._cnt_lock:
                self.active += 1
                self.flying += 1
            self.worker.pool.submit(self._run_with_token, row["id"])

    # -- 发送 ----------------------------------------------------------

    def _run_with_token(self, delivery_id: str) -> None:
        suspended = False
        try:
            suspended = self._attempt(delivery_id)
        except Exception:
            log.exception("send %s crashed", delivery_id)
            suspended = self._safe_crash_backoff(delivery_id)
        finally:
            if not suspended:
                with self._cnt_lock:
                    self.flying -= 1
                    self.active -= 1
                self.limit.release()
                self.kick()

    def _fence_gate(self) -> tuple[int, str] | None:
        """出站前的持久化闸门：成功续期才允许发出 HTTP。

        返回最新 creds；StaleEpoch/StoreUnavailable 一律不放行。"""
        creds = self.creds()
        if creds is None:
            raise StaleEpoch("lost lane before outbound",
                             lane_id=self.lane_id)
        epoch, fence = creds
        row = self.store.renew_lane(self.lane_id, self.worker.worker_id,
                                    epoch, fence, self.cfg.lease_ttl)
        self.worker.metric("renew")
        self.epoch, self.fence = row["lease_epoch"], row["fence_id"]
        return self.epoch, self.fence

    def _attempt(self, delivery_id: str) -> bool:
        """一次发送尝试。返回 True 表示已移交退避定时器（计数保留）。"""
        row = self.store.delivery(delivery_id)
        if row is None or row["status"] != "inflight":
            return False
        creds = self.creds()
        if creds is None:
            raise StaleEpoch("lost lane", lane_id=self.lane_id)
        self.epoch, self.fence = creds
        nb = row["not_before"]
        db_now = self.store.db_now()
        if nb > db_now:
            # 退避等待：让出令牌（active 保留），定时后重试
            self._release_flying_token()
            self._schedule(delivery_id, max(nb - db_now, 0.05))
            return True
        # drain deadline 已到：不再产生任何新的出站，等对手 drain_handoff
        # （fence 一旦易主，这里无论如何都会被数据库拒绝）。
        if self.frozen.is_set():
            return False
        # —— 关键：任何出站 side effect 之前必须通过 fence 续期写 ——
        self._fence_gate()
        ver = self.store.delivery_secret(self.lane_id, row["sig_version"])
        ev = self.store.event(row["event_id"])
        if ver is None or ev is None:
            self.store.mark_dead(delivery_id, 0, "version or event missing",
                                 worker_id=self.worker.worker_id,
                                 epoch=self.epoch, fence=self.fence)
            return False
        req = build_request(
            row["target_url"], delivery_id=delivery_id,
            event_id=row["event_id"], endpoint_id=self.lane_id,
            kid=row["kid"], object_key=row["object_key"], seq=row["seq"],
            payload=ev["payload"], secret=ver["secret"])
        result = send(req, timeout=self.cfg.http_timeout)
        if result.ok:
            self.store.mark_succeeded(delivery_id, result.code or 200,
                                      worker_id=self.worker.worker_id,
                                      epoch=self.epoch, fence=self.fence)
            self.kick()
            return False
        if result.retryable:
            delay = backoff_delay(
                row["fail_count"] + 1, base=self.cfg.backoff_base,
                cap=self.cfg.backoff_cap, retry_after=result.retry_after)
            try:
                self.store.mark_retry(
                    delivery_id, result.code,
                    result.error or f"HTTP {result.code}", delay,
                    worker_id=self.worker.worker_id,
                    epoch=self.epoch, fence=self.fence)
            except StaleEpoch:
                raise
            log.info("lane %s delivery %s retryable (%s), backoff %.2fs",
                     self.lane_id, delivery_id, result.code or result.error,
                     delay)
            self._release_flying_token()
            self._schedule(delivery_id, delay)
            return True
        self.store.mark_dead(
            delivery_id, result.code or 0,
            result.error or f"HTTP {result.code}",
            worker_id=self.worker.worker_id,
            epoch=self.epoch, fence=self.fence)
        self.kick()
        return False

    def _safe_crash_backoff(self, delivery_id: str) -> bool:
        """工作线程内意外异常：尽力落一条退避；fence 已失配则直接放弃。"""
        try:
            creds = self.creds()
            if creds is None:
                return False
            epoch, fence = creds
            self.store.mark_retry(delivery_id, None, "worker crash", 1.0,
                                  worker_id=self.worker.worker_id,
                                  epoch=epoch, fence=fence)
            self._release_flying_token()
            self._schedule(delivery_id, min(1.0, self.cfg.backoff_cap))
            return True
        except StaleEpoch:
            return False
        except StoreUnavailable:
            self.worker.mark_store_down()
            return False

    def _release_flying_token(self) -> None:
        with self._cnt_lock:
            self.flying -= 1
        self.limit.release()

    def _schedule(self, delivery_id: str, delay: float) -> None:
        t = threading.Timer(max(delay, 0.02), self._after_wait,
                            args=(delivery_id,))
        t.daemon = True
        t.name = f"backoff-{delivery_id[:10]}"
        t.start()

    def _after_wait(self, delivery_id: str) -> None:
        if self.stop_event.is_set():
            return
        try:
            creds = self.creds()
            if creds is None:
                # lane 已易主：本次在途已在易主事务里退回 pending，直接放弃
                self._drop_active()
                return
            # store 仍不可写：不发任何请求，稍后再探
            try:
                self.store.renew_lane(self.lane_id, self.worker.worker_id,
                                      creds[0], creds[1], self.cfg.lease_ttl)
                self.worker.metric("renew")
            except StoreUnavailable:
                self.worker.mark_store_down()
                self._schedule(delivery_id, 0.25)
                return
            self.limit.acquire()
            with self._cnt_lock:
                self.flying += 1
            self.worker.pool.submit(self._run_with_token, delivery_id)
        except StaleEpoch:
            self.worker.metric("stale_write_rejected")
            self._drop_active()
        except Exception:
            log.exception("after-backoff %s failed", delivery_id)
            self._drop_active()

    def _drop_active(self) -> None:
        with self._cnt_lock:
            self.active = max(self.active - 1, 0)
        self.kick()


class Worker:
    """一个 OS process 内的 worker 身份与其持有的全部 lane。"""

    def __init__(self, store: Store, cfg, *, worker_id: str | None = None):
        self.store = store
        self.cfg = cfg
        self.worker_id = worker_id or cfg.worker_id or f"worker-{os.getpid()}"
        self.incarnation = new_fence()
        self.pool = ThreadPoolExecutor(max_workers=cfg.max_workers,
                                       thread_name_prefix="whub-send")
        self.runners: dict[str, LaneRunner] = {}
        self._rlock = threading.RLock()
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.store_down = False
        self._sb_lock = threading.Lock()
        self._lease_serial = threading.Lock()
        self._last_renew: dict[str, float] = {}
        self.draining = False
        self.drain_deadline: float | None = None
        self.lease_thread: threading.Thread | None = None
        self.coordinator: "Coordinator | None" = None

    # -- 生命周期 ------------------------------------------------------

    def start(self) -> None:
        self.store.register_worker(self.worker_id, self.incarnation,
                                   os.getpid())
        self.lease_thread = threading.Thread(
            target=self._lease_loop, name=f"lease-{self.worker_id}",
            daemon=True)
        self.lease_thread.start()
        self.coordinator = Coordinator(self)
        self.coordinator.start()
        log.info("worker %s started (incarnation %s, ttl=%.1fs)",
                 self.worker_id, self.incarnation, self.cfg.lease_ttl)

    def stop(self, *, release_lanes: bool = True) -> None:
        log.info("worker %s stopping", self.worker_id)
        self.stop_event.set()
        self.wake.set()
        try:
            self.store.set_worker_stopping(self.worker_id)
        except (StoreUnavailable, Exception):
            pass
        if self.coordinator:
            self.coordinator.stop()
        if release_lanes:
            self._release_all_lanes("worker_shutdown")
        with self._rlock:
            for r in list(self.runners.values()):
                r.stop()
        self.pool.shutdown(wait=False)

    def _release_all_lanes(self, reason: str) -> None:
        for lane_id in list(self.runners):
            creds = self.lane_creds(lane_id)
            if creds is None:
                continue
            try:
                self.store.release_lane(lane_id, self.worker_id,
                                        creds[0], creds[1], reason=reason)
                self._ownership_log(lane_id, reason)
            except (StaleEpoch, LeaseNotOwned):
                pass
            except StoreUnavailable:
                log.warning("cannot release lane %s: store unavailable",
                            lane_id)

    def begin_drain(self, deadline: float | None = None) -> float:
        dl = self.cfg.drain_deadline if deadline is None else deadline
        row = self.store.set_worker_drain(self.worker_id, True, dl)
        self.draining = True
        now = self.store.db_now()
        self.drain_deadline = row["drain_deadline"]
        log.warning("worker %s entering DRAIN (deadline in %.1fs, %d lanes)",
                    self.worker_id, self.drain_deadline - now,
                    len(self.runners))
        return self.drain_deadline

    def end_drain(self) -> None:
        self.store.set_worker_drain(self.worker_id, False)
        self.draining = False
        self.drain_deadline = None

    # -- 指标 / 工具 ---------------------------------------------------

    def metric(self, name: str, n: int = 1) -> None:
        self.store.incr_metric(self.worker_id, name, n)

    def lane_creds(self, lane_id: str) -> tuple[int, str] | None:
        with self._rlock:
            return self._creds.get(lane_id)

    @property
    def _creds(self) -> dict[str, tuple[int, str]]:
        return {lid: (r.epoch, r.fence) for lid, r in self.runners.items()}

    def mark_store_down(self) -> None:
        with self._sb_lock:
            if not self.store_down:
                self.store_down = True
                log.error("durable store unavailable: worker %s freezes all "
                          "outbound side effects until write probe recovers",
                          self.worker_id)

    def _ownership_log(self, lane_id: str, reason: str,
                       row: dict | None = None) -> None:
        if row is None:
            row = dict(self.store.lease_view(lane_id) or {})
        log.info(
            "OWNERSHIP lane=%s old_owner=%s new_owner=%s old_epoch=%s "
            "new_epoch=%s reason=%s",
            lane_id, row.get("old_owner", "?"), row.get("owner_id"),
            row.get("old_epoch", "?"), row.get("lease_epoch"), reason)

    # -- reconcile：数据库行是唯一权威 --------------------------------

    def _reconcile(self) -> None:
        rows = self.store.owned_lanes(self.worker_id)
        db_lanes = {r["lane_id"]: r for r in rows}
        with self._rlock:
            for lid, row in db_lanes.items():
                r = self.runners.get(lid)
                if r is None:
                    if row["draining"]:
                        continue
                    runner = LaneRunner(self, lid, row["lease_epoch"],
                                        row["fence_id"])
                    self.runners[lid] = runner
                    runner.start()
                    log.info("worker %s assumed lane %s epoch=%d reason=%s",
                             self.worker_id, lid, row["lease_epoch"],
                             row["last_handoff_reason"])
                elif (r.epoch != row["lease_epoch"]
                      or r.fence != row["fence_id"]):
                    r.epoch, r.fence = row["lease_epoch"], row["fence_id"]
                    r.kick()
            for lid in list(self.runners):
                if lid not in db_lanes:
                    r = self.runners.pop(lid)
                    r.stop()
                    log.info("worker %s dropped lane %s (lease moved)",
                             self.worker_id, lid)

    # -- lease 管理循环 ------------------------------------------------

    def _lease_loop(self) -> None:
        # 管理节拍固定且较快（发现新 lane / steal / drain / reconcile），
        # 与续期频率解耦：单 worker 生产默认 ttl=30s 时也能秒级接管新 lane。
        tick = self.cfg.lease_tick_interval
        while not self.stop_event.wait(0):
            with self._lease_serial:
                if self.stop_event.is_set():
                    break
                try:
                    self._lease_tick()
                except StoreUnavailable:
                    self.mark_store_down()
                except StaleEpoch as e:
                    self.metric("stale_write_rejected")
                    log.info("stale write rejected in lease loop: %s", e)
                except Exception:
                    log.exception("lease tick failed")
            if self.wake.wait(timeout=max(tick, 0.05)):
                self.wake.clear()

    def _lease_tick(self) -> None:
        # store 熔断：只做可写探测，成功才恢复；期间不续期、不 claim、不发送
        if self.store_down:
            try:
                self.store.ping_write()
            except StoreUnavailable:
                return
            log.warning("store writable again: worker %s rejoining lease "
                        "competition (old epochs are not revived)",
                        self.worker_id)
            self.store_down = False

        self.store.heartbeat_worker(self.worker_id)
        w = self.store.list_workers(self.cfg.effective_worker_stale_after())
        me = next((x for x in w if x["worker_id"] == self.worker_id), None)
        if me is None:
            return
        self.draining = bool(me["draining"])
        self.drain_deadline = me["drain_deadline"] or None

        # drain 期间仍须正常续期（否则 lane 会普通过期，丧失 deadline 语义）；
        # 只停止“增长”，手头工作可在 deadline 前收尾。
        self._renew_owned()
        self._reconcile()

        if self.draining:
            self._drain_tick(me["drain_deadline"])
        else:
            self._grow_tick(w)

    def _renew_owned(self) -> None:
        interval = self.cfg.effective_renew_interval()
        now = self.store.db_now()
        for lane_id, r in list(self.runners.items()):
            # 按需续期：到达续期间隔，或剩余寿命 < 1.5*interval（保险）才写，
            # 避免每个快速管理节拍都刷一次 expires_at。
            last = self._last_renew.get(lane_id, 0.0)
            view = self.store.lease_view(lane_id)
            due = (now - last >= interval * (1 - self.cfg.renew_jitter)
                   or view is None
                   or view["expires_at"] - now < interval * 1.5)
            if not due:
                continue
            try:
                row = self.store.renew_lane(lane_id, self.worker_id,
                                            r.epoch, r.fence,
                                            self.cfg.lease_ttl)
                r.epoch, r.fence = row["lease_epoch"], row["fence_id"]
                self._last_renew[lane_id] = now
                self.metric("renew")
            except StaleEpoch as e:
                self.metric("stale_write_rejected")
                log.warning("renew rejected for lane %s (old epoch %s -> %s)",
                            lane_id, e.old_epoch, e.new_epoch)
                with self._rlock:
                    old = self.runners.pop(lane_id, None)
                if old:
                    old.stop()
                self._last_renew.pop(lane_id, None)
            except LeaseNotOwned:
                with self._rlock:
                    old = self.runners.pop(lane_id, None)
                if old:
                    old.stop()
                self._last_renew.pop(lane_id, None)

    def _grow_tick(self, workers: list) -> None:
        # 1) drain 期限已到的他人 lane：deadline 强制接手（单事务 epoch+1）
        for row in self.store.other_draining_due(self.worker_id)[
                : self.cfg.rebalance_max_moves]:
            try:
                res = self.store.steal_due_drain(
                    row["lane_id"], self.worker_id,
                    ttl=self.cfg.lease_ttl)
                if res:
                    self.metric("drain_handoff")
                    self.metric("orphan_recovered", res["requeued"])
                    log.info("OWNERSHIP lane=%s old_owner=%s new_owner=%s "
                             "old_epoch=%d new_epoch=%d reason=drain_handoff",
                             res["lane_id"], row["owner_id"],
                             self.worker_id, row["lease_epoch"],
                             res["lease_epoch"])
            except StaleEpoch:
                self.metric("stale_write_rejected")

        # 2) TTL 过期的 lane：expiry steal（failover 核心路径）。
        # 带在途孤儿（SIGKILL/STOP 的确定性证据）：仅 0.25*ttl 宽限，
        # 过滤单次续期调度抖动后尽快切换；纯空闲：1.5*ttl 后才回收。
        stolen = 0
        for row in self.store.stealable_lanes(
                inflight_grace=self.cfg.lease_ttl * 0.25,
                idle_grace=self.cfg.lease_ttl * 1.5):
            if stolen >= self.cfg.rebalance_max_moves:
                break
            try:
                res = self.store.steal_expired_lane(
                    row["lane_id"], self.worker_id,
                    ttl=self.cfg.lease_ttl)
            except StaleEpoch:
                self.metric("stale_write_rejected")
                continue
            if res:
                stolen += 1
                self.metric("steal")
                self.metric("orphan_recovered", res["requeued"])
                log.info("OWNERSHIP lane=%s old_owner=%s new_owner=%s "
                         "old_epoch=%d new_epoch=%d reason=%s requeued=%d",
                         res["lane_id"], row["owner_id"], self.worker_id,
                         row["lease_epoch"], res["lease_epoch"],
                         res["last_handoff_reason"], res["requeued"])

        # 3) 公共池：受公平份额与单轮 cap 约束的渐进 acquire
        active = [w for w in workers if w["alive"] and not w["draining"]]
        if not active:
            return
        pool = self.store.pool_lanes()
        if not pool:
            return
        total_lanes = len(pool) + sum(w["owned"] for w in active)
        fair = -(-total_lanes // len(active))  # ceil
        mine = next(w["owned"] for w in active
                    if w["worker_id"] == self.worker_id)
        budget = max(fair - mine, 0)
        budget = min(budget, self.cfg.rebalance_max_moves, len(pool))
        for row in pool[:budget]:
            try:
                res = self.store.acquire_from_pool(
                    row["lane_id"], self.worker_id,
                    ttl=self.cfg.lease_ttl, reason="acquire")
                self.metric("acquire")
                log.info("OWNERSHIP lane=%s old_owner=%s new_owner=%s "
                         "old_epoch=%d new_epoch=%d reason=acquire",
                         res["lane_id"], None, self.worker_id,
                         res["lease_epoch"] - 1, res["lease_epoch"])
            except (StaleEpoch, LeaseNotOwned):
                # 竞争失败：公共池被同拍的其它进程拿走，下一轮再来
                continue

    def _drain_tick(self, drain_deadline: float) -> None:
        now = self.store.db_now()
        deadline_passed = bool(drain_deadline) and now >= drain_deadline
        # 手头已空的 lane 提前交还（owned 只减不增；接手方拿到更高 epoch）。
        # deadline 已过且仍有在途工作的 lane **保留并继续续期**，等对手用
        # drain_handoff 强接——绝不能停续期导致普通过期（那就丢了 handoff 语义）。
        for lane_id, r in list(self.runners.items()):
            try:
                outstanding = self.store.lane_outstanding(lane_id)
                if outstanding == 0:
                    self.store.release_lane(lane_id, self.worker_id,
                                            r.epoch, r.fence,
                                            reason="drain_release")
                    self.metric("drain_handoff")
                    with self._rlock:
                        old = self.runners.pop(lane_id, None)
                    if old:
                        old.stop()
                    log.info("OWNERSHIP lane=%s old_owner=%s new_owner=%s "
                             "reason=drain_release (drained empty)",
                             lane_id, self.worker_id, None)
                elif deadline_passed:
                    # 冻结本地调度/发送，但 lease 仍由 _renew_owned 续着，
                    # 对手的 steal_due_drain 下一拍以 drain_handoff 接手
                    r.frozen.set()
                    log.info("drain deadline reached, %d outstanding on %s; "
                             "lease held for imminent drain_handoff",
                             outstanding, lane_id)
            except StaleEpoch:
                self.metric("stale_write_rejected")
                with self._rlock:
                    old = self.runners.pop(lane_id, None)
                if old:
                    old.stop()
            except StoreUnavailable:
                self.mark_store_down()
                return
        # deadline 到点且没有任何其他活跃成员：剩余 lane 交还公共池
        if deadline_passed:
            others = [w for w in self.store.active_workers(
                self.cfg.effective_worker_stale_after())
                if w["worker_id"] != self.worker_id and not w["draining"]]
            if not others:
                self._release_all_lanes("drain_expiry_release")
                self.metric("drain_handoff", len(self.runners))


class Coordinator:
    """DB 单例选举的再均衡协调器（任何 worker 都可能成为 leader）。"""

    def __init__(self, worker: Worker):
        self.worker = worker
        self.store = worker.store
        self.cfg = worker.cfg
        self.is_leader = False
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._loop, name=f"coord-{worker.worker_id}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def _loop(self) -> None:
        while not self.stop_event.wait(self.cfg.coord_interval):
            try:
                self._tick()
            except StoreUnavailable:
                self.worker.mark_store_down()
                self.is_leader = False
            except Exception:
                log.exception("coordinator tick failed")

    def _tick(self) -> None:
        ttl = self.cfg.coord_interval * 2
        if self.is_leader:
            self.is_leader = self.store.coord_renew(
                self.worker.worker_id, ttl)
        if not self.is_leader:
            self.is_leader = self.store.coord_acquire(
                self.worker.worker_id, ttl)
            if not self.is_leader:
                return
            log.info("worker %s elected rebalance coordinator",
                     self.worker.worker_id)

        req = self.store.take_rebalance_request()
        max_moves = (req["max_moves"] if req and req["max_moves"] > 0
                     else self.cfg.rebalance_max_moves)
        if req:
            self.worker.wake.set()
        self._sweep_orphans()
        self._rebalance(max_moves)

    def _sweep_orphans(self) -> None:
        """兜底：inflight fence 失配的投递退回 pending；过期空闲 lane 回池。

        普通成员只 steal 带在途孤儿的过期 lane；崩溃时恰好空闲的 lane 由
        协调器在这里以宽限（0.5*ttl）后集中回收，避免健康 worker 因续期
        拍边界互相误偷。"""
        try:
            n = self.store.sweep_orphan_inflight()
            if n:
                self.worker.metric("orphan_recovered", n)
                log.warning("sweep recovered %d orphan inflight deliveries", n)
            lanes = self.store.coord_reclaim_idle(
                self.worker.worker_id,
                grace=self.cfg.lease_ttl * 1.5, max_n=5)
            for lid in lanes:
                self.worker.metric("orphan_recovered")
                log.info("OWNERSHIP lane=%s reason=orphan_expiry_reclaim "
                         "reclaimed to pool by coordinator", lid)
            if lanes:
                self.worker.wake.set()
        except StoreUnavailable:
            raise

    def _rebalance(self, max_moves: int) -> None:
        """渐进搬运：单轮至多 max_moves 条，避免所有权一齐翻转。

        再均衡的资格判定**不只看 workers 心跳**：SIGSTOP 的 worker 心跳会
        变旧之前仍可能被当作健康成员。因此参与者必须此刻还持有至少一条
        **未过期** lease（真的在续期）；并且：
        - 过期 lease 行不参与搬运（交给 expiry steal）；
        - 不能把 lane 搬给一个一条有效 lease 都没有的疑似冻结成员。
        """
        workers = self.store.list_workers(
            self.cfg.effective_worker_stale_after())
        leases = self.store.list_leases()
        now = self.store.db_now()
        valid_lanes = [l for l in leases
                       if l["owner_id"] is not None
                       and not l["draining"] and l["expires_at"] >= now]
        held_counts: dict[str, int] = {}
        fresh_holders = {l["owner_id"] for l in valid_lanes}
        for l in leases:
            if l["owner_id"] is not None:
                held_counts[l["owner_id"]] = held_counts.get(
                    l["owner_id"], 0) + 1

        def healthy(w) -> bool:
            if not (w["alive"] and not w["draining"] and not w["stopping"]):
                return False
            wid = w["worker_id"]
            # 存活必须有新鲜心跳（被 SIGSTOP/挂死者无法刷新心跳，哪怕刚过
            # 通用失活阈值）。心跳每 lease_tick 一次，3 拍宽限足够。
            freshness = max(self.cfg.lease_tick_interval * 3.0, 1.0)
            if w["age_heartbeat"] > freshness:
                return False
            # 新加入/空闲成员（0 条 lane）可作为目标；一旦持有 lane，
            # 其中至少一条 lease 必须未过期（证明真的在续期）。
            return held_counts.get(wid, 0) == 0 or wid in fresh_holders

        active = [w for w in workers if healthy(w)]
        if len(active) < 2:
            return
        lane_rows = valid_lanes
        loads = {w["worker_id"]: 0 for w in active}
        for l in lane_rows:
            if l["owner_id"] in loads:
                loads[l["owner_id"]] += 1
        total = sum(loads.values()) + len([l for l in leases
                                           if l["owner_id"] is None])
        fair = -(-total // len(active))  # ceil
        donors = sorted(
            ((loads[w["worker_id"]], w["worker_id"]) for w in active
             if loads[w["worker_id"]] > fair),
            reverse=True)
        targets = sorted(
            (loads[w["worker_id"]], w["worker_id"]) for w in active
            if loads[w["worker_id"]] < fair)
        if not donors or not targets:
            return
        target_loads = {wid: n for n, wid in targets}
        moves = 0
        by_owner: dict[str, list] = {}
        for l in lane_rows:
            by_owner.setdefault(l["owner_id"], []).append(l)
        for dload, donor in donors:
            if moves >= max_moves:
                break
            candidates = sorted(by_owner.get(donor, []),
                                key=lambda l: l["lane_id"])
            ti = 0
            while dload > fair and moves < max_moves and candidates:
                target = min(target_loads, key=lambda k: target_loads[k])
                if target_loads[target] >= fair:
                    break
                lane = candidates.pop(0)
                try:
                    res = self.store.coord_move_lane(
                        lane["lane_id"], target,
                        ttl=self.cfg.lease_ttl,
                        by_worker=self.worker.worker_id,
                        heartbeat_max_age=max(
                            self.cfg.lease_tick_interval * 3.0, 1.0))
                except StaleEpoch:
                    self.worker.metric("stale_write_rejected")
                    continue
                except LeaseNotOwned:
                    break
                moves += 1
                dload -= 1
                target_loads[target] += 1
                self.worker.metric("rebalance_handoff")
                self.worker.metric("orphan_recovered", res["requeued"])
                self.worker.wake.set()
                log.info("OWNERSHIP lane=%s old_owner=%s new_owner=%s "
                         "old_epoch=%d new_epoch=%d reason=rebalance_handoff "
                         "requeued=%d",
                         res["lane_id"], donor, target,
                         res["lease_epoch"] - 1, res["lease_epoch"],
                         res["requeued"])
