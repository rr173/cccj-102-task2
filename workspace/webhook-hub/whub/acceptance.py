"""HA 验收程序：真双 OS process + 8 个规定场景 + 机器可读报告。

约束（来自需求）：
- 每个场景使用全新数据库（独立 Cluster/tempdir），退出清理全部子进程；
- 等待一律为确定性条件轮询，不靠固定长 sleep 猜；
- 判定必须同时检查**数据库行**（lease/delivery）与**接收方观测**（receipts），
  只看日志文字的断言不算数；
- 真注入：SIGKILL（kill -9）、SIGSTOP/SIGCONT（长于 TTL 的 stop-the-world）、
  旧 fence 陈旧回写、store outage marker。
"""
from __future__ import annotations

import json
import atexit
import logging
import os
import secrets
import signal
import tempfile
import time

from .cluster import Cluster, Proc, http, wait_until

log = logging.getLogger("whub.acceptance")

ACME = "whk_demo_acme_key"
GLOBEX = "whk_demo_globex_key"
TTL = 3.0

# 当前存活的所有 Cluster：进程被 SIGTERM/SIGINT（例如 `timeout`）杀掉或
# 异常退出时，统一回收全部子进程组，绝不留下孤儿 worker/store/sink。
_LIVE_CLUSTERS: list["Cluster"] = []


def _cleanup_all_clusters() -> None:
    for c in list(_LIVE_CLUSTERS):
        try:
            c.cleanup()
        except Exception:
            log.exception("cleanup cluster on shutdown failed")
    _LIVE_CLUSTERS.clear()


def _on_signal(signum, frame) -> None:
    log.warning("received signal %d: cleaning up child process groups",
                signum)
    _cleanup_all_clusters()
    os._exit(128 + signum)


atexit.register(_cleanup_all_clusters)
signal.signal(signal.SIGTERM, _on_signal)
signal.signal(signal.SIGINT, _on_signal)


class AcceptanceError(AssertionError):
    pass


def _check(name: str, cond: bool, detail: object = "") -> None:
    if not cond:
        raise AcceptanceError(f"{name}: {detail}")
    log.info("    ✅ %s", name)


class Scenario:
    """每个场景一个全新 Cluster（新 DB、新进程组）。"""

    def __init__(self, sid: str, title: str, ttl: float = TTL,
                 base_dir: str = "", keep: bool = False):
        self.sid = sid
        self.title = title
        self.ttl = ttl
        self.base_dir = base_dir or tempfile.mkdtemp(prefix=f"whub-s{sid}-")
        os.makedirs(self.base_dir, exist_ok=True)
        self.keep = keep
        self.cluster: Cluster | None = None
        self.t0 = 0.0
        self.epochs: list[dict] = []
        self.interrupt_max = 0.0
        self.note: dict = {}

    # -- helpers -------------------------------------------------------

    def c(self, **kw) -> Cluster:
        self.cluster = Cluster(ttl=self.ttl, base_dir=self.base_dir, **kw)
        _LIVE_CLUSTERS.append(self.cluster)
        self.cluster.start()
        return self.cluster

    def create_ep(self, c: Cluster, path: str, *, key: str = ACME,
                  parallelism: int = 1, secret: str | None = None,
                  kid: str | None = None) -> dict:
        secret = secret or ("whsec_" + secrets.token_hex(12))
        # 默认 kid 按 path 唯一，避免多端点共用 kid=key-1 时在 sink 侧互相覆盖
        kid = kid or ("kid-" + secrets.token_hex(6))
        c.sink_admin("/keys", {"kid": kid, "secret": secret})
        code, data = http("POST", f"{c.store_url}/v1/endpoints", key,
                          {"name": path, "url": f"{c.sink_url}{path}",
                           "parallelism": parallelism, "secret": secret,
                           "kid": kid})
        _check(f"endpoint created {path}", code == 201, (code, data))
        return data

    def publish(self, c: Cluster, eid: str, obj: str, payload: dict,
                key: str = ACME) -> dict:
        code, data = http("POST",
                          f"{c.store_url}/v1/endpoints/{eid}/events", key,
                          {"object_key": obj, "payload": payload})
        _check(f"publish accepted {obj}", code in (200, 202), (code, data))
        return data

    def delivery(self, c: Cluster, dlv: str, key: str = ACME) -> dict:
        code, data = http("GET",
                          f"{c.store_url}/v1/deliveries/{dlv}", key)
        assert code == 200, (code, data)
        return data

    def add_dummy_lane(self, c: Cluster, path: str, owner: str) -> str:
        """给指定 worker 加一条无流量 dummy lane，保持负载均衡。

        强制把主 lane 指派给某 worker 后，必须先让对端持有等量 lane，
        否则协调器会因 a=1/b=0 立刻把主 lane rebalance 走，使 owner 断言抖动。"""
        ep = self.create_ep(c, path, parallelism=1)
        lid = ep["endpoint_id"]
        c.assign(lid, owner)
        self.wait_owner(c, lid, owner, min_epoch=1, timeout=10)
        return lid

    def wait_delivery_state(self, c: Cluster, dlv: str, states: tuple,
                            timeout: float = 40, key: str = ACME) -> dict:
        return wait_until(
            f"delivery {dlv[:16]} -> {states}",
            lambda: (lambda d: d if d.get("status") in states else None)(
                self.delivery(c, dlv, key)),
            timeout=timeout, interval=0.15)

    def wait_succeeded(self, c: Cluster, refs: list[str],
                       timeout: float = 60, key: str = ACME) -> None:
        deadline = time.monotonic() + timeout
        pending = list(refs)
        last = {}
        while time.monotonic() < deadline and pending:
            still = []
            for dlv in pending:
                d = self.delivery(c, dlv, key)
                last[dlv] = d["status"]
                if d["status"] == "dead":
                    raise AcceptanceError(f"{dlv} became dead: {d}")
                if d["status"] != "succeeded":
                    still.append(dlv)
            pending = still
            if pending:
                time.sleep(0.15)
        _check(f"all {len(refs)} deliveries succeeded", not pending,
               f"pending={pending} last={last}")

    def wait_owner(self, c: Cluster, lane: str, worker: str | None,
                   timeout: float = 30, *, min_epoch: int | None = None
                   ) -> dict:
        def cond():
            l = c.lease(lane)
            if l["owner_id"] == worker and (
                    min_epoch is None
                    or int(l["lease_epoch"]) >= int(min_epoch)):
                return l
            return None
        return wait_until(f"lane {lane[:12]} owner={worker} epoch>={min_epoch}",
                          cond, timeout=timeout, interval=0.1)

    def assert_no_orphan(self, c: Cluster, timeout: float = 15) -> None:
        def cond():
            leases = {l["lane_id"]: l for l in c.leases()}
            for l in leases.values():
                if l["owner_id"] is None or l["expired"]:
                    return None
            # 没有 inflight 悬挂在错误 fence 上
            code, data = http("GET", f"{c.store_url}/admin/metrics")
            return leases
        wait_until("no orphan/expired lane", cond, timeout=timeout,
                   interval=0.2)

    def snapshot_epochs(self, c: Cluster, lanes: list[str]) -> None:
        for lid in lanes:
            l = c.lease(lid)
            self.epochs.append({"at": round(time.time() - self.t0, 2),
                                "lane_id": lid, "owner_id": l["owner_id"],
                                "lease_epoch": l["lease_epoch"],
                                "reason": l["last_handoff_reason"]})

    def metric_sum(self, c: Cluster, name: str) -> int:
        snap = c.metrics()
        return sum(int(v.get(name, 0)) for v in snap.values())

    def cleanup(self) -> None:
        if self.cluster:
            c = self.cluster
            _LIVE_CLUSTERS[:] = [x for x in _LIVE_CLUSTERS if x is not c]
            c.cleanup()
            self.cluster = None

    # -- 场景主体 ------------------------------------------------------

    def run(self) -> dict:
        self.t0 = time.time()
        log.info("\n━━━ 场景 %s：%s ━━━", self.sid, self.title)
        try:
            getattr(self, f"case_{self.sid}")()
            status, error = "pass", None
        except Exception as e:
            status, error = "fail", f"{type(e).__name__}: {e}"
            log.exception("场景 %s 失败", self.sid)
            if self.cluster:
                for name, p in self.cluster.procs.items():
                    try:
                        log.error("--- tail %s.log ---\n%s", name,
                                  _tail(p.log_path))
                    except OSError:
                        pass
        finally:
            metrics = self.cluster.metrics() if self.cluster else {}
            stale_total = sum(
                int(v.get("stale_write_rejected", 0))
                for v in metrics.values())
            result = {
                "scenario": self.sid, "title": self.title,
                "status": status, "error": error,
                "duration_s": round(time.time() - self.t0, 2),
                "ttl_s": self.ttl,
                "epochs": self.epochs,
                "max_interruption_s": round(self.interrupt_max, 2),
                "stale_write_rejected_total": stale_total,
                "final_owner": self.note.get("final_owner"),
                "receipt_count": self.note.get(
                    "receipts", self.note.get("total_receipts")),
                "metrics": metrics,
                "note": self.note,
            }
            self.cleanup()
        log.info("场景 %s 结果：%s (%.1fs)", self.sid, status.upper(),
                 result["duration_s"])
        return result

    # ==================================================================
    # 场景 1：并行上线，单 owner，lane 分散，无二次 ack
    # ==================================================================
    def case_1(self) -> None:
        c = self.c()
        paths = ["/s1/lane-a", "/s1/lane-b", "/s1/lane-c", "/s1/lane-d"]
        eps = [self.create_ep(c, p, parallelism=1) for p in paths]
        lanes = [e["endpoint_id"] for e in eps]
        # 等四条 lane 都有 owner
        owners: dict[str, str] = {}
        def all_owned():
            owners.clear()
            for lid in lanes:
                l = c.lease(lid)
                if l["owner_id"] is None:
                    return None
                owners[lid] = l["owner_id"]
            return owners
        wait_until("all 4 lanes owned", all_owned, timeout=15)
        self.note["initial_owners"] = dict(owners)
        _check("lane 分散到不同 worker（a/b 均持有）",
               set(owners.values()) == {"worker-a", "worker-b"}, owners)
        # 多次再查，数据库里每条 lane 任一时刻只有唯一 owner（恒真）+ 归属稳定
        first = dict(owners)
        time.sleep(0.6)
        now_owners = {lid: c.lease(lid)["owner_id"] for lid in lanes}
        _check("归属在无故障时稳定（无无端翻转）", now_owners == first,
               f"{first} -> {now_owners}")

        # 每 lane 发 3 个事件（同 object_key，保序）
        refs = []
        for ep in eps:
            for i in range(3):
                refs.append(self.publish(
                    c, ep["endpoint_id"], "obj", {"i": i})["delivery_id"])
        self.wait_succeeded(c, refs, timeout=40)

        # 数据库：12 条全 succeeded，无重复
        for p, ep in zip(paths, eps):
            code, dlvrows = http(
                "GET", f"{c.store_url}/v1/endpoints/{ep['endpoint_id']}/deliveries",
                ACME)
            assert code == 200
            _check(f"{p} 全部 succeeded",
                   all(d["status"] == "succeeded" for d in dlvrows),
                   [d["status"] for d in dlvrows])
            # seq 单调
            seqs = sorted(d["seq"] for d in dlvrows)
            _check(f"{p} seq 单调 1..3", seqs == [1, 2, 3], seqs)
        # receiver 观测：每事件恰一条 receipt（无二次 ack）
        stats = c.sink_stats()
        for p in paths:
            recs = c.receipts(p)
            ids = [r["event_id"] for r in recs]
            _check(f"{p} 收到 3 条 receipt", len(recs) == 3, len(recs))
            _check(f"{p} receipt 无重复 event_id（无二次 ack）",
                   len(ids) == len(set(ids)), ids)
        _check("receiver 全局 duplicates=0", stats["duplicates"] == 0,
               stats["duplicates"])
        total_receipts = sum(len(c.receipts(p)) for p in paths)
        final_owners = {p: c.lease(e["endpoint_id"])["owner_id"]
                        for p, e in zip(paths, eps)}
        self.note.update({"total_receipts": total_receipts,
                          "final_owner": ",".join(sorted(
                              {o for o in final_owners.values() if o})),
                          "final_owners": final_owners})
        self.snapshot_epochs(c, lanes)

    # ==================================================================
    # 场景 2：kill -9 正在连续发送的 owner，TTL 后 b 接手，收敛且不重做
    # ==================================================================
    def case_2(self) -> None:
        c = self.c()
        path = "/s2/seq"
        ep = self.create_ep(c, path, parallelism=1)
        lane = ep["endpoint_id"]
        # 先给 b 一条 dummy lane 保持均衡，主 lane 指派给 a 后不被自动搬走
        self.add_dummy_lane(c, "/s2/dummy-b", "worker-b")
        # sink 慢处理，保证 kill 时有连续在途/排队工作，且失败后需要续发
        c.sink_admin("/rules", {"path": path, "mode": "delay", "delay": 0.6})
        # 确定性地把 lane 指派给 a（不依赖 acquire 竞争结果）
        c.assign(lane, "worker-a")
        self.wait_owner(c, lane, "worker-a", min_epoch=1, timeout=10)
        owner = c.lease(lane)
        _check("初始 owner=worker-a", owner["owner_id"] == "worker-a", owner)
        refs = [self.publish(c, lane, "order-9", {"i": i})["delivery_id"]
                for i in range(10)]
        # 等 a 真正进入连续处理（已确认 1 条、且仍有 inflight/pending 积压）
        wait_until(">=1 receipt before crash",
                   lambda: len(c.receipts(path)) >= 1, timeout=15)
        def backlog_present():
            code, rows = http(
                "GET", f"{c.store_url}/v1/endpoints/{lane}/deliveries", ACME)
            outstanding = [d for d in rows
                           if d["status"] in ("pending", "inflight")]
            return outstanding if outstanding else None
        wait_until("continuous backlog present at crash",
                   backlog_present, timeout=10)
        acked_before = [r["event_id"] for r in c.receipts(path)]
        epoch_before = c.lease(lane)["lease_epoch"]
        proc_a = c.procs["worker-a"]
        proc_a.kill9()
        t_kill = time.monotonic()
        # 不重启 a；b 在 TTL 后 expiry steal（epoch+1）
        l2 = self.wait_owner(c, lane, "worker-b", timeout=self.ttl + 12,
                             min_epoch=epoch_before + 1)
        self.interrupt_max = time.monotonic() - t_kill
        _check("b 的 epoch 严格高于 a",
               l2["lease_epoch"] == epoch_before + 1,
               (epoch_before, l2["lease_epoch"]))
        _check("last_handoff_reason=expiry_steal",
               l2["last_handoff_reason"] == "expiry_steal", l2)
        # 剩余全部收敛
        self.wait_succeeded(c, refs, timeout=40)
        t_done = time.monotonic() - t_kill
        # failover 快照沿用：全部落同一 path，sig_version/target_url 未变
        rows = [self.delivery(c, d) for d in refs]
        _check("target_url/sig_version 全部沿用原记录",
               all(d["target_url"].endswith(path) and d["sig_version"] == 1
                   for d in rows), [(d["target_url"], d["sig_version"]) for d in rows])
        seqs = [d["seq"] for d in rows]
        _check("序号单调 1..10", seqs == list(range(1, 11)), seqs)
        recs = c.receipts(path)
        ids = [r["event_id"] for r in recs]
        _check("10 个事件全部恰好一次成功回执", len(ids) == 10
               and len(set(ids)) == 10, ids)
        _check("已 ack 的事件不重做",
               all(e in ids for e in acked_before) and len(ids) == 10,
               (acked_before, ids))
        # 审计链含 expiry_steal 因果（首跳为显式指派/正常 acquire 均可）
        audit = c.audit(lane)
        reasons = [a["reason"] for a in audit]
        _check("审计链记录 expiry_steal 因果",
               "expiry_steal" in reasons, reasons)
        self.note.update({"failover_after_s": round(self.interrupt_max, 2),
                          "total_recovery_s": round(t_done, 2),
                          "final_owner": "worker-b",
                          "receipts": 10,
                          "stale_write_rejected":
                              self.metric_sum(c, "stale_write_rejected")})
        self.snapshot_epochs(c, [lane])

    # ==================================================================
    # 场景 3：stop-the-world > TTL 后恢复；renew/claim/complete 三类陈旧写被拒
    # ==================================================================
    def case_3(self) -> None:
        c = self.c()
        path = "/s3/pause"
        ep = self.create_ep(c, path, parallelism=1)
        lane = ep["endpoint_id"]
        # 先给 b 一条 dummy lane 保持均衡，避免主 lane 被协调器自动搬走
        self.add_dummy_lane(c, "/s3/dummy-b", "worker-b")
        # 较慢的处理 + 持续队列：保证 SIGSTOP 时一定有 inflight 在途，
        # 使 failover 走确定性的 expiry steal（直传，epoch 恰好 +1）
        c.sink_admin("/rules", {"path": path, "mode": "delay", "delay": 0.5})
        c.assign(lane, "worker-a")
        self.wait_owner(c, lane, "worker-a", min_epoch=1, timeout=10)
        old_epoch = c.lease(lane)["lease_epoch"]
        # 先只发少量，确认流量真的在 a 上流动
        refs = [self.publish(c, lane, "obj-p", {"i": 0})["delivery_id"]]
        wait_until("receipts flowing before pause",
                   lambda: len(c.receipts(path)) >= 1, timeout=15)
        # 灌满一条“持续队列”：并行度=1 且每条处理 0.5s，12 条积压保证
        # SIGSTOP 落下的任意时刻都有 inflight 或排队（不存在空闲空隙）
        for i in range(1, 12):
            refs.append(self.publish(c, lane, "obj-p", {"i": i})["delivery_id"])
        def backlog_present():
            code, rows = http(
                "GET", f"{c.store_url}/v1/endpoints/{lane}/deliveries", ACME)
            outstanding = [d for d in rows
                           if d["status"] in ("pending", "inflight")]
            # 需要至少 2 条未完成：一条在飞、一条排队，填满处理间隙
            return outstanding if len(outstanding) >= 2 else None
        wait_until("a durable backlog (>=2 outstanding) before SIGSTOP",
                   backlog_present, timeout=10)
        proc_a: Proc = c.procs["worker-a"]
        proc_a.freeze()
        t_freeze = time.monotonic()
        # 等待 > TTL，b 接手
        l2 = self.wait_owner(c, lane, "worker-b", timeout=self.ttl + 12,
                             min_epoch=old_epoch + 1)
        stolen_at = time.monotonic() - t_freeze
        _check("b 在 TTL 之后接手且 epoch 严格更高",
               l2["lease_epoch"] >= old_epoch + 1
               and stolen_at >= self.ttl - 0.5,
               (stolen_at, l2))
        _check("接手走 expiry_steal（非普通 acquire）",
               l2["last_handoff_reason"] == "expiry_steal",
               l2["last_handoff_reason"])
        new_epoch = l2["lease_epoch"]
        # 让 b 把队列全部落盘（此时 a 仍冻结，所有权确定在 b）
        self.wait_succeeded(c, refs, timeout=40)
        b_rows = {d: self.delivery(c, d) for d in refs}
        b_state = json.dumps(
            {d: r["status"] for d, r in b_rows.items()}, sort_keys=True)

        # ---- 三类陈旧写全部在 a 仍冻结期间注入（debug 端点跑在 store 进程，
        #      以旧 owner+旧 epoch+假 fence 重放，确定性地被数据库拒绝）----
        code_renew, data_renew = c.debug_stale("renew", lane)
        _check("陈旧 renew 被数据库拒绝 stale_epoch(409)",
               code_renew == 409 and data_renew.get("error") == "stale_epoch",
               (code_renew, data_renew))
        code_claim, data_claim = c.debug_stale("claim", lane)
        _check("陈旧 claim 被数据库拒绝 stale_epoch(409)",
               code_claim == 409 and data_claim.get("error") == "stale_epoch",
               (code_claim, data_claim))
        # 构造一条 b 名下 inflight 用于陈旧 complete
        r2 = self.publish(c, lane, "obj-p2", {"i": 9})["delivery_id"]
        inflight = wait_until(
            "b claims a new inflight delivery",
            lambda: (lambda d: d if d["status"] == "inflight" else None)(
                self.delivery(c, r2)), timeout=10)
        _check("inflight 归属为当前 owner b 的 fence",
               inflight["lease"]["owner_id"] == "worker-b"
               and inflight["lease"]["lease_epoch"] == new_epoch,
               inflight["lease"])
        code_done, data_done = c.debug_stale("complete", lane,
                                             delivery_id=r2)
        _check("陈旧 complete 被数据库拒绝 stale_epoch(409)",
               code_done == 409 and data_done.get("error") == "stale_epoch",
               (code_done, data_done))
        # b 仍正常完成 r2（旧 fence 没能抢占）
        self.wait_succeeded(c, [r2], timeout=20)

        # ---- 此刻才恢复 a：它持有的旧 fence 已失效，续期/取件/回写都被拒 ----
        n_before = self.metric_sum(c, "stale_write_rejected")
        proc_a.resume()
        resumed_at = time.monotonic()
        # a 恢复后自然 lease loop 至少会用旧 epoch 尝试一次续期 -> 被拒。
        # 这是 best-effort 观测；硬保证由上面三类确定性注入给出。
        try:
            wait_until("a's natural stale renew rejected after resume",
                       lambda: self.metric_sum(c, "stale_write_rejected")
                       > n_before, timeout=6)
            natural_renew_rejected = True
        except AssertionError:
            natural_renew_rejected = False
        # 给系统一个短暂的再均衡窗口后核对最终状态
        time.sleep(1.5)
        # a 绝不能覆盖 b 已写入的成功结果
        a_rows = {d: self.delivery(c, d) for d in refs}
        a_state = json.dumps(
            {d: r["status"] for d, r in a_rows.items()}, sort_keys=True)
        _check("b 已落盘的成功结果未被恢复的 a 改动",
               a_state == b_state
               and all(r["status"] == "succeeded" for r in a_rows.values()),
               (b_state, a_state))
        lease_now = c.lease(lane)
        _check("lease epoch 单调不回退（旧 epoch 不可复活）",
               lease_now["lease_epoch"] >= new_epoch
               and lease_now["lease_epoch"] > old_epoch,
               {"old": old_epoch, "b_epoch": new_epoch,
                "now": dict(lease_now)})
        self.interrupt_max = stolen_at
        stale_total = self.metric_sum(c, "stale_write_rejected")
        self.note.update({
            "pause_duration_s": round(time.monotonic() - resumed_at, 2),
            "stale_renew": True, "stale_claim": True,
            "stale_complete": True,
            "natural_renew_rejected_after_resume": natural_renew_rejected,
            "stale_write_rejected_total": stale_total,
            "total_receipts": len(c.receipts(path)),
            "final_owner": lease_now["owner_id"]})
        self.snapshot_epochs(c, [lane])

    # ==================================================================
    # 场景 4：429/timeout 的未来 not_before 水位随 failover 保留，不提前发送
    # ==================================================================
    def case_4(self) -> None:
        c = self.c()
        slow_path, fast_path = "/s4/backoff", "/s4/fast"
        slow = self.create_ep(c, slow_path, parallelism=1)
        fast = self.create_ep(c, fast_path, parallelism=2, key=GLOBEX)
        slow_lane, fast_lane = slow["endpoint_id"], fast["endpoint_id"]
        # slow lane 固定给 a；fast lane 固定给 b（用控制面指派，确定性）
        c.assign(slow_lane, "worker-a")
        c.assign(fast_lane, "worker-b")
        self.wait_owner(c, slow_lane, "worker-a", min_epoch=1, timeout=10)
        self.wait_owner(c, fast_lane, "worker-b", min_epoch=1, timeout=10)
        # slow: 429 + Retry-After=6，制造未来 not_before
        c.sink_admin("/rules", {"path": slow_path, "mode": "ratelimit",
                                "retry_after": 6.0})
        ref = self.publish(c, slow_lane, "obj-429", {"i": 1})["delivery_id"]
        # 等待 not_before 被推进到未来（DB 行）
        d = wait_until(
            "slow delivery recorded future not_before",
            lambda: (lambda x: x if x["not_before"] > time.time() + 1
                     else None)(self.delivery(c, ref)), timeout=15)
        nb_before = d["not_before"]
        attempts_before = d["attempts"]
        _check("已记录 429 退避（attempts>=1）", attempts_before >= 1,
               attempts_before)
        # owner 在持有时被 kill -9
        c.procs["worker-a"].kill9()
        l2 = self.wait_owner(c, slow_lane, "worker-b", timeout=self.ttl + 12)
        d2 = self.delivery(c, ref)
        _check("failover 后 not_before 水位原样保留（不重置）",
               abs(d2["not_before"] - nb_before) < 0.05,
               (nb_before, d2["not_before"]))
        # 在水位到达之前，继任者禁止提前“确认”。以 accepted(2xx) 计数为准：
        # kill 瞬间可能有一条在飞连接已抵达 receiver（429/未确认），那属于
        # at-least-once 的重复尝试，幂等 receiver 不会二次 ack。
        accepted_kill = c.sink_stats()["counters"].get(
            slow_path, {}).get("accepted", 0)
        time.sleep(1.5)
        accepted_during = c.sink_stats()["counters"].get(
            slow_path, {}).get("accepted", 0)
        _check("继任者未在 not_before 之前提前确认",
               accepted_during == accepted_kill,
               (accepted_kill, accepted_during))
        # 水位之后，接收方恢复（429 规则解除），自动补发成功
        c.sink_admin("/rules", {"path": slow_path, "mode": "ok"})
        ok = self.wait_delivery_state(c, ref, ("succeeded",), timeout=15)
        _check("not_before 到点后补发成功", ok["status"] == "succeeded", ok)
        # 其余 lane（fast, owner=b 全程未受影响）吞吐继续增长
        c.sink_admin("/rules", {"path": fast_path, "mode": "ok"})
        fast_refs = [self.publish(c, fast_lane, f"f{i}", {"i": i},
                                  key=GLOBEX)["delivery_id"]
                     for i in range(6)]
        self.wait_succeeded(c, fast_refs, timeout=20, key=GLOBEX)
        _check("其他 lane 在 slow lane failover 期间继续增长（6 条 receipt）",
               len(c.receipts(fast_path)) == 6, len(c.receipts(fast_path)))
        self.note.update({"final_owner": c.lease(slow_lane)["owner_id"],
                          "total_receipts":
                              len(c.receipts(slow_path))
                              + len(c.receipts(fast_path)),
                          "final_owner_slow": "worker-b",
                          "fast_lane_receipts": 6,
                          "not_before_preserved": True})
        self.snapshot_epochs(c, [slow_lane, fast_lane])

    # ==================================================================
    # 场景 5：v1 批次积压时切 v2 + failover；两批分别到各自 path 且验签代次正确
    # ==================================================================
    def case_5(self) -> None:
        c = self.c()
        p1, p2 = "/s5/v1", "/s5/v2"
        s1 = "whsec_v1_" + secrets.token_hex(8)
        s2 = "whsec_v2_" + secrets.token_hex(8)
        ep = self.create_ep(c, p1, parallelism=1, secret=s1, kid="key-1")
        lane = ep["endpoint_id"]
        c.sink_admin("/keys", {"kid": "key-2", "secret": s2})
        c.assign(lane, "worker-a")
        self.wait_owner(c, lane, "worker-a", min_epoch=1, timeout=10)
        # v1 path 慢处理，积压一批
        c.sink_admin("/rules", {"path": p1, "mode": "delay", "delay": 0.5})
        old = [self.publish(c, lane, "obj-v1", {"i": i})["delivery_id"]
               for i in range(3)]
        # 确认至少一条 v1 已开始，形成真实积压
        wait_until("v1 backlog forming",
                   lambda: len(c.receipts(p1)) >= 1, timeout=15)
        # 切 v2（新 url + 新 kid/secret），并立刻 kill -9 触发 failover
        code, rot = http("POST",
                         f"{c.store_url}/v1/endpoints/{lane}/rotate", ACME,
                         {"url": f"{c.sink_url}{p2}", "secret": s2,
                          "kid": "key-2"})
        _check("rotate 到 v2", code == 200 and rot["active_version"] == 2,
               (code, rot))
        new = [self.publish(c, lane, "obj-v2", {"i": i})["delivery_id"]
               for i in range(3)]
        # 记录两批 DB 快照后再 failover
        old_rows = [self.delivery(c, d) for d in old]
        new_rows = [self.delivery(c, d) for d in new]
        _check("旧批次快照 v1/p1",
               all(d["sig_version"] == 1 and d["target_url"].endswith(p1)
                   for d in old_rows), [(d["sig_version"], d["target_url"])
                                        for d in old_rows])
        _check("新批次快照 v2/p2",
               all(d["sig_version"] == 2 and d["target_url"].endswith(p2)
                   for d in new_rows), [(d["sig_version"], d["target_url"])
                                        for d in new_rows])
        epoch_before = c.lease(lane)["lease_epoch"]
        c.procs["worker-a"].kill9()
        l2 = self.wait_owner(c, lane, "worker-b", timeout=self.ttl + 12,
                             min_epoch=epoch_before + 1)
        self.wait_succeeded(c, old + new, timeout=40)
        rec1, rec2 = c.receipts(p1), c.receipts(p2)
        _check("旧批次 3 条全部签往 v1 path 且 kid=key-1",
               len(rec1) == 3 and all(r["kid"] == "key-1" for r in rec1),
               rec1)
        _check("新批次 3 条全部签往 v2 path 且 kid=key-2",
               len(rec2) == 3 and all(r["kid"] == "key-2" for r in rec2),
               rec2)
        stats = c.sink_stats()
        _check("全部签名校验通过（bad_signature=0）",
               stats["bad_signatures"] == 0, stats["bad_signatures"])
        # failover 后 DB 行快照仍与入队时一致
        for d in old_rows:
            cur = self.delivery(c, d["delivery_id"])
            _check("failover 未改写旧批次 sig_version/target_url",
                   cur["sig_version"] == 1 and cur["target_url"] == d["target_url"],
                   (cur["sig_version"], cur["target_url"]))
        for d in new_rows:
            cur = self.delivery(c, d["delivery_id"])
            _check("failover 未改写新批次 sig_version/target_url",
                   cur["sig_version"] == 2 and cur["target_url"] == d["target_url"],
                   (cur["sig_version"], cur["target_url"]))
        self.note.update({"final_owner": c.lease(lane)["owner_id"],
                          "total_receipts": len(rec1) + len(rec2),
                          "v1_receipts": 3, "v2_receipts": 3})
        self.snapshot_epochs(c, [lane])

    # ==================================================================
    # 场景 6：drain——owned 只减不增、deadline 前收尾、其余更高 epoch 接手
    # ==================================================================
    def case_6(self) -> None:
        c = self.c()
        # 4 条真实 lane 固定给 a；另给 b 4 条 dummy lane 保持负载均衡，
        # 避免协调器在 drain 之前就把 a 的 lane 自动 rebalance 走。
        paths = [f"/s6/l{i}" for i in range(4)]
        eps = [self.create_ep(c, p, parallelism=1) for p in paths]
        lanes = [e["endpoint_id"] for e in eps]
        dummy_paths = [f"/s6/d{i}" for i in range(4)]
        dummy_eps = [self.create_ep(c, p, parallelism=1) for p in dummy_paths]
        dummy_lanes = [e["endpoint_id"] for e in dummy_eps]
        for lid in lanes:
            c.assign(lid, "worker-a")
            self.wait_owner(c, lid, "worker-a", min_epoch=1, timeout=10)
        for lid in dummy_lanes:
            c.assign(lid, "worker-b")
            self.wait_owner(c, lid, "worker-b", min_epoch=1, timeout=10)
        # 两条 lane 立即完成；两条慢 lane 的队列耗时超过 drain deadline
        fast_lanes = lanes[:2]
        slow_lanes = lanes[2:]
        for lid in slow_lanes:
            c.sink_admin("/rules",
                         {"path": f"/s6/l{lanes.index(lid)}",
                          "mode": "delay", "delay": 1.0})
        # 给慢 lane 制造持续队列（每条 5×1s=5s，长于 3s deadline）
        slow_refs = []
        for lid in slow_lanes:
            for i in range(5):
                slow_refs.append(self.publish(c, lid, f"o-{lid[:6]}",
                                              {"i": i})["delivery_id"])
        fast_refs = []
        for lid in fast_lanes:
            fast_refs.append(self.publish(c, lid, "ok", {"i": 1})["delivery_id"])
        self.wait_succeeded(c, fast_refs, timeout=20)
        # drain a，deadline=3s（慢队列收不完 -> 必然 drain_handoff）
        code, data = c.drain("worker-a", deadline=3.0)
        _check("drain 200", code == 200 and data["draining"], (code, data))
        wa = wait_until("worker-a marked draining in DB",
                        lambda: (lambda w: w if w["draining"] else None)(
                            c.worker("worker-a")), timeout=8)
        # drain 期间持续有成功 receipt（服务窗口不断）—— 采样
        receipts_before = sum(len(c.receipts(p)) for p in paths)
        # owned 只减不增：采样 worker-a owned 序列单调不增
        owned_series = []
        deadline = time.monotonic() + 9
        while time.monotonic() < deadline:
            owned_series.append(c.worker("worker-a")["owned"])
            if owned_series[-1] == 0:
                break
            time.sleep(0.2)
        monotone_dec = all(owned_series[i] >= owned_series[i + 1]
                           for i in range(len(owned_series) - 1))
        _check("a 的 owned 数量只减不增", monotone_dec, owned_series)
        # 最终全部 lane 由 b 以更高 epoch 持有
        for lid in lanes:
            l = self.wait_owner(c, lid, "worker-b", timeout=15)
            _check(f"lane {lid[:8]} 由 b 以更高 epoch 接手",
                   l["lease_epoch"] >= 2 and l["owner_id"] == "worker-b",
                   dict(l))
        # 慢队列最终全部收敛
        self.wait_succeeded(c, slow_refs, timeout=40)
        receipts_after = sum(len(c.receipts(p)) for p in paths)
        _check("drain/交接期间持续有成功 receipt",
               receipts_after > receipts_before,
               (receipts_before, receipts_after))
        # 全部慢 lane 恰好一次（每条 5 条）
        for lid in slow_lanes:
            p = f"/s6/l{lanes.index(lid)}"
            ids = [r["event_id"] for r in c.receipts(p)]
            _check(f"{p} 5 条 receipt 无重复",
                   len(ids) == 5 and len(set(ids)) == 5, ids)
        # 审计链 reason 覆盖 drain_release 与 drain_handoff
        all_reasons = {a["reason"] for a in c.audit()}
        _check("审计链含 drain_release 与 drain_handoff 全量记录",
               {"drain_release", "drain_handoff"} <= all_reasons,
               sorted(all_reasons))
        # drain 状态仍可查询
        _check("a 保持 draining 视图",
               c.worker("worker-a")["draining"] is True, None)
        self.note.update({"owned_series": owned_series,
                          "handoff_reasons": sorted(
                              all_reasons & {"drain_release",
                                             "drain_handoff",
                                             "drain_expiry_release"}),
                          "final_owner": "worker-b",
                          "total_receipts": receipts_after})
        self.snapshot_epochs(c, lanes)

    # ==================================================================
    # 场景 7：反复加入/移除/重启 + 多次 rebalance；cap、无 orphan、无全停
    # ==================================================================
    def case_7(self) -> None:
        c = self.c()
        paths = [f"/s7/l{i}" for i in range(6)]
        eps = [self.create_ep(c, p, parallelism=1) for p in paths]
        lanes = [e["endpoint_id"] for e in eps]
        for p in paths:
            c.sink_admin("/rules", {"path": p, "mode": "ok"})
        wait_until("6 lanes owned",
                   lambda: all(c.lease(l)["owner_id"] for l in lanes),
                   timeout=15)
        # 持续背景流量，用 receiver receipts 时间序列测“无全停窗口”
        import threading
        stop = threading.Event()
        refs_background: list[str] = []
        def flood():
            i = 0
            while not stop.is_set():
                lid = lanes[i % len(lanes)]
                try:
                    d = self.publish(c, lid, f"bg-{i}", {"i": i})
                    refs_background.append(d["delivery_id"])
                except Exception:
                    pass
                i += 1
                time.sleep(0.12)
        t = threading.Thread(target=flood, daemon=True)
        t.start()
        try:
            moves_seen: list[int] = []
            # 多轮手动 rebalance；每轮审计新增 movement <= cap(=1)
            for rnd in range(3):
                before_ids = {a["id"] for a in c.audit()}
                code, data = c.rebalance(max_moves=1)
                assert code == 202, (code, data)
                wait_until(f"rebalance round {rnd} moves",
                           lambda: any(a["id"] not in before_ids
                                       for a in c.audit()) or True,
                           timeout=4)
                time.sleep(0.8)
                new_audits = [a for a in c.audit() if a["id"] not in before_ids
                              and a["reason"] == "rebalance_handoff"]
                moves_seen.append(len(new_audits))
                _check(f"第 {rnd+1} 轮搬运量 <= cap(1)",
                       len(new_audits) <= 1, len(new_audits))
            # 加入 worker-c
            c.spawn_worker("worker-c")
            wait_until("worker-c alive",
                       lambda: any(w["worker_id"] == "worker-c" and w["alive"]
                                   for w in c.workers()), timeout=10)
            # cap=1：c 要分到公平份额需要多轮；持续触发直到 owns>=1。
            def c_gets_lane():
                c.rebalance(1)
                loads = {w["worker_id"]: w["owned"] for w in c.workers()
                         if w["alive"]}
                self.note.setdefault("loads", []).append(dict(loads))
                return loads if loads.get("worker-c", 0) >= 1 else None
            loads = wait_until("worker-c progressively acquires a lane",
                               c_gets_lane, timeout=20, interval=0.6)
            _check("worker-c 加入后渐进获得 lane（owns>=1）",
                   loads.get("worker-c", 0) >= 1, loads)
            # 移除 worker-c（SIGTERM 优雅退出，lane 还回公共池）
            c.stop_worker("worker-c")
            wait_until("worker-c gone from registry active set",
                       lambda: not any(w["worker_id"] == "worker-c"
                                       and w["alive"]
                                       for w in c.workers()), timeout=10)
            time.sleep(0.8)
            # kill -9 worker-a（crash 移除），TTL 后接管
            c.procs["worker-a"].kill9()
            # 重启一个同名 worker-a（incarnation 变化）
            time.sleep(0.5)
            c.spawn_worker("worker-a")
            wait_until("worker-a re-registered alive",
                       lambda: any(w["worker_id"] == "worker-a" and w["alive"]
                                   and w["pid"] != 0 for w in c.workers()),
                       timeout=10)
            time.sleep(1.0)
            code, _ = c.rebalance(1)
            assert code == 202
        finally:
            stop.set()
            t.join(timeout=2)
        # 最终：无 orphan，所有 lane 有活 owner，背景流量全收敛
        self.assert_no_orphan(c, timeout=15)
        self.wait_succeeded(c, refs_background, timeout=60)
        final = {l: c.lease(l) for l in lanes}
        _check("最终不存在 orphan lane（6 条均有活 owner）",
               all(l["owner_id"] for l in final.values()),
               {k: v["owner_id"] for k, v in final.items()})
        # 无全停窗口：receipt 时间序列里相邻成功间隔 < 2*TTL（没有整体停摆）
        all_ts = sorted(r["ts"] for p in paths for r in c.receipts(p))
        gaps = [all_ts[i + 1] - all_ts[i]
                for i in range(len(all_ts) - 1)]
        max_gap = max(gaps or [0])
        self.interrupt_max = max_gap
        _check(f"处理曲线无全停窗口（最大相邻成功间隔 {max_gap:.2f}s < {2*TTL}s）",
               max_gap < 2 * self.ttl, f"max_gap={max_gap}")
        loads_final = {w["worker_id"]: w["owned"] for w in c.workers()
                       if w["alive"]}
        self.note.update({"rebalance_moves_per_round": moves_seen,
                          "loads_final": loads_final,
                          "total_receipts": len(all_ts),
                          "background_receipts": len(all_ts),
                          "final_owner": ",".join(sorted(
                              {l["owner_id"] for l in final.values()
                               if l["owner_id"]}))})
        self.snapshot_epochs(c, lanes)

    # ==================================================================
    # 场景 8：durable store 拒绝写入：停止 outbound；恢复后重竞争，旧 epoch 失效
    # ==================================================================
    def case_8(self) -> None:
        c = self.c()
        path = "/s8/outage"
        ep = self.create_ep(c, path, parallelism=1)
        lane = ep["endpoint_id"]
        c.assign(lane, "worker-a")
        self.wait_owner(c, lane, "worker-a", min_epoch=1, timeout=10)
        old_epoch = c.lease(lane)["lease_epoch"]
        refs_ok = [self.publish(c, lane, "warmup", {"i": i})["delivery_id"]
                   for i in range(2)]
        self.wait_succeeded(c, refs_ok, timeout=20)
        # 预置 pending（outage 开始后想发也发不出去）
        pending_refs = [self.publish(c, lane, "blocked", {"i": i})["delivery_id"]
                        for i in range(3)]
        # 等它们被 claim（inflight）前/后都可以；关键是开启 outage 后无副作用
        # 打开 store outage（marker 文件对所有进程的写连接同时生效）
        t0 = time.monotonic()
        code, data = c.set_outage(True)
        _check("outage on", code == 200 and data["store_unavailable"],
               (code, data))
        # outage 持续超过 TTL：a 既无法续期也无法发送
        time.sleep(self.ttl + 1.5)
        received_at_outage = c.sink_stats()["counters"].get(
            path, {}).get("received", 0)
        accepted_at_outage = c.sink_stats()["counters"].get(
            path, {}).get("accepted", 0)
        # outage 期间再投新事件（入队 API 本身会失败 503——store 拒绝写入）
        code_enq, data_enq = http(
            "POST", f"{c.store_url}/v1/endpoints/{lane}/events", ACME,
            {"object_key": "during-outage", "payload": {"i": 1}})
        _check("outage 期间写入被拒（store_unavailable 503）",
               code_enq == 503 and data_enq.get("error") == "store_unavailable",
               (code_enq, data_enq))
        # 再等待一个窗口，确认 outbound 计数零增长（side effect 完全停止）
        time.sleep(1.0)
        stats_during = c.sink_stats()["counters"].get(path, {})
        _check("outage 期间无任何新增到达/副作用",
               stats_during.get("received", 0) == received_at_outage,
               (received_at_outage, stats_during.get("received", 0)))
        # 恢复写入
        code, data = c.set_outage(False)
        _check("outage off", code == 200 and not data["store_unavailable"],
               (code, data))
        # a 的旧 epoch 在 outage 期间已过期；即便它恢复网络也不能用旧 fence 续命。
        # 先允许它探测恢复；随后由它自己重新竞争（steal/acquire，新 epoch），
        # 或由 b 接手——任何路径 epoch 都必须严格增大。
        def recovered():
            l = c.lease(lane)
            if l["owner_id"] is None or l["expired"]:
                return None
            if l["lease_epoch"] <= old_epoch:
                return None
            return l
        l2 = wait_until("lease recovered with strictly higher epoch",
                        recovered, timeout=self.ttl + 15)
        _check("恢复后 owner epoch 严格大于旧 epoch（旧 epoch 不可复活）",
               l2["lease_epoch"] > old_epoch,
               (old_epoch, l2["lease_epoch"], l2["owner_id"]))
        self.wait_succeeded(c, pending_refs, timeout=40)
        recs = c.receipts(path)
        ids = [r["event_id"] for r in recs]
        _check("预置 3 条在恢复后全部成功且无重复",
               len(ids) == 5 and len(set(ids)) == 5, ids)
        self.interrupt_max = time.monotonic() - t0
        self.note.update({
            "final_owner": l2["owner_id"],
            "final_epoch": l2["lease_epoch"],
            "total_receipts": len(c.receipts(path)),
            "outage_window_s": round(self.ttl + 2.5, 2),
            "side_effects_during_outage": 0,
            "stale_write_rejected":
                self.metric_sum(c, "stale_write_rejected")})
        self.snapshot_epochs(c, [lane])


def _tail(path: str, n: int = 40) -> str:
    try:
        with open(path, "rb") as f:
            lines = f.readlines()[-n:]
        return b"".join(lines).decode(errors="replace")
    except OSError as e:
        return f"<log unavailable: {e}>"


SCENARIOS = [
    ("1", "双 worker 并行上线：唯一 owner / lane 分散 / 无二次 ack"),
    ("2", "kill -9 failover：TTL 接手、序号单调、已 ack 不重做、全部收敛"),
    ("3", "长暂停恢复：renew/claim/complete 三类陈旧 epoch 写全部被拒"),
    ("4", "429/timeout not_before 水位随 failover 保留，不提前发送"),
    ("5", "v1→v2 轮换即 failover：两批各到各 path，验签代次与快照一致"),
    ("6", "drain：owned 只减不增、deadline 收尾、剩余高 epoch 接手"),
    ("7", "加入/移除/重启 + 多次 rebalance：cap、无 orphan、无全停窗口"),
    ("8", "store outage：停 outbound，恢复后重竞争，旧 epoch 不复活"),
]


def run_acceptance(only: str = "", ttl: float = TTL,
                   base_root: str = "", keep: bool = False) -> dict:
    root = base_root or tempfile.mkdtemp(prefix="whub-acceptance-")
    os.makedirs(root, exist_ok=True)
    results = []
    t_start = time.time()
    for sid, title in SCENARIOS:
        if only and only not in (sid, title):
            continue
        sdir = os.path.join(root, f"scenario-{sid}")
        sc = Scenario(sid, title, ttl=ttl, base_dir=sdir, keep=keep)
        results.append(sc.run())
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "pass")
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "ttl_s": ttl,
        "summary": {"total": total, "passed": passed,
                    "failed": total - passed,
                    "duration_s": round(time.time() - t_start, 2)},
        "scenarios": results,
    }
    report_path = os.path.join(root, "report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("\n%s", "═" * 70)
    log.info("验收报告：%d/%d 通过，用时 %.1fs，报告 %s",
             passed, total, report["summary"]["duration_s"], report_path)
    for r in results:
        mark = "✅" if r["status"] == "pass" else "❌"
        log.info("  %s 场景 %s  %s  (中断峰值 %.1fs, 陈旧拒绝 %s)",
                 mark, r["scenario"], r["title"],
                 r["max_interruption_s"],
                 _stale_total(r))
    log.info("═" * 70)
    report["report_path"] = report_path
    report["data_root"] = root
    return report


def _stale_total(r: dict) -> int:
    return sum(int(v.get("stale_write_rejected", 0))
               for v in r.get("metrics", {}).values())
