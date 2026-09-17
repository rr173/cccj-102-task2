"""HA 自动验收：真实多进程 + 故障注入 + 确定性等待 + 机器可读报告。

每个场景都是**全新集群、全新数据库**：
  store 进程 + worker-a 进程 + worker-b 进程 + 故障注入 sink 进程
退出时无条件清理全部子进程（SIGKILL 兜底，含进程组）。

注入手段全部是真实的：
  kill -9（SIGKILL）、SIGSTOP/SIGCONT（stop-the-world）、store 写闸门
  （POST /admin/store-outage）、旧 fence 回写（/test/stale-attempts）、
  receiver 429/timeout（/admin/rules）。

等待全部是**条件轮询**（轮询数据库行与 receiver 观测），不使用固定长 sleep
猜测结果；只用 store 的单调时钟计算 deadline。
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

log = logging.getLogger("whub.acceptance")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------- HTTP

def http(method: str, url: str, key: str | None = None,
         body: dict | None = None, timeout: float = 10.0,
         retries: int = 0):
    data = json.dumps(body).encode() if body is not None else None
    last = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if key:
            req.add_header("Authorization", f"Bearer {key}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return r.getcode(), json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {"raw": raw.decode(errors="replace")}
            return e.code, payload
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            if attempt < retries:
                time.sleep(0.1)
    raise last


def wait_for(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, _ = http("GET", url, timeout=2)
            if code == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"service not ready: {url}")


def wait_until(desc: str, predicate, timeout: float = 30.0,
               interval: float = 0.15):
    """确定性等待：轮询 predicate 直到真值或超时，返回最终值。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s: {desc}; last={last}")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------- 集群

class Cluster:
    def __init__(self, name: str, ttl: float, tmpdir: str,
                 acquire_budget: int = 3, rebalance_budget: int = 2,
                 extra_workers: int = 0):
        self.name = name
        self.ttl = ttl
        self.dir = os.path.join(tmpdir, name)
        os.makedirs(os.path.join(self.dir, "log"), exist_ok=True)
        self.store_port = free_port()
        self.sink_port = free_port()
        self.worker_ports = {"a": free_port(), "b": free_port()}
        for i in range(extra_workers):
            self.worker_ports[chr(ord("c") + i)] = free_port()
        self.procs: dict[str, subprocess.Popen] = {}
        self.acquire_budget = acquire_budget
        self.rebalance_budget = rebalance_budget

    @property
    def store(self) -> str:
        return f"http://127.0.0.1:{self.store_port}"

    @property
    def sink(self) -> str:
        return f"http://127.0.0.1:{self.sink_port}"

    def worker_url(self, w: str) -> str:
        return f"http://127.0.0.1:{self.worker_ports[w]}"

    def _spawn(self, key: str, args: list[str], env: dict) -> None:
        logfile = open(os.path.join(self.dir, "log", f"{key}.log"), "ab")
        p = subprocess.Popen(
            [sys.executable, "-m", "whub", *args],
            cwd=ROOT, env={**os.environ, **env}, stdout=logfile,
            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True)
        self.procs[key] = p

    def start(self, seed: bool = False) -> None:
        common = {
            "WHUB_LEASE_TTL": str(self.ttl),
            "WHUB_RENEW_INTERVAL": str(max(self.ttl / 3, 0.3)),
            "WHUB_HEARTBEAT_INTERVAL": str(max(self.ttl / 4, 0.3)),
            "WHUB_SWEEP_INTERVAL": "0.15",
            "WHUB_ACQUIRE_BUDGET": str(self.acquire_budget),
            "WHUB_REBALANCE_BUDGET": str(self.rebalance_budget),
            "WHUB_RENEW_JITTER": "0.05",
            "WHUB_HTTP_TIMEOUT": "3",
            "WHUB_BACKOFF_BASE": "0.2",
            "WHUB_BACKOFF_CAP": "8",
            "WHUB_SEED": "1" if seed else "0",
            "WHUB_DRAIN_DEADLINE": str(self.ttl * 3),
            "WHUB_MAX_WORKERS": "64",
        }
        self._spawn("sink", ["sink", "--port", str(self.sink_port)], common)
        self._spawn("store", [
            "store", "--host", "127.0.0.1", "--port", str(self.store_port)],
            {**common, "WHUB_DB": os.path.join(self.dir, "store.db"),
             "WHUB_SEED": "0"})
        wait_for(f"{self.store}/healthz")
        wait_for(f"{self.sink}/healthz")
        for w, port in self.worker_ports.items():
            self._spawn(f"worker-{w}", [
                "worker", f"--worker-id=worker-{w}",
                "--host", "127.0.0.1", "--port", str(port)],
                {**common, "WHUB_STORE_URL": self.store,
                 "WHUB_PORT": str(port), "WHUB_SEED": "0"})
        for w in self.worker_ports:
            wait_for(f"{self.worker_url(w)}/healthz")
        log.info("[%s] cluster up: store:%s sink:%s workers=%s",
                 self.name, self.store_port, self.sink_port,
                 {k: v for k, v in self.worker_ports.items()})

    def kill(self, w: str, sig: int = signal.SIGKILL) -> None:
        p = self.procs.get(f"worker-{w}")
        if p and p.poll() is None:
            os.killpg(os.getpgid(p.pid), sig)
            log.warning("[%s] sent signal %s -> worker-%s pid=%d",
                        self.name, sig, w, p.pid)

    def pause(self, w: str) -> None:
        """真实 OS 级 stop-the-world：SIGSTOP 冻结整个 worker 进程组。

        与应用层 sleep 不同，进程的所有线程（含 HTTP server 与续租循环）
        都不会再跑，直到 SIGCONT；冻结期间 lease 因无法续租而到期。"""
        p = self.procs.get(f"worker-{w}")
        if p and p.poll() is None:
            os.killpg(os.getpgid(p.pid), signal.SIGSTOP)
            log.warning("[%s] SIGSTOP worker-%s pid=%d (stop-the-world)",
                        self.name, w, p.pid)

    def resume(self, w: str) -> None:
        p = self.procs.get(f"worker-{w}")
        if p and p.poll() is None:
            os.killpg(os.getpgid(p.pid), signal.SIGCONT)
            log.warning("[%s] SIGCONT worker-%s pid=%d (resumed)",
                        self.name, w, p.pid)

    def restart_worker(self, w: str) -> None:
        old = self.procs.get(f"worker-{w}")
        if old and old.poll() is None:
            os.killpg(os.getpgid(old.pid), signal.SIGKILL)
            old.wait(timeout=5)
        port = self.worker_ports[w]
        env = {
            "WHUB_LEASE_TTL": str(self.ttl),
            "WHUB_RENEW_INTERVAL": str(max(self.ttl / 3, 0.3)),
            "WHUB_HEARTBEAT_INTERVAL": str(max(self.ttl / 4, 0.3)),
            "WHUB_SWEEP_INTERVAL": "0.15",
            "WHUB_ACQUIRE_BUDGET": str(self.acquire_budget),
            "WHUB_REBALANCE_BUDGET": str(self.rebalance_budget),
            "WHUB_RENEW_JITTER": "0.05",
            "WHUB_HTTP_TIMEOUT": "3",
            "WHUB_BACKOFF_BASE": "0.2",
            "WHUB_BACKOFF_CAP": "8",
            "WHUB_STORE_URL": self.store,
            "WHUB_PORT": str(port), "WHUB_SEED": "0",
            "WHUB_DRAIN_DEADLINE": str(self.ttl * 3),
        }
        self._spawn(f"worker-{w}", [
            "worker", f"--worker-id=worker-{w}",
            "--host", "127.0.0.1", "--port", str(port)], env)
        wait_for(f"{self.worker_url(w)}/healthz")

    def stop(self) -> None:
        for key, p in list(self.procs.items()):
            if p.poll() is None:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.time() + 5
        for key, p in self.procs.items():
            left = max(deadline - time.time(), 0.1)
            try:
                p.wait(timeout=left)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for key, p in self.procs.items():
            try:
                p.wait(timeout=3)
            except Exception:
                pass

    # ---- 便捷操作 ----------------------------------------------------

    def sink_rule(self, path: str, mode: str = "ok", **kw) -> None:
        body = {"path": path, "mode": mode, **kw}
        code, data = http("POST", f"{self.sink}/admin/rules", body=body)
        assert code == 200, data

    def sink_stats(self) -> dict:
        return http("GET", f"{self.sink}/admin/stats")[1]

    def sink_reset(self) -> None:
        http("POST", f"{self.sink}/admin/reset", body={})

    def sink_key(self, kid: str, secret: str) -> None:
        http("POST", f"{self.sink}/admin/keys",
             body={"kid": kid, "secret": secret})

    def store_now(self) -> float:
        # 期限只信 store 的单调时钟：直接查 store 进程的 /rpc
        return http("POST", f"{self.store}/rpc",
                    body={"op": "now", "args": {}})[1]["result"]

    def leases(self) -> list[dict]:
        return http("GET", f"{self.store}/admin/leases")[1]

    def workers(self) -> list[dict]:
        return http("GET", f"{self.store}/admin/workers")[1]

    def ownership_log(self) -> list[dict]:
        return http("GET", f"{self.store}/admin/ownership-log")[1]

    def counters(self) -> list[dict]:
        return http("GET", f"{self.store}/admin/counters")[1]

    def counter(self, worker: str, name: str) -> int:
        for c in self.counters():
            if c["worker_id"] == worker and c["name"] == name:
                return c["value"]
        return 0

    def orphans(self) -> list:
        return http("GET", f"{self.store}/admin/orphans")[1]

    def store_outage(self, reject: bool) -> None:
        code, data = http("POST", f"{self.store}/admin/store-outage",
                          body={"reject_writes": reject})
        assert code == 200, data

    def rebalance(self, budget: int | None = None) -> dict:
        return http("POST", f"{self.store}/admin/rebalance",
                    body={"budget": budget or self.rebalance_budget})[1]

    def drain(self, w: str, deadline: float | None = None,
              draining: bool = True) -> dict:
        body = {"draining": draining}
        if deadline is not None:
            body["deadline"] = deadline
        return http("POST", f"{self.store}/admin/workers/worker-{w}/drain",
                    body=body)[1]


# ---------------------------------------------------------------- 业务辅助

class Tenant:
    def __init__(self, c: Cluster, name: str):
        self.cluster = c
        code, data = http("POST", f"{c.store}/admin/tenants",
                          body={"name": name})
        assert code == 201, data
        self.tid = data["tenant_id"]
        self.api_key = data["api_key"]

    def endpoint(self, path: str, parallelism: int = 4,
                 secret: str | None = None, kid: str | None = None,
                 v2: bool = False) -> dict:
        c = self.cluster
        secret = secret or ("whsec_" + secrets.token_hex(12))
        # 默认 kid 必须全局唯一：sink 按 kid 存密钥，同一 sink 上端点复用
        # "key-1" 会互相覆盖造成签名校验失败。
        kid = kid or ("kid-" + secrets.token_hex(8))
        body = {"name": path, "url": f"{c.sink}{path}",
                "parallelism": parallelism, "secret": secret, "kid": kid}
        code, data = http("POST", f"{c.store}/v1/endpoints",
                          key=self.api_key, body=body)
        assert code == 201, data
        c.sink_key(kid, secret)
        data["kid"] = kid
        if v2:
            secret2 = "whsec2_" + secrets.token_hex(12)
            kid2 = "kid2-" + secrets.token_hex(8)
            code, rot = http("POST", f"{c.store}/v1/endpoints/"
                             f"{data['endpoint_id']}/rotate",
                             key=self.api_key,
                             body={"url": f"{c.sink}{path}-v2",
                                   "secret": secret2, "kid": kid2})
            assert code == 200, rot
            c.sink_key(kid2, secret2)
        data["secret"] = secret
        return data

    def rotate(self, eid: str, path: str, kid: str | None = None,
               secret: str | None = None) -> tuple[str, str]:
        c = self.cluster
        secret = secret or ("whsec_" + secrets.token_hex(12))
        kid = kid or ("kid-" + secrets.token_hex(8))
        code, data = http(
            "POST", f"{c.store}/v1/endpoints/{eid}/rotate",
            key=self.api_key,
            body={"url": f"{c.sink}{path}", "kid": kid, "secret": secret})
        assert code == 200, data
        c.sink_key(kid, secret)
        return kid, secret

    def publish(self, eid: str, obj: str, payload: dict | None = None
                ) -> dict:
        c = self.cluster
        code, data = http("POST", f"{c.store}/v1/endpoints/{eid}/events",
                          key=self.api_key,
                          body={"object_key": obj,
                                "payload": payload or {"x": 1}})
        assert code in (200, 202), data
        return data

    def job(self, job_id: str) -> dict:
        code, data = http("GET",
                          f"{self.cluster.store}/v1/deliveries/{job_id}",
                          key=self.api_key)
        assert code == 200, data
        return data

    def wait_succeeded(self, job_ids: list[str], timeout: float = 30):
        def check():
            rows = [self.job(j) for j in job_ids]
            if all(r["status"] == "succeeded" for r in rows):
                return rows
            return None
        return wait_until(f"jobs succeeded: {len(job_ids)}", check, timeout)


def owner_map(c: Cluster) -> dict[str, str]:
    return {l["lane_id"]: l["owner_id"] for l in c.leases() if l["owner_id"]}


def epoch_map(c: Cluster) -> dict[str, int]:
    return {l["lane_id"]: l["lease_epoch"] for l in c.leases() if l["owner_id"]}


# ================================================================ 验收主体

class CheckFail(Exception):
    pass


class Scenario:
    def __init__(self, num: int, title: str):
        self.num = num
        self.title = title
        self.checks: list[dict] = []
        self.epoch_changes: list[dict] = []
        self.stale_rejected = 0
        self.final_owners: dict = {}
        self.receipts = 0
        self.max_outage = 0.0
        self.detail: dict = {}
        self.error: str | None = None

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        self.checks.append({"name": name, "ok": bool(cond), "detail": detail})
        if not cond:
            raise CheckFail(f"{name} {detail}")

    @property
    def passed(self) -> bool:
        return self.error is None and all(c["ok"] for c in self.checks)


class Acceptance:
    def __init__(self, ttl: float = 4.0, report_path: str = "acceptance-report.json",
                 keep_logs: bool = False, scenarios: str = ""):
        self.ttl = ttl
        self.report_path = report_path
        self.keep_logs = keep_logs
        wanted = [int(x) for x in scenarios.split(",") if x.strip()]
        self.wanted = wanted or list(range(1, 9))
        self.tmpdir = tempfile.mkdtemp(prefix="whub-accept-")
        self.results: list[Scenario] = []

    def run(self) -> int:
        log.info("HA acceptance: ttl=%ss tmp=%s scenarios=%s",
                 self.ttl, self.tmpdir, self.wanted)
        for num in self.wanted:
            sc = Scenario(num, SCENARIO_TITLES[num])
            c = Cluster(f"s{num}", self.ttl, self.tmpdir,
                        acquire_budget=3, rebalance_budget=2)
            t0 = time.time()
            try:
                c.start()
                handler = getattr(self, f"scenario_{num}")
                handler(c, sc)
            except CheckFail as e:
                sc.error = f"assertion: {e}"
            except Exception as e:
                log.exception("scenario %d crashed", num)
                sc.error = f"crash: {type(e).__name__}: {e}"
            finally:
                sc.detail["elapsed"] = round(time.time() - t0, 2)
                try:
                    self._snapshot(c, sc)
                except Exception:
                    pass
                c.stop()
                self.results.append(sc)
                log.info("scenario %d %s (%d checks, stale=%d, outage=%.2fs)",
                         num, "PASS" if sc.passed else "FAIL",
                         len(sc.checks), sc.stale_rejected, sc.max_outage)
        self._write_report()
        ok = all(s.passed for s in self.results)
        self._print_summary()
        return 0 if ok else 1

    # ---- 通用快照 ----------------------------------------------------

    def _snapshot(self, c: Cluster, sc: Scenario) -> None:
        try:
            sc.final_owners = {
                l["object_key"]: l["owner_id"]
                for l in c.leases() if l["owner_id"]}
            sc.stale_rejected = sum(
                x["value"] for x in c.counters()
                if x["name"] == "stale_write_rejected")
            stats = c.sink_stats()
            sc.receipts = sum(len(v) for v in stats.get("receipts", {}).values())
            sc.epoch_changes = [
                {"lane": e["object_key"], "old": e["old_owner"],
                 "new": e["new_owner"], "old_epoch": e["old_epoch"],
                 "new_epoch": e["new_epoch"], "reason": e["reason"]}
                for e in c.ownership_log()]
        except Exception:
            pass

    # ---- 场景 1：并行上线，单 owner、分散处理、无二次 ack -------------

    def scenario_1(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s1")
        c.sink_rule("/s1", "ok")
        ep = t.endpoint("/s1", parallelism=6)["endpoint_id"]
        refs = []
        for i in range(12):
            refs.append(t.publish(ep, f"obj-{i}")["delivery_id"])
        t.wait_succeeded(refs, timeout=25)

        # 每条 lane 最终恰有一个 owner（owner 非空且唯一）
        leases = [l for l in c.leases()]
        sc.check("12 条 lane 全部有 owner",
                 all(l["owner_id"] for l in leases) and len(leases) == 12,
                 detail=str([(l["object_key"], l["owner_id"]) for l in leases]))
        owners = {l["owner_id"] for l in leases}
        sc.check("不同 lane 分散到 a/b 两个进程",
                {"worker-a", "worker-b"}.issubset(owners),
                detail=str(owners))
        # receiver 观测：无二次 ack（receipt event_id 唯一）
        stats = c.sink_stats()
        recv = stats["receipts"].get("/s1", [])
        eids = [r["event_id"] for r in recv]
        sc.check("receiver 收到 12 条 receipt", len(recv) == 12,
                 detail=f"n={len(recv)}")
        sc.check("receipt 集合不存在二次 ack（event_id 唯一）",
                 len(eids) == len(set(eids)) == 12 and stats["duplicates"] == 0,
                 detail=f"dup={stats['duplicates']}")
        # DB 侧：每个 job 仅成功一次
        jobs = http("GET", f"{c.store}/v1/endpoints/{ep}/deliveries",
                    key=t.api_key)[1]
        sc.check("12 个 job 全部 succeeded 且各只一次",
                 len(jobs) == 12 and all(j["status"] == "succeeded"
                                         for j in jobs), detail="")
        # 序号每对象单调
        sc.check("seq 均为 1（每 lane 单 job），且无洞",
                 sorted(j["seq"] for j in jobs) == list(range(1, 13))
                 or all(j["seq"] == 1 for j in jobs), detail="")

    # ---- 场景 2：kill -9 中途接手，序号单调、不重做、收敛 -------------

    def scenario_2(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s2")
        # 慢响应（但短于 http timeout）保证 kill 时正有在途 job
        c.sink_rule("/s2", "delay", delay=0.5)
        ep = t.endpoint("/s2", parallelism=1)["endpoint_id"]

        # 等 lane 被某个 worker 拿走
        first = t.publish(ep, "stream", {"i": 0})
        lane_id = first["lane_id"]

        def lane_owner():
            for l in c.leases():
                if l["lane_id"] == lane_id:
                    return l["owner_id"]
            return None
        owner = wait_until("lane owned", lane_owner, timeout=8)
        victim = "a" if owner == "worker-a" else "b"
        survivor = "b" if victim == "a" else "a"
        sc.detail["victim"] = victim
        t.wait_succeeded([first["delivery_id"]], timeout=12)
        start_epoch = next(l["lease_epoch"] for l in c.leases()
                           if l["lane_id"] == lane_id)

        # 连续投递并在中途 kill -9
        refs = [first["delivery_id"]]
        for i in range(1, 9):
            refs.append(t.publish(ep, "stream", {"i": i})["delivery_id"])
        time.sleep(0.7)  # 让 stream 正在发送（确定性依据是后面的收敛断言）
        c.kill(victim, signal.SIGKILL)
        killed_at = c.store_now()
        sc.detail["killed_at"] = killed_at

        rows = t.wait_succeeded(refs, timeout=40)
        recovered_at = c.store_now()
        sc.max_outage = round(recovered_at - killed_at, 2)

        lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        sc.check("剩余 lane 由存活 worker 接手",
                 lane["owner_id"] == f"worker-{survivor}",
                 detail=lane["owner_id"])
        sc.check("failover 后 epoch 递增",
                 lane["lease_epoch"] > start_epoch,
                 detail=f"{start_epoch}->{lane['lease_epoch']}")
        seqs = [r["seq"] for r in rows]
        sc.check("object_key 序号严格单调 1..9", seqs == list(range(1, 10)),
                 detail=str(seqs))
        stats = c.sink_stats()
        recv = stats["receipts"].get("/s2", [])
        eids = [r["event_id"] for r in recv]
        sc.check("9 个事件全部成功且每事件仅一次有效 ack（9 个不同 event_id）",
                 len(set(eids)) == 9,
                 detail=f"receipts={len(recv)} unique={len(set(eids))} "
                        f"dup={stats['duplicates']}")
        # 已落 ack 的 job 不重做：DB 中 succeeded 只结算一次（无回退）
        statuses = {r["delivery_id"]: r["status"] for r in rows}
        sc.check("所有 job 终态唯一为 succeeded（无 succeeded 被回退重做）",
                 all(s == "succeeded" for s in statuses.values())
                 and len(statuses) == 9, detail=str(set(statuses.values())))
        # kill 时刻在途但未提交的 job 允许重发，由 receiver 幂等吸收（不产生二次 ack）
        sc.detail["receiver_dedup_hits"] = stats["duplicates"]

    # ---- 场景 3：超 TTL stop-the-world，三类陈旧写全拒 ----------------

    def scenario_3(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s3")
        c.sink_rule("/s3", "ok")
        ep = t.endpoint("/s3", parallelism=2)["endpoint_id"]
        # 两条 lane：一条用于 renew/claim，一条提供 job_id 做 complete
        j1 = t.publish(ep, "freeze-lane", {"i": 1})
        j2 = t.publish(ep, "freeze-job", {"i": 1})
        lane_id = j1["lane_id"]
        job_id = j2["delivery_id"]
        t.wait_succeeded([j1["delivery_id"], j2["delivery_id"]], timeout=15)

        lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        victim_url = c.worker_url("a" if lane["owner_id"] == "worker-a" else "b")
        victim = "a" if lane["owner_id"] == "worker-a" else "b"
        # 等待启动期自动 rebalance 窗口（1.5*TTL）关闭，使冻结期间唯一可能的
        # owner 变更就是 expiry_steal，epoch 严格 +1，断言确定性。
        time.sleep(self.ttl * 1.6)
        lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        victim_url = c.worker_url("a" if lane["owner_id"] == "worker-a" else "b")
        victim = "a" if lane["owner_id"] == "worker-a" else "b"
        old_epoch = lane["lease_epoch"]
        # 控制面 lease 视图只给截断 fence；用 /rpc 取完整 fence 模拟旧进程回写
        full = http("POST", f"{c.store}/rpc",
                    body={"op": "lane", "args": {"lane_id": lane_id}})[1]
        old_fence = full["result"]["fence_id"]
        other = "b" if victim == "a" else "a"

        # 真实 OS 级 stop-the-world：SIGSTOP 冻结整个 worker 进程组，
        # 超过 TTL（含一次续租/扫描周期），其 lease 因无法续租而到期。
        c.pause(victim)
        froze_at = c.store_now()
        # 等待对端在 TTL 后接手
        wait_until("lane stolen after TTL",
                   lambda: next((l["owner_id"] for l in c.leases()
                                 if l["lane_id"] == lane_id), None)
                   == f"worker-{other}", timeout=self.ttl * 5)
        new_lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        sc.check("TTL 过期后 lane 被对端 steal，epoch+1",
                 new_lane["lease_epoch"] == old_epoch + 1
                 and new_lane["owner_id"] == f"worker-{other}",
                 detail=f"{old_epoch}->{new_lane['lease_epoch']}")

        # 恢复旧 worker（SIGCONT）：它的进程内仍是旧 fence，随后用旧 fence
        # 分别尝试 renew / claim / complete，三类陈旧写都必须被数据库拒绝。
        c.resume(victim)
        wait_until("victim HTTP server responsive after SIGCONT",
                   lambda: http("GET", f"{victim_url}/worker/view")[0] == 200,
                   timeout=self.ttl * 3)
        code, attempts = http("POST", f"{victim_url}/test/stale-attempts",
                              body={"lane_id": lane_id, "epoch": old_epoch,
                                    "fence": old_fence, "job_id": job_id})
        sc.check("stale-attempts 接口可执行", code == 200, detail=str(attempts))
        sc.check("陈旧 renew 被数据库拒绝(stale_epoch)",
                 attempts.get("renew") == "stale_epoch", detail=str(attempts))
        sc.check("陈旧 claim 被数据库拒绝(stale_epoch)",
                 attempts.get("claim") == "stale_epoch", detail=str(attempts))
        sc.check("陈旧 complete 被数据库拒绝(stale_epoch)",
                 attempts.get("complete") == "stale_epoch", detail=str(attempts))

        # 新 owner 已落盘状态不变
        after = next(l for l in c.leases() if l["lane_id"] == lane_id)
        job = t.job(job_id)
        sc.check("新 owner 的 epoch 保持不变（旧回写未覆盖所有权）",
                 after["lease_epoch"] == new_lane["lease_epoch"]
                 and after["owner_id"] == f"worker-{other}",
                 detail=f"epoch {after['lease_epoch']} owner {after['owner_id']}")
        sc.check("旧回写未覆盖已 succeeded 的 job",
                 job["status"] == "succeeded", detail=job["status"])
        sc.check("stale_write_rejected 至少 3 次",
                 c.counter(f"worker-{victim}", "stale_write_rejected") >= 3,
                 detail=str(c.counters()))
        sc.max_outage = round(c.store_now() - froze_at, 2)

    # ---- 场景 4：429/timeout 的 future not_before，继任者不提前 --------

    def scenario_4(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s4")
        hold = self.ttl * 5
        c.sink_rule("/s4-throttle", "ratelimit", retry_after=hold)
        c.sink_rule("/s4-fast", "ok")
        ep_bad = t.endpoint("/s4-throttle", parallelism=2)["endpoint_id"]
        ep_fast = t.endpoint("/s4-fast", parallelism=4)["endpoint_id"]

        j1 = t.publish(ep_bad, "rate-limited", {"i": 1})
        lane_id = j1["lane_id"]
        # 等待 429 退避水位写入（job 先被 claim、收到 429、回写 not_before）
        def backoff_set():
            l = next((x for x in c.leases() if x["lane_id"] == lane_id), None)
            if l is not None and l["not_before"] > c.store_now():
                return l
            job = t.job(j1["delivery_id"])
            if job["fail_count"] >= 1 and job["not_before"] > c.store_now():
                return l or {"not_before": job["not_before"], "lease_epoch": 0,
                             "owner_id": job["leased_by"]}
            return None
        lane = wait_until("lane not_before in future after 429",
                          backoff_set, timeout=25)
        sc.check("已记录 429 对应的 future not_before",
                 lane["not_before"] > c.store_now(),
                 detail=f"nb={lane['not_before']} now={c.store_now()}")
        victim = "a" if lane["owner_id"] == "worker-a" else "b"
        other = "b" if victim == "a" else "a"
        old_epoch = lane["lease_epoch"]
        c.kill(victim, signal.SIGKILL)

        wait_until("throttled lane stolen",
                   lambda: next((l["owner_id"] for l in c.leases()
                                 if l["lane_id"] == lane_id), None)
                   == f"worker-{other}", timeout=self.ttl * 4)
        new_lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        sc.check("继任者拿到更高 epoch 且保留 not_before 水位",
                 new_lane["lease_epoch"] > old_epoch
                 and new_lane["not_before"] >= lane["not_before"] - 0.01,
                 detail=f"nb={new_lane['not_before']}")

        # 在 not_before 到期前，该 lane 不应产生任何 receipt（含失败尝试外的成功）
        time.sleep(self.ttl + 0.5)  # 跨越多个 TTL+steal 周期，但仍小于 hold
        stats = c.sink_stats()
        sent_throttled = len(stats["receipts"].get("/s4-throttle", []))
        sc.check("未到 not_before，继任者禁止提前发出（0 成功 receipt）",
                 sent_throttled == 0, detail=f"receipts={sent_throttled}")

        # 其余 lane 的吞吐继续增长：快端点持续投递成功
        refs = [t.publish(ep_fast, f"flow-{i}", {"i": i})["delivery_id"]
                for i in range(10)]
        t.wait_succeeded(refs, timeout=20)
        sc.check("其余 lane 吞吐继续增长（10/10 成功）", True)
        # 限流计数确实发生（证明旧 owner 试过且被 429 挡下）
        sc.check("receiver 记录到 ratelimited",
                 stats["counters"].get("/s4-throttle", {}).get(
                     "ratelimited", 0) >= 1
                 or c.sink_stats()["counters"].get(
                     "/s4-throttle", {}).get("ratelimited", 0) >= 1,
                 detail=str(c.sink_stats()["counters"]))

    # ---- 场景 5：v1 积压 + 切 v2 + failover，路径/代次与快照一致 ------

    def scenario_5(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s5")
        c.sink_rule("/s5-v1", "ok")
        c.sink_rule("/s5-v2", "ok")
        epinfo = t.endpoint("/s5-v1", parallelism=2, kid="key-1",
                            secret="sec-v1-snapshot")
        eid = epinfo["endpoint_id"]
        # v1 积压：先让 v1 路径变慢，形成积压
        c.sink_rule("/s5-v1", "delay", delay=0.8)
        old_jobs = [t.publish(eid, "batch-old", {"i": i})["delivery_id"]
                    for i in range(4)]
        time.sleep(1.0)  # 积压形成（不依赖固定 sleep 判定结果，只用于制造积压）
        lane_id = t.job(old_jobs[0])["lane_id"]
        lane = next(l for l in c.leases() if l["lane_id"] == lane_id)
        victim = "a" if lane["owner_id"] == "worker-a" else "b"
        other = "b" if victim == "a" else "a"

        # 配置切 v2（新 URL + 新 kid + 新 secret）并立刻 failover
        t.rotate(eid, "/s5-v2", kid="key-2", secret="sec-v2-snapshot")
        new_jobs = [t.publish(eid, "batch-new", {"i": i})["delivery_id"]
                    for i in range(3)]
        c.kill(victim, signal.SIGKILL)

        t.wait_succeeded(old_jobs + new_jobs, timeout=40)
        stats = c.sink_stats()
        v1 = stats["receipts"].get("/s5-v1", [])
        v2 = stats["receipts"].get("/s5-v2", [])
        old_ids = {t.job(j)["event_id"] for j in old_jobs}
        new_ids = {t.job(j)["event_id"] for j in new_jobs}
        got_v1 = {r["event_id"] for r in v1}
        got_v2 = {r["event_id"] for r in v2}
        sc.check("v1 积压批次全部到达旧 path /s5-v1",
                 old_ids.issubset(got_v1),
                 detail=f"{len(got_v1 & old_ids)}/4")
        sc.check("v2 新增批次全部到达新 path /s5-v2",
                 new_ids.issubset(got_v2),
                 detail=f"{len(got_v2 & new_ids)}/3")
        sc.check("v1 receipt 验签代次=1（kid key-1）",
                 len(v1) == 4 and all(r["kid"] == "key-1"
                                      and r["sig_version"] == 1 for r in v1),
                 detail=str([(r["kid"], r["sig_version"]) for r in v1]))
        sc.check("v2 receipt 验签代次=2（kid key-2）",
                 len(v2) == 3 and all(r["kid"] == "key-2"
                                      and r["sig_version"] == 2 for r in v2),
                 detail=str([(r["kid"], r["sig_version"]) for r in v2]))
        sc.check("无坏签名（快照密钥全部验过）",
                 stats["bad_signatures"] == 0,
                 detail=f"bad={stats['bad_signatures']}")
        new_owner = next(l for l in c.leases() if l["lane_id"] == lane_id)
        sc.check("failover 后由存活 worker 负责",
                 new_owner["owner_id"] == f"worker-{other}",
                 detail=new_owner["owner_id"])

    # ---- 场景 6：drain 只减不增、按时收尾、handoff reason 齐全 ---------

    def scenario_6(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s6")
        c.sink_rule("/s6", "ok")
        ep = t.endpoint("/s6", parallelism=8)["endpoint_id"]
        # 先铺 12 条 lane，自然分散；等待 lane 被两个 worker 分摊
        refs = [t.publish(ep, f"lane-{i}", {"i": i})["delivery_id"]
                for i in range(12)]
        t.wait_succeeded(refs[:8], timeout=20)
        wait_until("worker-a owns at least 2 lanes before drain",
                   lambda: next((w["owned"] for w in c.workers()
                                 if w["worker_id"] == "worker-a"), 0) >= 2,
                   timeout=15)
        before = c.workers()
        wa = next(w for w in before if w["worker_id"] == "worker-a")
        initial_owned = wa["owned"]
        sc.check("drain 前 worker-a 持有部分 lane", initial_owned > 0,
                 detail=str(initial_owned))

        # 持续背景流量，证明服务期间不断成功 receipt
        bg = [t.publish(ep, f"bg-{i}", {"i": i})["delivery_id"]
              for i in range(6)]
        # drain a：deadline 短于 TTL，让到期 lane 交还
        c.drain("a", deadline=self.ttl * 2)
        owned_series = [initial_owned]
        deadline = time.time() + self.ttl * 6
        while time.time() < deadline:
            wa2 = next(w for w in c.workers() if w["worker_id"] == "worker-a")
            owned_series.append(wa2["owned"])
            if wa2["owned"] == 0:
                break
            time.sleep(0.2)
        sc.check("drain 期间 owned 数量只减不增",
                 all(b >= a for b, a in zip(owned_series, owned_series[1:])),
                 detail=str(owned_series))
        final_a = next(w for w in c.workers() if w["worker_id"] == "worker-a")
        sc.check("deadline 后 worker-a 不再持有任何 lane",
                 final_a["owned"] == 0, detail=str(final_a["owned"]))
        sc.check("worker-a 保持 draining 状态", final_a["draining"] is True)

        # b 接手的 lane 有更高 epoch，且 handoff reason 留痕
        log_entries = c.ownership_log()
        reasons = [e["reason"] for e in log_entries]
        sc.check("API/日志留下 handoff reason（drain_handoff/expiry_steal）",
                 "drain_handoff" in reasons or "expiry_steal" in reasons,
                 detail=str(set(reasons)))
        # 全部 job（含背景流）最终成功，服务期间持续有 receipt
        t.wait_succeeded(refs + bg, timeout=30)
        stats = c.sink_stats()
        recv = len(stats["receipts"].get("/s6", []))
        sc.check("drain 全程成功 receipt 不断（18 个事件全成功）",
                 recv == 18 and stats["duplicates"] == 0,
                 detail=f"receipts={recv}")
        sc.check("无 orphan lane", len(c.orphans()) == 0)

    # ---- 场景 7：加入/移除/重启 + 多次 rebalance，无 orphan/全停 -------

    def scenario_7(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s7")
        c.sink_rule("/s7", "ok")
        ep = t.endpoint("/s7", parallelism=10)["endpoint_id"]
        refs = [t.publish(ep, f"k-{i}", {"i": i})["delivery_id"]
                for i in range(16)]
        t.wait_succeeded(refs[:6], timeout=20)

        def loads():
            ws = c.workers()
            return {w["worker_id"]: w["owned"] for w in ws}

        # 每轮搬运量不超过 rebalance_budget（2）
        for _ in range(3):
            moved = c.rebalance(budget=2)["moved"]
            sc.check("单轮 rebalance 搬动 <= budget",
                     len(moved) <= 2, detail=f"moved={len(moved)}")

        # 重启 b（新进程、新 incarnation）：register 立即释放其旧 lane 到公共池
        c.restart_worker("b")
        wait_until("no orphans after b restart",
                   lambda: len(c.orphans()) == 0, timeout=self.ttl * 4)
        wait_until("all lanes owned after b restart",
                   lambda: all(l["owner_id"] for l in c.leases()),
                   timeout=self.ttl * 4)

        # kill a，再观察
        c.kill("a", signal.SIGKILL)
        wait_until("b absorbed all lanes",
                   lambda: all(l["owner_id"] == "worker-b"
                               for l in c.leases()),
                   timeout=self.ttl * 4)
        c.restart_worker("a")
        for _ in range(2):
            moved = c.rebalance(budget=2)["moved"]
            sc.check("恢复后单轮搬动仍 <= budget",
                     len(moved) <= 2, detail=f"moved={len(moved)}")
        # 最终：无 orphan，lane 在两个健康成员间，全部 job 收敛
        t.wait_succeeded(refs, timeout=40)
        sc.check("最终不存在 orphan lane", len(c.orphans()) == 0,
                 detail=str(c.orphans()))
        owners = {l["owner_id"] for l in c.leases()}
        sc.check("lane 由在线成员持有", owners <= {"worker-a", "worker-b"}
                 and len(owners) >= 1, detail=str(owners))
        stats = c.sink_stats()
        sc.check("16 个事件全部成功、无重复 ack",
                 len(stats["receipts"].get("/s7", [])) == 16
                 and stats["duplicates"] == 0,
                 detail=str(len(stats["receipts"].get("/s7", []))))
        # 处理曲线无全停窗口：a 死后仍有 lane 属于 b，且背景新 job 能成功
        bg = [t.publish(ep, f"post-{i}", {"i": i})["delivery_id"]
              for i in range(4)]
        t.wait_succeeded(bg, timeout=20)
        sc.check("failover 期间新增工作持续收敛", True)

    # ---- 场景 8：store outage 停止出站；恢复后旧 epoch 不复活 ----------

    def scenario_8(self, c: Cluster, sc: Scenario) -> None:
        t = Tenant(c, "s8")
        c.sink_rule("/s8", "ok")
        ep = t.endpoint("/s8", parallelism=4)["endpoint_id"]
        # 先建立稳态所有权
        warm = [t.publish(ep, f"warm-{i}", {"i": i})["delivery_id"]
                for i in range(4)]
        t.wait_succeeded(warm, timeout=20)
        before = c.sink_stats()["counters"].get("/s8", {}).get("received", 0)

        # 关闭 store 写入
        c.store_outage(True)
        gate_at = c.store_now()

        def workers_saw_outage():
            total = 0
            for w in ("a", "b"):
                _, view = http("GET", c.worker_url(w) + "/worker/view")
                if isinstance(view, dict):
                    total += view.get("store_unavailable", 0) \
                             + view.get("touch_blocked", 0)
            return total or None
        wait_until("workers observed store write failure",
                   workers_saw_outage, timeout=self.ttl * 3)
        # 再跨一个完整的 claim/touch/renew 周期
        time.sleep(self.ttl + 0.5)
        during = c.sink_stats()["counters"].get("/s8", {}).get("received", 0)
        sc.check("store 拒绝写入期间 worker 不产生新的出站投递",
                 during == before,
                 detail=f"before={before} during={during}")
        blocked = workers_saw_outage() or 0
        sc.check("worker 记录 touch/写被阻断（不靠内存 ownership 发送）",
                 blocked > 0, detail=str(blocked))

        # 恢复写入：worker 重新竞争；旧 epoch 若已过期不可复活
        c.store_outage(False)
        recovered_at = c.store_now()
        sc.max_outage = round(recovered_at - gate_at, 2)
        # 恢复后新工作可以成功
        after = [t.publish(ep, f"after-{i}", {"i": i})["delivery_id"]
                 for i in range(6)]
        t.wait_succeeded(after, timeout=30)
        sc.check("store 恢复后投递恢复，6 个新 job 成功", True)

        # 选一条 lane：用 outage 开始前可能的旧 epoch/fence 尝试写，必须被拒
        lane = c.leases()[0]
        full = http("POST", f"{c.store}/rpc",
                    body={"op": "lane",
                          "args": {"lane_id": lane["lane_id"]}})[1]["result"]
        owner = full["owner_id"]
        wcode = "a" if owner == "worker-a" else "b"
        # 构造一个必定陈旧的 fence（epoch-1 / 假 fence）
        code, resp = http("POST", c.worker_url(wcode) + "/test/stale-attempts",
                          body={"lane_id": lane["lane_id"],
                                "epoch": full["lease_epoch"] - 1,
                                "fence": "fence_deadbeef", "job_id": None})
        sc.check("恢复后旧 epoch 仍不可复活（renew/claim 均拒）",
                 code == 200 and resp.get("renew") == "stale_epoch"
                 and resp.get("claim") == "stale_epoch",
                 detail=str(resp))
        sc.check("最终无 orphan lane", len(c.orphans()) == 0)
        stats = c.sink_stats()
        sc.check("全程无重复 ack",
                 stats["duplicates"] == 0, detail=str(stats["duplicates"]))

    # ---- 报告 --------------------------------------------------------

    def _write_report(self) -> None:
        report = {
            "generated_at": time.time(),
            "ttl_seconds": self.ttl,
            "store_clock_trusted": True,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for s in self.results if s.passed),
                "failed": sum(1 for s in self.results if not s.passed)},
            "scenarios": [{
                "id": s.num, "title": s.title, "passed": s.passed,
                "error": s.error,
                "checks_passed": sum(1 for x in s.checks if x["ok"]),
                "checks_total": len(s.checks),
                "stale_write_rejected": s.stale_rejected,
                "receipts": s.receipts,
                "max_outage_seconds": s.max_outage,
                "final_owners": s.final_owners,
                "epoch_changes": s.epoch_changes,
                "detail": s.detail,
                "checks": s.checks,
            } for s in self.results],
        }
        with open(self.report_path, "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        log.info("machine-readable report: %s", os.path.abspath(
            self.report_path))

    def _print_summary(self) -> None:
        bar = "═" * 72
        log.info("\n%s\nHA 验收报告", bar)
        for s in self.results:
            mark = "✅" if s.passed else "❌"
            log.info("%s 场景 %d %s | checks=%d/%d stale拒=%d 收据=%d "
                     "最大中断=%.2fs", mark, s.num, s.title,
                     sum(1 for x in s.checks if x["ok"]), len(s.checks),
                     s.stale_rejected, s.receipts, s.max_outage)
            if not s.passed:
                log.info("   FAIL: %s", s.error)
                for ch in s.checks:
                    if not ch["ok"]:
                        log.info("     - %s %s", ch["name"], ch["detail"])
        p = sum(1 for s in self.results if s.passed)
        log.info("%s\n合计：%d/%d 通过", bar, p, len(self.results))


SCENARIO_TITLES = {
    1: "并行上线单 owner / lane 分散 / 无二次 ack",
    2: "kill -9 后 TTL 接手 / 序号单调 / 不重做 / 全收敛",
    3: "超 TTL STW 恢复 / renew+claim+complete 陈旧写全拒",
    4: "429 future not_before 不提前 / 其余 lane 吞吐继续",
    5: "v1 积压与 v2 新批次按快照路径与签名代次投递",
    6: "drain 只减不增 / deadline handoff / 服务不断",
    7: "加入移除重启+多次 rebalance / cap 约束 / 无 orphan",
    8: "store outage 停止出站 / 恢复后旧 epoch 不复活",
}
