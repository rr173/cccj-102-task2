"""HA 验收的进程编排：真实独立 OS process，绝不做进程内模拟。

- store 进程：共享 durable SQLite + 控制面 API（故障注入/debug 入口）；
- worker 进程：``python3 -m whub worker``，各自独立 PID、独立连接；
- sink 进程：带故障注入的接收方。

支持 ``SIGKILL``（kill -9）、``SIGSTOP``/``SIGCONT``（stop-the-world）、
store outage marker。子进程全部进入新进程组，退出时整组清理，不留孤儿。
等待一律是“轮询确定性条件 + 超时失败”，不用固定长 sleep 猜结果。
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

log = logging.getLogger("whub.cluster")

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def http(method: str, url: str, key: str | None = None,
         body: dict | None = None, timeout: float = 5.0, retries: int = 0):
    last = (0, {"error": "unreachable"})
    for attempt in range(retries + 1):
        data = json.dumps(body).encode() if body is not None else None
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
            last = (0, {"error": "unreachable", "message": str(e)})
            if attempt < retries:
                time.sleep(0.25)
    return last


def wait_until(desc: str, pred, timeout: float = 30.0,
               interval: float = 0.1) -> object:
    """轮询直到 pred() 返回真值；超时断言失败并给出描述。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = pred()
        except Exception as e:  # 条件求值期间进程可能正被 kill
            last = None
            log.debug("wait condition %r raised: %s", desc, e)
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {desc} (last={last!r})")


@dataclass
class Proc:
    name: str
    popen: subprocess.Popen
    log_path: str
    port_file: str = ""

    @property
    def pid(self) -> int:
        return self.popen.pid

    def port(self) -> int | None:
        if not self.port_file or not os.path.exists(self.port_file):
            return None
        try:
            with open(self.port_file) as f:
                return int(f.read().strip())
        except (ValueError, OSError):
            return None

    def wait_port(self, timeout: float = 10.0) -> int:
        port = wait_until(
            f"{self.name} port file {self.port_file}",
            self.port, timeout=timeout, interval=0.05)
        return int(port)

    def signal(self, sig: int) -> None:
        try:
            os.killpg(os.getpgid(self.popen.pid), sig)
        except ProcessLookupError:
            pass

    def kill9(self) -> None:
        log.warning("SIGKILL to %s (pid %d)", self.name, self.popen.pid)
        self.signal(signal.SIGKILL)

    def freeze(self) -> None:
        """stop-the-world：SIGSTOP 整个进程组（worker 无法续期/感知时间流逝）。"""
        log.warning("SIGSTOP (stop-the-world) to %s (pid %d)", self.name,
                    self.popen.pid)
        self.signal(signal.SIGSTOP)

    def resume(self) -> None:
        log.warning("SIGCONT to %s (pid %d)", self.name, self.popen.pid)
        self.signal(signal.SIGCONT)

    def terminate(self, timeout: float = 5.0) -> int:
        if self.popen.poll() is None:
            # 先 SIGCONT 解停，否则被 SIGSTOP 的进程收不到/处理不了 SIGTERM，
            # 会白等到超时再 SIGKILL。
            self.signal(signal.SIGCONT)
            self.signal(signal.SIGTERM)
            try:
                return self.popen.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.signal(signal.SIGKILL)
        try:
            return self.popen.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.signal(signal.SIGKILL)
            return -9


@dataclass
class Cluster:
    ttl: float = 3.0
    base_dir: str = ""
    sink_port: int = 0
    debug_admin: bool = True
    procs: dict[str, Proc] = field(default_factory=dict)
    dir: str = ""
    db_path: str = ""
    outage_marker: str = ""
    store_url: str = ""
    sink_url: str = ""
    env_extra: dict = field(default_factory=dict)
    _cleaned: bool = False

    def start(self, *, workers=("worker-a", "worker-b"), start_sink=True,
              seed=True) -> None:
        self.dir = self.base_dir or tempfile.mkdtemp(prefix="whub-ha-")
        log_dir = os.path.join(self.dir, "log")
        run_dir = os.path.join(self.dir, "run")
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(run_dir, exist_ok=True)
        self.db_path = os.path.join(self.dir, "shared.db")
        self.outage_marker = os.path.join(run_dir, "STORE_OUTAGE")

        env = os.environ.copy()
        env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
        env["WHUB_DB"] = self.db_path
        env["WHUB_LEASE_TTL"] = str(self.ttl)
        env["WHUB_RENEW_INTERVAL"] = str(max(self.ttl / 5.0, 0.25))
        env["WHUB_LEASE_TICK_INTERVAL"] = "0.2"
        env["WHUB_COORD_INTERVAL"] = "0.3"
        env["WHUB_REBALANCE_MAX_MOVES"] = "1"
        env["WHUB_HTTP_TIMEOUT"] = "4"
        env["WHUB_OUTAGE_MARKER"] = self.outage_marker
        env["WHUB_SEED"] = "1" if seed else "0"
        env["WHUB_POLL_INTERVAL"] = "0.08"
        env.update(self.env_extra)

        def _spawn(name, argv, env_over=None):
            lf = open(os.path.join(log_dir, f"{name}.log"), "ab")
            e = env.copy()
            if env_over:
                e.update(env_over)
            pf = os.path.join(run_dir, f"{name}.port")
            p = subprocess.Popen(
                argv, cwd=ROOT, env=e, stdout=lf, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                preexec_fn=os.setsid, close_fds=True)
            proc = Proc(name=name, popen=p, log_path=lf.name, port_file=pf)
            self.procs[name] = proc
            log.info("spawned %s pid=%d", name, p.pid)
            return proc

        # sink 需要固定端口才能被端点 URL 引用
        if start_sink:
            if not self.sink_port:
                self.sink_port = _free_port()
            _spawn("sink",
                   [sys.executable, "-m", "whub", "sink",
                    "--host", "127.0.0.1", "--port", str(self.sink_port)])

        store_argv = [sys.executable, "-m", "whub", "store",
                      "--port-file", os.path.join(run_dir, "store.port")]
        if self.debug_admin:
            store_argv.append("--debug-admin")
        if not seed:
            store_argv.append("--no-seed")
        store = _spawn("store", store_argv)
        store_port = store.wait_port()
        self.store_url = f"http://127.0.0.1:{store_port}"
        self.sink_url = f"http://127.0.0.1:{self.sink_port}"

        for wid in workers:
            self.spawn_worker(wid, env=env)

        wait_until("store healthy",
                   lambda: http("GET", f"{self.store_url}/healthz")[0] == 200,
                   timeout=15)
        if start_sink:
            wait_until("sink healthy",
                       lambda: http("GET",
                                    f"{self.sink_url}/healthz")[0] == 200,
                       timeout=15)
        time.sleep(0.2)

    def spawn_worker(self, worker_id: str, *, env: dict | None = None,
                     ttl: float | None = None) -> Proc:
        log_dir = os.path.join(self.dir, "log")
        run_dir = os.path.join(self.dir, "run")
        lf = open(os.path.join(log_dir, f"{worker_id}.log"), "ab")
        e = (env or os.environ.copy()).copy()
        e.setdefault("PYTHONPATH", ROOT)
        e["WHUB_DB"] = self.db_path
        e["WHUB_LEASE_TTL"] = str(ttl or self.ttl)
        e["WHUB_RENEW_INTERVAL"] = str(max((ttl or self.ttl) / 3.0, 0.2))
        e["WHUB_COORD_INTERVAL"] = "0.3"
        e["WHUB_REBALANCE_MAX_MOVES"] = "1"
        e["WHUB_HTTP_TIMEOUT"] = "4"
        e["WHUB_OUTAGE_MARKER"] = self.outage_marker
        e["WHUB_POLL_INTERVAL"] = "0.08"
        pf = os.path.join(run_dir, f"{worker_id}.port")
        p = subprocess.Popen(
            [sys.executable, "-m", "whub", "worker",
             "--worker-id", worker_id,
             "--port-file", pf],
            cwd=ROOT, env=e, stdout=lf, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, preexec_fn=os.setsid, close_fds=True)
        proc = Proc(name=worker_id, popen=p, log_path=lf.name, port_file=pf)
        self.procs[worker_id] = proc
        proc.wait_port()
        return proc

    # -- 便捷查询 ------------------------------------------------------

    def workers(self) -> list[dict]:
        code, data = http("GET", f"{self.store_url}/admin/workers")
        assert code == 200, (code, data)
        return data

    def worker(self, wid: str) -> dict:
        return next(w for w in self.workers() if w["worker_id"] == wid)

    def leases(self) -> list[dict]:
        code, data = http("GET", f"{self.store_url}/admin/leases")
        assert code == 200, (code, data)
        return data

    def lease(self, lane_id: str) -> dict:
        return next(l for l in self.leases() if l["lane_id"] == lane_id)

    def audit(self, lane_id: str | None = None) -> list[dict]:
        url = f"{self.store_url}/admin/audit"
        if lane_id:
            url += f"?lane={lane_id}"
        code, data = http("GET", url)
        assert code == 200
        return data

    def metrics(self) -> dict:
        code, data = http("GET", f"{self.store_url}/admin/metrics")
        assert code == 200
        return data

    def drain(self, wid: str, deadline: float | None = None):
        return http("POST",
                    f"{self.store_url}/admin/workers/{wid}/drain",
                    body={"deadline": deadline} if deadline is not None else {})

    def undrain(self, wid: str):
        return http("POST",
                    f"{self.store_url}/admin/workers/{wid}/undrain", body={})

    def rebalance(self, max_moves: int = 1):
        return http("POST", f"{self.store_url}/admin/rebalance",
                    body={"max_moves": max_moves})

    def assign(self, lane_id: str, wid: str,
               reason: str = "acceptance_assign"):
        return http("POST",
                    f"{self.store_url}/admin/lanes/{lane_id}/assign",
                    body={"worker_id": wid, "reason": reason})

    def set_outage(self, on: bool):
        return http("POST", f"{self.store_url}/admin/outage",
                    body={"on": on})

    def debug_stale(self, op: str, lane_id: str,
                    delivery_id: str | None = None):
        return http("POST", f"{self.store_url}/admin/debug/{op}",
                    body={"lane_id": lane_id, "delivery_id": delivery_id})

    def sink_admin(self, path: str, body: dict | None = None):
        code, data = http("POST", f"{self.sink_url}/admin{path}",
                          body=body or {})
        assert code == 200, (path, code, data)
        return data

    def sink_stats(self) -> dict:
        code, data = http("GET", f"{self.sink_url}/admin/stats", retries=4)
        assert code == 200, (code, data)
        return data

    def receipts(self, path: str) -> list[dict]:
        return self.sink_stats()["receipts"].get(path, [])

    # -- 生命周期 ------------------------------------------------------

    def stop_worker(self, name: str) -> None:
        p = self.procs.pop(name, None)
        if p:
            p.terminate()

    def cleanup(self) -> None:
        if self._cleaned:
            return
        self._cleaned = True
        for name in list(self.procs):
            p = self.procs.pop(name, None)
            if p:
                p.terminate(timeout=4)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.cleanup()


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
