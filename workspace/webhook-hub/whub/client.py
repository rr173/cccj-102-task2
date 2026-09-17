"""durable store 客户端：同一接口的两种后端。

* :class:`DirectClient` —— 调用方进程内持有 :class:`~whub.engine.Engine`
  （单 worker 兼容入口：仲裁仍是数据库事务，只是不经网络）。
* :class:`HttpStore` —— 连接独立 OS 进程的 store 服务（HA 形态）。

worker 代码只依赖本接口，因此切换形态不需要改投递逻辑。
所有返回对象统一为可 JSON 化的 dict（HTTP 与直连行为一致）。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Optional

from .engine import Engine, StoreError, StaleEpoch, LeaseNotOwned, StoreUnavailable

# RPC 名 => (引擎方法, 是否读操作)
_RPC: dict[str, str] = {
    "now": "now",
    "create_tenant": "create_tenant",
    "tenant_by_key": "tenant_by_key",
    "tenant": "tenant",
    "create_endpoint": "create_endpoint",
    "endpoint": "endpoint",
    "endpoints_for_tenant": "endpoints_for_tenant",
    "active_endpoint_ids": "active_endpoint_ids",
    "set_parallelism": "set_parallelism",
    "set_disabled": "set_disabled",
    "version": "version",
    "versions": "versions",
    "rotate_key": "rotate_key",
    "retire_version": "retire_version",
    "find_event": "find_event",
    "event": "event",
    "enqueue": "enqueue",
    "register_worker": "register_worker",
    "heartbeat": "heartbeat",
    "deregister_worker": "deregister_worker",
    "set_drain": "set_drain",
    "renew": "renew",
    "touch": "touch",
    "release": "release",
    "sweep": "sweep",
    "rebalance": "rebalance",
    "claim": "claim",
    "complete_success": "complete_success",
    "complete_retry": "complete_retry",
    "complete_dead": "complete_dead",
    "replay": "replay",
    "skip": "skip",
    "job": "job",
    "job_for_event": "job_for_event",
    "delivery_secret": "delivery_secret",
    "list_jobs": "list_jobs",
    "lane": "lane",
    "lanes_for_endpoint": "lanes_for_endpoint",
    "list_leases": "list_leases",
    "list_workers": "list_workers",
    "worker": "worker",
    "owned_lanes": "owned_lanes",
    "active_job_count": "active_job_count",
    "orphan_lanes": "orphan_lanes",
    "counters": "counters",
    "ownership_log": "ownership_log",
    "counts": "counts",
    "pending_version_counts": "pending_version_counts",
    "reset_for_test": "reset_for_test",
}

ERRORS = {
    StaleEpoch.code: StaleEpoch,
    LeaseNotOwned.code: LeaseNotOwned,
    StoreUnavailable.code: StoreUnavailable,
}


def _jsonable(v: Any) -> Any:
    """sqlite3.Row -> dict；list 递归；None 透传。"""
    if v is None:
        return None
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    if hasattr(v, "keys"):
        return {k: v[k] for k in v.keys()}
    return v


class DirectClient:
    """进程内直连。用于兼容的单 worker 入口与单元测试。"""

    mode = "direct"

    def __init__(self, engine: Engine):
        self.engine = engine

    def rpc(self, op: str, **kwargs) -> Any:
        method = getattr(self.engine, _RPC[op])
        return _jsonable(method(**kwargs))

    # store 故障注入（直连模式下即写闸门本身）
    def set_reject_writes(self, reject: bool) -> dict:
        self.engine.reject_writes = reject
        return {"reject_writes": reject}

    def close(self) -> None:
        self.engine.close()


class HttpStore:
    """跨进程 HTTP 客户端，连接独立 store 服务进程。"""

    mode = "http"

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def rpc(self, op: str, **kwargs) -> Any:
        if op not in _RPC:
            raise KeyError(f"unknown rpc {op}")
        body = json.dumps({"op": op, "args": kwargs}).encode()
        req = urllib.request.Request(
            f"{self.base}/rpc", data=body, method="POST",
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                payload = json.loads(r.read())
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                raise StoreError(f"store {e.code}: {raw[:200]!r}")
            code = payload.get("code", "store_error")
            cls = ERRORS.get(code, StoreError)
            raise cls(payload.get("error", code), **payload.get("extra", {}))
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # store 进程不可达等价于无法写入：调用方必须停止出站副作用
            raise StoreUnavailable(f"store unreachable: {e}")
        return payload.get("result")

    def set_reject_writes(self, reject: bool) -> dict:
        """store outage 注入：由 store 服务自己的 admin 端口承载。"""
        body = json.dumps({"reject_writes": reject}).encode()
        req = urllib.request.Request(
            f"{self.base}/admin/store-outage", data=body, method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read())

    def health(self) -> dict:
        with urllib.request.urlopen(f"{self.base}/healthz",
                                    timeout=self.timeout) as r:
            return json.loads(r.read())

    def close(self) -> None:
        pass
