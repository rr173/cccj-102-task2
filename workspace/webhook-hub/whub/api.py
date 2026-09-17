"""统一 HTTP 面：租户业务 API + HA 控制面 + worker 注入接口。

部署形态：
* 兼容入口 ``hub``：本 server 与 Engine、Worker 同进程（store=DirectClient）；
* HA 形态：store 进程提供本 server（worker=None，含 /rpc），
  worker-a/b 各自只挂 ``/worker/*`` 与 ``/test/*``（store=HttpStore，worker 非空）。

所有数据操作都走 store 客户端，因此两种形态的业务语义完全一致。
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .client import DirectClient, HttpStore, StoreError, StaleEpoch, LeaseNotOwned, StoreUnavailable
from .config import HubConfig
from .util import new_id

log = logging.getLogger("whub.api")

METRIC_NAMES = ("acquire", "renew", "steal", "stale_write_rejected",
                "drain_handoff", "orphan_recovered")


def lane_key(eid: str, object_key: str) -> str:
    return "ln_" + hashlib.sha1(f"{eid}|{object_key}".encode()).hexdigest()[:24]


class HubServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, store, cfg: HubConfig, worker=None,
                 engine=None, enable_rpc: bool = False):
        self.store = store
        self.cfg = cfg
        self.worker = worker
        self.engine = engine          # 仅 store 进程：/rpc 直接走引擎
        self.enable_rpc = enable_rpc
        super().__init__(addr, Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = "whub/2.0"

    @property
    def store(self):
        return self.server.store

    @property
    def cfg(self) -> HubConfig:
        return self.server.cfg

    @property
    def worker(self):
        return self.server.worker

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    # ---- helpers -----------------------------------------------------

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code: int, error_code: str, message: str, **extra) -> None:
        self._json(code, {"code": error_code, "error": message, **extra})

    def _read_json(self) -> dict | None:
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            data = json.loads(raw or b"{}")
        except json.JSONDecodeError as e:
            self._err(400, "bad_request", f"invalid json: {e}")
            return None
        if not isinstance(data, dict):
            self._err(400, "bad_request", "body must be a JSON object")
            return None
        return data

    def _auth(self):
        m = re.fullmatch(r"Bearer\s+(\S+)",
                         self.headers.get("Authorization", ""))
        if not m:
            self._err(401, "unauthorized", "missing bearer token")
            return None
        t = self.store.rpc("tenant_by_key", api_key=m.group(1))
        if not t:
            self._err(401, "unauthorized", "invalid api key")
            return None
        return t

    def _admin_auth(self) -> bool:
        token = self.cfg.admin_token
        if not token:
            return True
        m = re.fullmatch(r"Bearer\s+(\S+)",
                         self.headers.get("Authorization", ""))
        if not m or not secrets.compare_digest(m.group(1), token):
            self._err(401, "unauthorized", "admin token required")
            return False
        return True

    def _endpoint_for_tenant(self, eid: str, tenant_row):
        ep = self.store.rpc("endpoint", eid=eid)
        if not ep or ep["tenant_id"] != tenant_row["id"]:
            self._err(404, "not_found", "endpoint not found")
            return None
        return ep

    def _store_error(self, e: StoreError) -> None:
        mapping = {StaleEpoch.code: 409, LeaseNotOwned.code: 409,
                   StoreUnavailable.code: 503}
        self._err(mapping.get(e.code, 500), e.code, str(e), **e.extra)

    # ---- routing -----------------------------------------------------

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/healthz":
                return self._json(200, {"ok": True, "role": self._role()})
            if path == "/metrics":
                return self._metrics()

            if path.startswith("/admin/"):
                return self._admin_get(path)
            if path.startswith("/worker") or path.startswith("/test/"):
                return self._worker_get(path)

            tenant = self._auth()
            if tenant is None:
                return
            if path == "/v1/endpoints":
                return self._list_endpoints(tenant)
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)", path)
            if m:
                return self._get_endpoint(tenant, m.group(1))
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/versions", path)
            if m:
                return self._list_versions(tenant, m.group(1))
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/leases", path)
            if m:
                return self._list_lane_leases(tenant, m.group(1))
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/deliveries", path)
            if m:
                return self._list_jobs(tenant, m.group(1))
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._get_job(tenant, m.group(1))
            self._err(404, "not_found", "no route")
        except StoreError as e:
            self._store_error(e)
        except Exception as e:
            log.exception("GET %s failed", path)
            self._err(500, "internal_error", str(e))

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/rpc" and self.server.enable_rpc:
                return self._rpc()
            if path == "/admin/tenants":
                return self._create_tenant()
            if path.startswith("/admin/"):
                return self._admin_post(path)
            if path.startswith("/test/"):
                return self._worker_post(path)

            tenant = self._auth()
            if tenant is None:
                return
            data = self._read_json()
            if data is None:
                return
            if path == "/v1/endpoints":
                return self._create_endpoint(tenant, data)
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/events", path)
            if m:
                return self._publish(tenant, m.group(1), data)
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/rotate", path)
            if m:
                return self._rotate(tenant, m.group(1), data)
            m = re.fullmatch(
                r"/v1/endpoints/(ep_[A-Za-z0-9]+)/versions/(\d+)/retire", path)
            if m:
                return self._retire(tenant, m.group(1), int(m.group(2)))
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)/replay", path)
            if m:
                return self._replay(tenant, m.group(1))
            self._err(404, "not_found", "no route")
        except StoreError as e:
            self._store_error(e)
        except Exception as e:
            log.exception("POST %s failed", path)
            self._err(500, "internal_error", str(e))

    def do_PATCH(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            tenant = self._auth()
            if tenant is None:
                return
            data = self._read_json()
            if data is None:
                return
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)", path)
            if m:
                return self._update_endpoint(tenant, m.group(1), data)
            self._err(404, "not_found", "no route")
        except StoreError as e:
            self._store_error(e)
        except Exception as e:
            log.exception("PATCH %s failed", path)
            self._err(500, "internal_error", str(e))

    def do_DELETE(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            tenant = self._auth()
            if tenant is None:
                return
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._skip(tenant, m.group(1))
            self._err(404, "not_found", "no route")
        except StoreError as e:
            self._store_error(e)
        except Exception as e:
            log.exception("DELETE %s failed", path)
            self._err(500, "internal_error", str(e))

    def _role(self) -> str:
        if self.worker is not None:
            return f"worker:{self.worker.wid}"
        return "store" if self.server.enable_rpc else "hub"

    # ---- RPC 透传（store 进程） --------------------------------------

    def _rpc(self) -> None:
        data = self._read_json()
        if data is None:
            return
        op = data.get("op")
        args = data.get("args") or {}
        try:
            result = self.store.rpc(str(op), **args)
        except StoreError as e:
            return self._store_error(e)
        except TypeError as e:
            return self._err(400, "bad_args", str(e))
        self._json(200, {"ok": True, "result": result})

    # ---- 租户业务 ----------------------------------------------------

    def _create_tenant(self) -> None:
        data = self._read_json()
        if data is None:
            return
        tid = new_id("tnt")
        api_key = "whk_" + new_id("key")[4:]
        name = str(data.get("name", tid))
        self.store.rpc("create_tenant", tid=tid, name=name, api_key=api_key)
        self._json(201, {"tenant_id": tid, "name": name, "api_key": api_key})

    def _list_endpoints(self, tenant) -> None:
        rows = self.store.rpc("endpoints_for_tenant", tenant_id=tenant["id"])
        self._json(200, [self._endpoint_view(r) for r in rows])

    def _create_endpoint(self, tenant, data: dict) -> None:
        url = data.get("url")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return self._err(400, "bad_request", "valid url required")
        name = str(data.get("name", "endpoint"))
        parallelism = int(data.get("parallelism", 1))
        if not 1 <= parallelism <= 128:
            return self._err(400, "bad_request", "parallelism must be 1..128")
        eid = new_id("ep")
        secret = data.get("secret") or ("whsec_" + secrets.token_hex(24))
        kid = data.get("kid") or "key-1"
        self.store.rpc("create_endpoint", eid=eid, tenant_id=tenant["id"],
                       name=name, version=1, kid=kid, secret=secret, url=url,
                       parallelism=parallelism)
        self._json(201, {"endpoint_id": eid, "name": name, "url": url,
                         "parallelism": parallelism, "active_version": 1,
                         "kid": kid, "secret": secret})

    def _endpoint_view(self, ep: dict) -> dict:
        v = self.store.rpc("version", eid=ep["id"],
                           version=ep["active_version"])
        return {"endpoint_id": ep["id"], "name": ep["name"],
                "url": v["url"] if v else None,
                "active_version": ep["active_version"],
                "kid": v["kid"] if v else None,
                "secret": v["secret"] if v else None,
                "parallelism": ep["parallelism"],
                "disabled": bool(ep["disabled"]),
                "counts": self.store.rpc("counts", eid=ep["id"]),
                "pending_by_version":
                    self.store.rpc("pending_version_counts", eid=ep["id"])}

    def _get_endpoint(self, tenant, eid: str) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if ep:
            self._json(200, self._endpoint_view(ep))

    def _list_versions(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        rows = self.store.rpc("versions", eid=eid)
        self._json(200, [{"version": v["version"], "kid": v["kid"],
                          "url": v["url"], "status": v["status"],
                          "secret": v["secret"], "created_at": v["created_at"],
                          "retired_at": v["retired_at"]} for v in rows])

    def _update_endpoint(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "parallelism" in data:
            n = int(data["parallelism"])
            if not 1 <= n <= 128:
                return self._err(400, "bad_request",
                                 "parallelism must be 1..128")
            self.store.rpc("set_parallelism", eid=eid, n=n)
        if "disabled" in data:
            self.store.rpc("set_disabled", eid=eid,
                           disabled=bool(data["disabled"]))
        self._json(200, self._endpoint_view(
            self.store.rpc("endpoint", eid=eid)))

    def _publish(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "payload" not in data:
            return self._err(400, "bad_request", "payload required")
        object_key = str(data.get("object_key") or data.get("object")
                         or "default")
        idem = str(data["idempotency_key"]) if data.get(
            "idempotency_key") is not None else None
        if idem is not None:
            existing = self.store.rpc("find_event", tenant_id=tenant["id"],
                                      idem_key=idem)
            if existing:
                d = self.store.rpc("job_for_event", event_id=existing["id"])
                return self._json(200, {
                    "event_id": existing["id"],
                    "delivery_id": d["id"] if d else None,
                    "deduped": True, "status": d["status"] if d else None})
        try:
            payload = json.dumps(data["payload"], ensure_ascii=False)
        except (TypeError, ValueError):
            return self._err(400, "bad_request", "payload must be JSON")
        event_id = new_id("evt")
        job_id = new_id("dlv")
        lid = lane_key(eid, object_key)
        row = self.store.rpc("enqueue", lane_id=lid, event_id=event_id,
                             tenant_id=tenant["id"], eid=eid, idem_key=idem,
                             object_key=object_key, payload=payload,
                             job_id=job_id)
        self._json(202, {"event_id": event_id, "delivery_id": job_id,
                         "lane_id": lid, "object_key": object_key,
                         "seq": row["seq"], "signed_version": row["sig_version"],
                         "deduped": False})

    def _rotate(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        cur = self.store.rpc("version", eid=eid,
                             version=ep["active_version"])
        url = data.get("url", cur["url"])
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return self._err(400, "bad_request", "valid url required")
        v = self.store.rpc("rotate_key", eid=eid, url=url,
                           secret=data.get("secret"), kid=data.get("kid"))
        self._json(200, {"active_version": v["version"], "kid": v["kid"],
                         "url": v["url"], "secret": v["secret"],
                         "pending_by_version":
                             self.store.rpc("pending_version_counts", eid=eid),
                         "note": "切换前已排队的 job 用旧快照；新 job 只走新版本"})

    def _retire(self, tenant, eid: str, version: int) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        r = self.store.rpc("retire_version", eid=eid, version=version)
        if r["retired"]:
            self._json(200, r)
        else:
            self._err(409, "version_busy",
                      "version still has queued deliveries", **r)

    def _job_view(self, j: dict) -> dict:
        return {"delivery_id": j["id"], "endpoint_id": j["endpoint_id"],
                "lane_id": j["lane_id"], "event_id": j["event_id"],
                "seq": j["seq"], "sig_version": j["sig_version"],
                "target_url": j["target_url"], "kid": j["kid"],
                "status": j["status"], "attempts": j["attempts"],
                "fail_count": j["fail_count"], "last_status": j["last_status"],
                "last_error": j["last_error"], "not_before": j["not_before"],
                "leased_by": j["leased_by"], "lease_epoch": j["lease_epoch"],
                "created_at": j["created_at"], "updated_at": j["updated_at"]}

    def _owned_job(self, tenant, did: str):
        j = self.store.rpc("job", job_id=did)
        if not j:
            self._err(404, "not_found", "delivery not found")
            return None
        ep = self.store.rpc("endpoint", eid=j["endpoint_id"])
        if not ep or ep["tenant_id"] != tenant["id"]:
            self._err(404, "not_found", "delivery not found")
            return None
        return j

    def _get_job(self, tenant, did: str) -> None:
        j = self._owned_job(tenant, did)
        if j:
            self._json(200, self._job_view(j))

    def _list_jobs(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        rows = self.store.rpc("list_jobs", eid=eid, limit=100)
        self._json(200, [self._job_view(r) for r in rows])

    def _list_lane_leases(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        rows = self.store.rpc("lanes_for_endpoint", eid=eid)
        self._json(200, [self._lease_view(r) for r in rows])

    def _replay(self, tenant, did: str) -> None:
        j = self._owned_job(tenant, did)
        if not j:
            return
        r = self.store.rpc("replay", job_id=did)
        code = 200 if r.get("replayed") else 409
        self._json(code, {"delivery_id": did, **r})

    def _skip(self, tenant, did: str) -> None:
        j = self._owned_job(tenant, did)
        if not j:
            return
        ok = self.store.rpc("skip", job_id=did)
        self._json(200, {"delivery_id": did, "skipped": ok})

    # ---- 控制面：worker / lease / drain / rebalance ------------------

    @staticmethod
    def _lease_view(l: dict) -> dict:
        return {
            "lane_id": l["lane_id"], "endpoint_id": l["endpoint_id"],
            "object_key": l["object_key"],
            "owner_id": l["owner_id"],
            "lease_epoch": l["lease_epoch"],
            "fence_id": (l["fence_id"][:18] + "…") if l["fence_id"] else None,
            "expires_at": l["expires_at"],
            "draining": bool(l["draining"]),
            "last_handoff_reason": l["last_handoff_reason"],
            "not_before": l["not_before"],
            "sig_version": l["sig_version"],
            "target_url": l["target_url"],
            "last_seq": l["last_seq"],
            "updated_at": l["updated_at"]}

    def _admin_get(self, path: str) -> None:
        if not self._admin_auth():
            return
        if path == "/admin/workers":
            workers = self.store.rpc("list_workers")
            leases = self.store.rpc("list_leases")
            owned: dict[str, int] = {}
            for l in leases:
                if l["owner_id"]:
                    owned[l["owner_id"]] = owned.get(l["owner_id"], 0) + 1
            out = []
            now = self.store.rpc("now")
            for w in workers:
                out.append({
                    "worker_id": w["worker_id"],
                    "incarnation": w["incarnation"],
                    "host": w["host"], "port": w["port"],
                    "draining": bool(w["draining"]),
                    "drain_until": w["drain_until"],
                    "last_heartbeat": w["last_heartbeat"],
                    "heartbeat_age": round(now - w["last_heartbeat"], 3),
                    "owned": owned.get(w["worker_id"], 0),
                    "active": self.store.rpc(
                        "active_job_count", worker_id=w["worker_id"])})
            return self._json(200, out)
        if path == "/admin/leases":
            return self._json(200, [self._lease_view(l)
                                    for l in self.store.rpc("list_leases")])
        if path == "/admin/ownership-log":
            return self._json(200, self.store.rpc("ownership_log"))
        if path == "/admin/counters":
            return self._json(200, self.store.rpc("counters"))
        if path == "/admin/orphans":
            return self._json(200, self.store.rpc("orphan_lanes"))
        self._err(404, "not_found", "no route")

    def _admin_post(self, path: str) -> None:
        if not self._admin_auth():
            return
        data = self._read_json()
        if data is None:
            return
        if path == "/admin/store-outage":
            return self._json(200, self.store.set_reject_writes(
                bool(data.get("reject_writes"))))
        if path == "/admin/rebalance":
            budget = int(data.get("budget", self.cfg.rebalance_budget))
            r = self.store.rpc("rebalance", budget=budget,
                               trigger=data.get("trigger", "api"),
                               ttl=self.cfg.lease_ttl)
            return self._json(200, r)
        if path == "/admin/reset":
            self.store.rpc("reset_for_test")
            return self._json(200, {"ok": True})
        m = re.fullmatch(r"/admin/workers/([A-Za-z0-9_.-]+)/drain", path)
        if m:
            wid = m.group(1)
            draining = bool(data.get("draining", True))
            deadline = data.get("deadline", self.cfg.drain_deadline)
            r = self.store.rpc("set_drain", worker_id=wid,
                               draining=draining,
                               deadline=float(deadline) if deadline else None)
            return self._json(200, r)
        self._err(404, "not_found", "no route")

    # ---- worker 自身视图 / 测试注入 ----------------------------------

    def _worker_get(self, path: str) -> None:
        if self.worker is None:
            return self._err(404, "not_found", "worker endpoints only")
        if path in ("/worker/status", "/worker/view"):
            return self._json(200, self.worker.view())
        self._err(404, "not_found", "no route")

    def _worker_post(self, path: str) -> None:
        if self.worker is None:
            return self._err(404, "not_found", "worker endpoints only")
        if not self.cfg.freeze_support:
            return self._err(403, "disabled", "test hooks disabled")
        data = self._read_json()
        if data is None:
            return
        if path == "/test/freeze":
            until = self.worker.freeze(float(data.get("seconds", 5)))
            return self._json(202, {"freezing": True, "resume_at": until})
        if path == "/test/stale-attempts":
            r = self.worker.stale_attempts(
                lane_id=data["lane_id"], epoch=int(data["epoch"]),
                fence=data["fence"], job_id=data.get("job_id"))
            return self._json(200, r)
        self._err(404, "not_found", "no route")

    # ---- Prometheus 指标 ---------------------------------------------

    def _metrics(self) -> None:
        lines = [
            "# HELP whub_lease_ops lease operations by worker and kind",
            "# TYPE whub_lease_ops counter",
        ]
        counts = {}
        for r in self.store.rpc("counters"):
            counts[(r["worker_id"], r["name"])] = r["value"]
        workers = [w["worker_id"] for w in self.store.rpc("list_workers")]
        for wid in sorted(set(workers) | {w for w, _ in counts}):
            for name in METRIC_NAMES:
                lines.append(
                    f'whub_lease_ops{{worker="{wid}",kind="{name}"}} '
                    f'{counts.get((wid, name), 0)}')
        leases = self.store.rpc("list_leases")
        owned: dict[str, int] = {}
        for l in leases:
            if l["owner_id"]:
                owned[l["owner_id"]] = owned.get(l["owner_id"], 0) + 1
        lines += ["# TYPE whub_owned_lanes gauge",
                  "# HELP whub_owned_lanes lanes currently owned by worker"]
        for wid in sorted(owned):
            lines.append(
                f'whub_owned_lanes{{worker="{wid}"}} {owned[wid]}')
        lines.append("# TYPE whub_active_jobs gauge")
        for wid in sorted(set(workers)):
            lines.append(
                f'whub_active_jobs{{worker="{wid}"}} '
                f'{self.store.rpc("active_job_count", worker_id=wid)}')
        lines.append("# TYPE whub_orphan_lanes gauge")
        lines.append(
            f'whub_orphan_lanes {len(self.store.rpc("orphan_lanes"))}')
        body = "\n".join(lines) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())
