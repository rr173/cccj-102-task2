"""Hub 控制面与入口 API（BaseHTTPRequestHandler，零三方依赖）。

鉴权：Authorization: Bearer <tenant api_key>。
所有业务接口都以租户身份限定作用域，端点/投递不属于本租户一律 404，
因此一个租户无法看到或操作其他租户的数据。
"""
from __future__ import annotations

import json
import logging
import re
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import HubConfig
from .db import Store
from .dispatcher import Supervisor
from .util import new_id

log = logging.getLogger("whub.api")


def _row(r) -> dict:
    return {k: r[k] for k in r.keys()}


class HubServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, store: Store, supervisor: Supervisor,
                 cfg: HubConfig):
        self.store = store
        self.supervisor = supervisor
        self.cfg = cfg
        super().__init__(addr, Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = "whub/1.0"

    @property
    def store(self) -> Store:
        return self.server.store

    @property
    def supervisor(self) -> Supervisor:
        return self.server.supervisor

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    # -- helpers -------------------------------------------------------

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict | None:
        """解析请求体；空体返回 {}，解析失败已写 400 并返回 None。"""
        n = int(self.headers.get("Content-Length", 0) or 0)
        if not n:
            return {}
        raw = self.rfile.read(n)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            self._json(400, {"error": f"invalid json: {e}"})
            return None
        if not isinstance(data, dict):
            self._json(400, {"error": "body must be a JSON object"})
            return None
        return data

    def _auth(self):
        """返回当前租户 Row；失败已写响应。"""
        m = re.fullmatch(r"Bearer\s+(\S+)", self.headers.get("Authorization", ""))
        if not m:
            self._json(401, {"error": "missing bearer token"})
            return None
        t = self.store.tenant_by_key(m.group(1))
        if not t:
            self._json(401, {"error": "invalid api key"})
            return None
        return t

    def _endpoint_for_tenant(self, eid: str, tenant_row):
        ep = self.store.endpoint(eid)
        if not ep or ep["tenant_id"] != tenant_row["id"]:
            self._json(404, {"error": "endpoint not found"})
            return None
        return ep

    # -- routing -------------------------------------------------------

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/healthz":
                return self._json(200, {"ok": True})
            if path == "/metrics":
                return self._metrics()

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
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/deliveries", path)
            if m:
                return self._list_deliveries(tenant, m.group(1))
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._get_delivery(tenant, m.group(1))
            self._json(404, {"error": "no route"})
        except Exception as e:
            log.exception("GET %s failed", path)
            self._json(500, {"error": str(e)})

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            # 公开：自助创建租户（便于一条命令体验；生产可关闭/改为内部接口）
            if path == "/admin/tenants":
                return self._create_tenant()

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
                return self._publish_event(tenant, m.group(1), data)
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/rotate", path)
            if m:
                return self._rotate(tenant, m.group(1), data)
            m = re.fullmatch(
                r"/v1/endpoints/(ep_[A-Za-z0-9]+)/versions/(\d+)/retire", path)
            if m:
                return self._retire_version(tenant, m.group(1), int(m.group(2)))
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)/replay", path)
            if m:
                return self._replay(tenant, m.group(1))
            self._json(404, {"error": "no route"})
        except Exception as e:
            log.exception("POST %s failed", path)
            self._json(500, {"error": str(e)})

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
            self._json(404, {"error": "no route"})
        except Exception as e:
            log.exception("PATCH %s failed", path)
            self._json(500, {"error": str(e)})

    def do_DELETE(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            tenant = self._auth()
            if tenant is None:
                return
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._skip_delivery(tenant, m.group(1))
            self._json(404, {"error": "no route"})
        except Exception as e:
            log.exception("DELETE %s failed", path)
            self._json(500, {"error": str(e)})

    # -- handlers ------------------------------------------------------

    def _create_tenant(self) -> None:
        data = self._read_json()
        if data is None:
            return
        tid = new_id("tnt")
        api_key = "whk_" + new_id("key")[4:]
        name = str(data.get("name", tid))
        self.store.create_tenant(tid, name, api_key)
        self._json(201, {"tenant_id": tid, "name": name, "api_key": api_key})

    def _list_endpoints(self, tenant) -> None:
        rows = self.store.endpoints_for_tenant(tenant["id"])
        self._json(200, [self._endpoint_view(r) for r in rows])

    def _create_endpoint(self, tenant, data: dict) -> None:
        url = data.get("url")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return self._json(400, {"error": "valid url required"})
        name = str(data.get("name", "endpoint"))
        parallelism = int(data.get("parallelism", 1))
        if parallelism < 1 or parallelism > 128:
            return self._json(400, {"error": "parallelism must be 1..128"})
        eid = new_id("ep")
        secret = data.get("secret") or ("whsec_" + secrets.token_hex(24))
        kid = data.get("kid") or "key-1"
        self.store.create_endpoint(eid, tenant["id"], name, 1, kid, secret, url,
                                   parallelism)
        self._json(201, {"endpoint_id": eid, "name": name, "url": url,
                         "parallelism": parallelism,
                         "active_version": 1,
                         "kid": kid, "secret": secret})

    def _endpoint_view(self, ep) -> dict:
        v = self.store.version(ep["id"], ep["active_version"])
        return {"endpoint_id": ep["id"], "name": ep["name"], "url": v["url"],
                "active_version": ep["active_version"], "kid": v["kid"],
                "secret": v["secret"], "parallelism": ep["parallelism"],
                "disabled": bool(ep["disabled"]),
                "counts": self.store.counts(ep["id"]),
                "pending_by_version": self.store.pending_version_counts(ep["id"])}

    def _get_endpoint(self, tenant, eid: str) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if ep:
            self._json(200, self._endpoint_view(ep))

    def _list_versions(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        self._json(200, [{
            "version": v["version"], "kid": v["kid"], "url": v["url"],
            "status": v["status"],
            "secret": v["secret"],
            "created_at": v["created_at"], "retired_at": v["retired_at"],
        } for v in self.store.versions(eid)])

    def _update_endpoint(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "parallelism" in data:
            n = int(data["parallelism"])
            if n < 1 or n > 128:
                return self._json(400, {"error": "parallelism must be 1..128"})
            self.store.set_parallelism(eid, n)
        if "disabled" in data:
            self.store.set_disabled(eid, bool(data["disabled"]))
        self._json(200, self._endpoint_view(self.store.endpoint(eid)))

    def _publish_event(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "payload" not in data:
            return self._json(400, {"error": "payload required"})
        object_key = str(data.get("object_key") or data.get("object") or "default")
        idem = data.get("idempotency_key")
        if idem is not None:
            idem = str(idem)
            existing = self.store.find_event(tenant["id"], idem)
            if existing:
                d = self.store.delivery_for_event(existing["id"])
                return self._json(200, {
                    "event_id": existing["id"],
                    "delivery_id": d["id"] if d else None,
                    "deduped": True,
                    "status": d["status"] if d else None})
        try:
            payload = json.dumps(data["payload"], ensure_ascii=False)
        except (TypeError, ValueError):
            return self._json(400, {"error": "payload must be JSON"})
        event_id = new_id("evt")
        delivery_id = new_id("dlv")
        row = self.store.enqueue(event_id, tenant["id"], eid, idem, object_key,
                                 payload, delivery_id)
        self.supervisor.kick(eid)
        self._json(202, {"event_id": event_id, "delivery_id": delivery_id,
                         "object_key": object_key, "seq": row["seq"],
                         "signed_version": row["sig_version"],
                         "deduped": False})

    def _rotate(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        cur = self.store.version(eid, ep["active_version"])
        url = data.get("url", cur["url"])
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return self._json(400, {"error": "valid url required"})
        secret = data.get("secret")
        kid = data.get("kid")
        v = self.store.rotate_key(eid, url, secret, kid)
        self.supervisor.kick(eid)
        self._json(200, {"active_version": v["version"], "kid": v["kid"],
                         "url": v["url"], "secret": v["secret"],
                         "pending_by_version": self.store.pending_version_counts(eid),
                         "note": "切换前已排队的投递继续用旧版本签名；新事件只走新版本"})

    def _retire_version(self, tenant, eid: str, version: int) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        result = self.store.retire_version(eid, version)
        if result["retired"]:
            self._json(200, result)
        else:
            self._json(409, {"error": "version still has queued deliveries",
                             **result})

    def _delivery_view(self, d) -> dict:
        return {"delivery_id": d["id"], "endpoint_id": d["endpoint_id"],
                "event_id": d["event_id"], "object_key": d["object_key"],
                "seq": d["seq"], "sig_version": d["sig_version"],
                "target_url": d["target_url"], "kid": d["kid"],
                "status": d["status"], "attempts": d["attempts"],
                "fail_count": d["fail_count"], "last_status": d["last_status"],
                "last_error": d["last_error"], "not_before": d["not_before"],
                "created_at": d["created_at"], "updated_at": d["updated_at"]}

    def _get_delivery(self, tenant, did: str) -> None:
        d = self.store.delivery(did)
        if not d:
            return self._json(404, {"error": "delivery not found"})
        ep = self.store.endpoint(d["endpoint_id"])
        if not ep or ep["tenant_id"] != tenant["id"]:
            return self._json(404, {"error": "delivery not found"})
        self._json(200, self._delivery_view(d))

    def _list_deliveries(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        rows = self.store.list_deliveries(eid, limit=100)
        self._json(200, [self._delivery_view(r) for r in rows])

    def _owned_delivery(self, tenant, did: str):
        d = self.store.delivery(did)
        if not d:
            self._json(404, {"error": "delivery not found"})
            return None
        ep = self.store.endpoint(d["endpoint_id"])
        if not ep or ep["tenant_id"] != tenant["id"]:
            self._json(404, {"error": "delivery not found"})
            return None
        return d

    def _replay(self, tenant, did: str) -> None:
        d = self._owned_delivery(tenant, did)
        if not d:
            return
        result = self.store.replay(did)
        if result.get("replayed"):
            self.supervisor.kick(d["endpoint_id"])
            self._json(200, {"delivery_id": did, **result})
        else:
            # 成功的事件拒绝重放 / 已在途：幂等确认，不产生新投递
            self._json(409, {"delivery_id": did, **result})

    def _skip_delivery(self, tenant, did: str) -> None:
        d = self._owned_delivery(tenant, did)
        if not d:
            return
        ok = self.store.skip(did)
        if ok:
            self.supervisor.kick(d["endpoint_id"])
        self._json(200, {"delivery_id": did, "skipped": ok})

    def _metrics(self) -> None:
        lines = []
        for eid in self.store.active_endpoint_ids():
            c = self.store.counts(eid)
            for state in ("pending", "inflight", "succeeded", "dead", "canceled"):
                lines.append(f'whub_deliveries{{endpoint="{eid}",status="{state}"}} '
                             f'{c.get(state, 0)}')
        body = "\n".join(lines) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())
