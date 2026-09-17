"""Hub 控制面与入口 API（BaseHTTPRequestHandler，零三方依赖）。

- 租户面：端点/事件/密钥轮换/投递查询（Bearer 鉴权，租户隔离 404）。
- 集群面：worker 列表、lease 列表、drain/undrain、rebalance 触发、
  所有权审计链、指标 JSON，以及（可选开放的）陈旧写注入与 store outage 开关。
  store / worker / hub 三种进程都可加载本 API，数据完全来自共享 durable store。

错误响应统一形如 ``{"error": "<code>", ...}``；lease 协议的稳定错误码：
``stale_epoch`` / ``lease_not_owned`` / ``store_unavailable`` /
``worker_not_found``。
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import HubConfig
from .db import Store, LeaseError, StoreUnavailable
from .util import new_id

log = logging.getLogger("whub.api")


def _row(r) -> dict:
    return {k: r[k] for k in r.keys()}


class HubServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, store: Store, cfg: HubConfig, worker=None):
        self.store = store
        self.cfg = cfg
        self.worker = worker      # store-only 进程为 None
        super().__init__(addr, Handler)

    def kick(self, endpoint_id: str) -> None:
        if self.worker is None:
            return
        with self.worker._rlock:
            r = self.worker.runners.get(endpoint_id)
        if r:
            r.kick()


class Handler(BaseHTTPRequestHandler):
    server_version = "whub/2.0"

    @property
    def store(self) -> Store:
        return self.server.store

    @property
    def cfg(self) -> HubConfig:
        return self.server.cfg

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    # -- helpers -------------------------------------------------------

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, code: int, error: str, message: str = "", **extra) -> None:
        payload = {"error": error}
        if message:
            payload["message"] = message
        payload.update(extra)
        self._json(code, payload)

    def _read_json(self) -> dict | None:
        n = int(self.headers.get("Content-Length", 0) or 0)
        if not n:
            return {}
        raw = self.rfile.read(n)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            self._error(400, "invalid_json", str(e))
            return None
        if not isinstance(data, dict):
            self._error(400, "invalid_body", "body must be a JSON object")
            return None
        return data

    def _auth(self):
        m = re.fullmatch(r"Bearer\s+(\S+)", self.headers.get("Authorization", ""))
        if not m:
            self._error(401, "unauthorized", "missing bearer token")
            return None
        t = self.store.tenant_by_key(m.group(1))
        if not t:
            self._error(401, "unauthorized", "invalid api key")
            return None
        return t

    def _endpoint_for_tenant(self, eid: str, tenant_row):
        ep = self.store.endpoint(eid)
        if not ep or ep["tenant_id"] != tenant_row["id"]:
            self._error(404, "not_found", "endpoint not found")
            return None
        return ep

    # -- routing -------------------------------------------------------

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/healthz":
                return self._json(200, {"ok": True, "role": self.cfg.role,
                                        "worker_id": self.cfg.worker_id or None})
            if path == "/metrics":
                return self._metrics_prom()
            if path == "/admin/metrics":
                return self._json(200, self.store.metrics_snapshot())
            if path == "/admin/workers":
                return self._list_workers()
            if path == "/admin/leases":
                return self._list_leases()
            if path == "/admin/audit":
                return self._list_audit()
            if path == "/admin/coord":
                return self._json(200, self.store.coord_info())

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
            m = re.fullmatch(r"/v1/endpoints/(ep_[A-Za-z0-9]+)/deliveries",
                             path)
            if m:
                return self._list_deliveries(tenant, m.group(1))
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._get_delivery(tenant, m.group(1))
            self._error(404, "no_route", path)
        except LeaseError as e:
            self._lease_error(e)
        except StoreUnavailable as e:
            self._error(503, StoreUnavailable.error_code, str(e))
        except Exception as e:
            log.exception("GET %s failed", path)
            self._error(500, "internal_error", str(e))

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/admin/tenants":
                return self._create_tenant()
            if path == "/admin/rebalance":
                return self._request_rebalance()
            if path == "/admin/outage":
                return self._toggle_outage()
            m = re.fullmatch(r"/admin/workers/([A-Za-z0-9_.\-]+)/drain", path)
            if m:
                return self._drain_worker(m.group(1))
            m = re.fullmatch(r"/admin/workers/([A-Za-z0-9_.\-]+)/undrain", path)
            if m:
                return self._undrain_worker(m.group(1))
            m = re.fullmatch(r"/admin/lanes/(ep_[A-Za-z0-9]+)/assign", path)
            if m:
                return self._assign_lane(m.group(1))
            m = re.fullmatch(r"/admin/debug/(renew|claim|complete)", path)
            if m:
                return self._debug_stale_write(m.group(1))

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
            self._error(404, "no_route", path)
        except LeaseError as e:
            self._lease_error(e)
        except StoreUnavailable as e:
            self._error(503, StoreUnavailable.error_code, str(e))
        except Exception as e:
            log.exception("POST %s failed", path)
            self._error(500, "internal_error", str(e))

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
            self._error(404, "no_route", path)
        except LeaseError as e:
            self._lease_error(e)
        except StoreUnavailable as e:
            self._error(503, StoreUnavailable.error_code, str(e))
        except Exception as e:
            log.exception("PATCH %s failed", path)
            self._error(500, "internal_error", str(e))

    def do_DELETE(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            tenant = self._auth()
            if tenant is None:
                return
            m = re.fullmatch(r"/v1/deliveries/(dlv_[A-Za-z0-9]+)", path)
            if m:
                return self._skip_delivery(tenant, m.group(1))
            self._error(404, "no_route", path)
        except LeaseError as e:
            self._lease_error(e)
        except StoreUnavailable as e:
            self._error(503, StoreUnavailable.error_code, str(e))
        except Exception as e:
            log.exception("DELETE %s failed", path)
            self._error(500, "internal_error", str(e))

    def _lease_error(self, e: LeaseError) -> None:
        self._error(e.http_status, e.error_code, str(e), lane_id=e.lane_id,
                    old_epoch=e.old_epoch, new_epoch=e.new_epoch)

    # -- 集群面 --------------------------------------------------------

    def _list_workers(self) -> None:
        rows = self.store.list_workers(self.cfg.effective_worker_stale_after())
        self._json(200, [self._worker_view(r) for r in rows])

    @staticmethod
    def _worker_view(r) -> dict:
        return {"worker_id": r["worker_id"], "incarnation": r["incarnation"],
                "pid": r["pid"], "alive": r["alive"],
                "draining": bool(r["draining"]),
                "drain_deadline": r["drain_deadline"] or None,
                "stopping": bool(r["stopping"]),
                "heartbeat_at": r["heartbeat_at"],
                "heartbeat_age": round(r["age_heartbeat"], 3),
                "owned": r["owned"], "active": r["active"]}

    def _list_leases(self) -> None:
        now = self.store.db_now()
        out = []
        for l in self.store.list_leases():
            out.append({
                "lane_id": l["lane_id"],
                "owner_id": l["owner_id"],
                "lease_epoch": l["lease_epoch"],
                "fence_id": l["fence_id"],
                "expires_at": l["expires_at"],
                "expires_in": round(l["expires_at"] - now, 3),
                "expired": (l["owner_id"] is not None
                            and l["expires_at"] < now),
                "draining": bool(l["draining"]),
                "drain_deadline": l["drain_deadline"] or None,
                "last_handoff_reason": l["last_handoff_reason"],
                "created_at": l["created_at"],
                "updated_at": l["updated_at"]})
        self._json(200, out)

    def _list_audit(self) -> None:
        qs = self.path.split("?", 1)[1] if "?" in self.path else ""
        lane = None
        for kv in qs.split("&"):
            if kv.startswith("lane="):
                lane = kv.split("=", 1)[1]
        rows = self.store.list_audit(limit=200, lane_id=lane)
        self._json(200, [{
            "id": r["id"], "at": r["at"], "lane_id": r["lane_id"],
            "old_owner": r["old_owner"], "new_owner": r["new_owner"],
            "old_epoch": r["old_epoch"], "new_epoch": r["new_epoch"],
            "reason": r["reason"], "by_worker": r["by_worker"]}
            for r in rows])

    def _drain_worker(self, worker_id: str) -> None:
        data = self._read_json()
        if data is None:
            return
        try:
            deadline = float(data["deadline"]) if "deadline" in data else None
            row = self.store.set_worker_drain(worker_id, True, deadline)
        except KeyError:
            return self._error(404, "worker_not_found", worker_id)
        # 通知本进程 worker 立即感知（其余成员靠下一次心跳读库发现）
        if self.server.worker and self.server.worker.worker_id == worker_id:
            self.server.worker.draining = True
            self.server.worker.drain_deadline = row["drain_deadline"]
        self._json(200, {"worker_id": worker_id, "draining": True,
                         "drain_deadline": row["drain_deadline"]})

    def _undrain_worker(self, worker_id: str) -> None:
        try:
            self.store.set_worker_drain(worker_id, False)
        except KeyError:
            return self._error(404, "worker_not_found", worker_id)
        if self.server.worker and self.server.worker.worker_id == worker_id:
            self.server.worker.draining = False
            self.server.worker.drain_deadline = None
        self._json(200, {"worker_id": worker_id, "draining": False})

    def _request_rebalance(self) -> None:
        data = self._read_json()
        if data is None:
            return
        max_moves = int(data.get("max_moves", self.cfg.rebalance_max_moves))
        if max_moves < 1:
            return self._error(400, "invalid_max_moves", "must be >= 1")
        self.store.request_rebalance(max_moves)
        if self.server.worker:
            self.server.worker.wake.set()
        self._json(202, {"requested": True, "max_moves": max_moves})

    def _assign_lane(self, lane_id: str) -> None:
        data = self._read_json()
        if data is None:
            return
        to = str(data.get("worker_id", ""))
        if not to:
            return self._error(400, "worker_id_required", "")
        reason = str(data.get("reason", "control_plane_assign"))
        try:
            row = self.store.force_assign_lane(
                lane_id, to, ttl=self.cfg.lease_ttl, reason=reason)
        except LeaseError as e:
            return self._lease_error(e)
        if self.server.worker:
            self.server.worker.wake.set()
        self._json(200, {"lane_id": lane_id, "owner_id": row["owner_id"],
                         "lease_epoch": row["lease_epoch"],
                         "last_handoff_reason": row["last_handoff_reason"]})

    def _toggle_outage(self) -> None:
        data = self._read_json()
        if data is None:
            return
        marker = self.cfg.outage_marker
        if not marker:
            return self._error(400, "outage_not_configured",
                               "set WHUB_OUTAGE_MARKER to inject outage")
        on = bool(data.get("on", True))
        if on:
            os.makedirs(os.path.dirname(marker) or ".", exist_ok=True)
            with open(marker, "w") as f:
                f.write("outage")
        else:
            try:
                os.remove(marker)
            except FileNotFoundError:
                pass
        self._json(200, {"store_unavailable": on, "marker": marker})

    def _debug_stale_write(self, op: str) -> None:
        """以“上一任 owner + 旧 epoch + 旧 fence”身份重放三类写操作。

        仅当 WHUB_DEBUG_ADMIN=1 开放。数据库必须全部以 stale_epoch 拒绝，
        且当前 owner 已落盘状态保持不变——验收场景 3 的确定性注入点。"""
        if not self.cfg.debug_admin:
            return self._error(404, "no_route", "debug admin disabled")
        data = self._read_json()
        if data is None:
            return
        lane = str(data.get("lane_id", ""))
        lease = self.store.lease_view(lane)
        if lease is None:
            return self._error(404, "not_found", "lane not found")
        audit = self.store.list_audit(limit=5, lane_id=lane)
        latest = audit[0] if audit else None
        zombie_owner = (latest["old_owner"] if latest and latest["old_owner"]
                        else (lease["owner_id"] or "zombie"))
        zombie_epoch = lease["lease_epoch"] - 1
        zombie_fence = "fnc_stale_zombie"
        try:
            if op == "renew":
                self.store.renew_lane(lane, zombie_owner, zombie_epoch,
                                      zombie_fence, self.cfg.lease_ttl)
            elif op == "claim":
                self.store.claim_due(lane, 10, zombie_owner, zombie_epoch,
                                     zombie_fence)
            elif op == "complete":
                did = data.get("delivery_id")
                if not did:
                    row = self.store.list_deliveries(lane, limit=1,
                                                     status="inflight")
                    if not row:
                        return self._error(409, "no_inflight_delivery",
                                           "need an inflight delivery")
                    did = row[0]["id"]
                self.store.mark_succeeded(did, 200, worker_id=zombie_owner,
                                          epoch=zombie_epoch,
                                          fence=zombie_fence)
        except LeaseError as e:
            self.store.incr_metric("__debug__", "stale_write_rejected")
            return self._lease_error(e)
        self._error(500, "assertion_violation",
                    "stale write was ACCEPTED — fencing broken")

    # -- 租户面 --------------------------------------------------------

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
            return self._error(400, "invalid_url", "valid url required")
        name = str(data.get("name", "endpoint"))
        parallelism = int(data.get("parallelism", 1))
        if parallelism < 1 or parallelism > 128:
            return self._error(400, "invalid_parallelism",
                               "parallelism must be 1..128")
        eid = new_id("ep")
        secret = data.get("secret") or ("whsec_" + secrets.token_hex(24))
        kid = data.get("kid") or "key-1"
        self.store.create_endpoint(eid, tenant["id"], name, 1, kid, secret,
                                   url, parallelism)
        self._json(201, {"endpoint_id": eid, "name": name, "url": url,
                         "parallelism": parallelism,
                         "active_version": 1,
                         "kid": kid, "secret": secret})

    def _lease_view_public(self, eid: str) -> dict:
        l = self.store.lease_view(eid)
        if l is None:
            return {}
        return {"owner_id": l["owner_id"], "lease_epoch": l["lease_epoch"],
                "expires_at": l["expires_at"], "draining": bool(l["draining"]),
                "last_handoff_reason": l["last_handoff_reason"]}

    def _endpoint_view(self, ep) -> dict:
        v = self.store.version(ep["id"], ep["active_version"])
        return {"endpoint_id": ep["id"], "name": ep["name"], "url": v["url"],
                "active_version": ep["active_version"], "kid": v["kid"],
                "secret": v["secret"], "parallelism": ep["parallelism"],
                "disabled": bool(ep["disabled"]),
                "lease": self._lease_view_public(ep["id"]),
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
            "status": v["status"], "secret": v["secret"],
            "created_at": v["created_at"], "retired_at": v["retired_at"],
        } for v in self.store.versions(eid)])

    def _update_endpoint(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "parallelism" in data:
            n = int(data["parallelism"])
            if n < 1 or n > 128:
                return self._error(400, "invalid_parallelism",
                                   "parallelism must be 1..128")
            self.store.set_parallelism(eid, n)
        if "disabled" in data:
            self.store.set_disabled(eid, bool(data["disabled"]))
        self.server.kick(eid)
        self._json(200, self._endpoint_view(self.store.endpoint(eid)))

    def _publish_event(self, tenant, eid: str, data: dict) -> None:
        ep = self._endpoint_for_tenant(eid, tenant)
        if not ep:
            return
        if "payload" not in data:
            return self._error(400, "payload_required", "")
        object_key = str(data.get("object_key") or data.get("object")
                         or "default")
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
            return self._error(400, "invalid_payload", "payload must be JSON")
        event_id = new_id("evt")
        delivery_id = new_id("dlv")
        row = self.store.enqueue(event_id, tenant["id"], eid, idem, object_key,
                                 payload, delivery_id)
        self.server.kick(eid)
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
            return self._error(400, "invalid_url", "valid url required")
        secret = data.get("secret")
        kid = data.get("kid")
        v = self.store.rotate_key(eid, url, secret, kid)
        self.server.kick(eid)
        self._json(200, {"active_version": v["version"], "kid": v["kid"],
                         "url": v["url"], "secret": v["secret"],
                         "pending_by_version":
                             self.store.pending_version_counts(eid)})

    def _retire_version(self, tenant, eid: str, version: int) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        result = self.store.retire_version(eid, version)
        if result["retired"]:
            self._json(200, result)
        else:
            self._error(409, "version_busy",
                        "version still has queued deliveries", **result)

    def _delivery_view(self, d) -> dict:
        l = self.store.lease_view(d["endpoint_id"])
        return {"delivery_id": d["id"], "endpoint_id": d["endpoint_id"],
                "event_id": d["event_id"], "object_key": d["object_key"],
                "seq": d["seq"], "sig_version": d["sig_version"],
                "target_url": d["target_url"], "kid": d["kid"],
                "status": d["status"], "attempts": d["attempts"],
                "fail_count": d["fail_count"], "last_status": d["last_status"],
                "last_error": d["last_error"], "not_before": d["not_before"],
                "lease": ({"owner_id": l["owner_id"],
                           "lease_epoch": l["lease_epoch"]}
                          if l is not None else None),
                "created_at": d["created_at"], "updated_at": d["updated_at"]}

    def _get_delivery(self, tenant, did: str) -> None:
        d = self.store.delivery(did)
        if not d:
            return self._error(404, "not_found", "delivery not found")
        ep = self.store.endpoint(d["endpoint_id"])
        if not ep or ep["tenant_id"] != tenant["id"]:
            return self._error(404, "not_found", "delivery not found")
        self._json(200, self._delivery_view(d))

    def _list_deliveries(self, tenant, eid: str) -> None:
        if not self._endpoint_for_tenant(eid, tenant):
            return
        rows = self.store.list_deliveries(eid, limit=100)
        self._json(200, [self._delivery_view(r) for r in rows])

    def _owned_delivery(self, tenant, did: str):
        d = self.store.delivery(did)
        if not d:
            self._error(404, "not_found", "delivery not found")
            return None
        ep = self.store.endpoint(d["endpoint_id"])
        if not ep or ep["tenant_id"] != tenant["id"]:
            self._error(404, "not_found", "delivery not found")
            return None
        return d

    def _replay(self, tenant, did: str) -> None:
        d = self._owned_delivery(tenant, did)
        if not d:
            return
        result = self.store.replay(did)
        if result.get("replayed"):
            self.server.kick(d["endpoint_id"])
            self._json(200, {"delivery_id": did, **result})
        else:
            self._error(409, "replay_refused", result.get("reason", ""),
                        delivery_id=did, **result)

    def _skip_delivery(self, tenant, did: str) -> None:
        d = self._owned_delivery(tenant, did)
        if not d:
            return
        ok = self.store.skip(did)
        if ok:
            self.server.kick(d["endpoint_id"])
        self._json(200, {"delivery_id": did, "skipped": ok})

    def _metrics_prom(self) -> None:
        lines = []
        for eid in self.store.active_endpoint_ids():
            c = self.store.counts(eid)
            for state in ("pending", "inflight", "succeeded", "dead",
                          "canceled"):
                lines.append(f'whub_deliveries{{endpoint="{eid}",status="{state}"}} '
                             f'{c.get(state, 0)}')
        snap = self.store.metrics_snapshot()
        for wid, counters in snap.items():
            for name, n in counters.items():
                lines.append(
                    f'whub_lease_ops{{worker="{wid}",op="{name}"}} {n}')
        body = "\n".join(lines) + "\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())
