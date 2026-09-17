"""模拟外部合作方接收方（sink）。

能力：
- HMAC-SHA256 签名校验（X-Whub-Signature: t=..,v1=..），可要求特定 kid；
- 按 (path, event_id) 幂等：同一事件重复投递返回首次的结果，不重复计数；
- 管理接口注入故障：delay（超时）、ratelimit（429 + Retry-After）、down（503）、
  badsig、fail（永久 400，模拟“毒消息”）；
- 统计到达顺序与最大并行度，供端到端验收断言。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VALID_MODES = ("ok", "delay", "ratelimit", "down", "badsig", "fail", "requirekid")


class SinkState:
    def __init__(self):
        # RLock：_webhook 持锁期间会调用同样需要取锁的 _verify
        self.lock = threading.RLock()
        # path -> rule dict
        self.rules: dict[str, dict] = {}
        # path -> {event_id: first_response}
        self.seen: dict[str, dict[str, dict]] = {}
        # path -> [receipts]
        self.receipts: dict[str, list[dict]] = {}
        self.counters: dict[str, dict] = {}
        self.inflight: dict[str, int] = {}
        self.max_parallel: dict[str, int] = {}
        self.duplicates = 0
        self.bad_signatures = 0
        # kid -> secret（支持轮换后两个 kid 都有效）
        self.keys: dict[str, str] = {}

    def _counters(self, path: str) -> dict:
        return self.counters.setdefault(path, {
            "received": 0, "accepted": 0, "duplicates": 0,
            "ratelimited": 0, "unavailable": 0, "permanent": 0,
            "bad_signature": 0})


class SinkServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, state: SinkState):
        self.state = state
        super().__init__(addr, SinkHandler)


class SinkHandler(BaseHTTPRequestHandler):
    server_version = "sink/1.0"

    def log_message(self, fmt, *args):
        pass

    @property
    def state(self) -> SinkState:
        return self.server.state

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- admin ---------------------------------------------------------

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/healthz":
            return self._json(200, {"ok": True, "service": "sink"})
        if path == "/admin/stats":
            with self.state.lock:
                return self._json(200, {
                    "rules": self.state.rules,
                    "receipts": {p: [dict(r, secret=None) for r in rs]
                                 for p, rs in self.state.receipts.items()},
                    "counters": self.state.counters,
                    "max_parallel": self.state.max_parallel,
                    "duplicates": self.state.duplicates,
                    "bad_signatures": self.state.bad_signatures,
                    "keys": list(self.state.keys)})
        self._json(404, {"error": "no route"})

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/admin/rules":
            return self._set_rule()
        if path == "/admin/keys":
            return self._add_key()
        if path == "/admin/reset":
            return self._reset()
        if path.startswith("/admin/"):
            self._json(404, {"error": "no route"})
            return
        self._webhook(path)

    def _read_body(self) -> bytes:
        n = int(self.headers.get("Content-Length", 0) or 0)
        return self.rfile.read(n) if n else b""

    def _set_rule(self) -> None:
        data = json.loads(self._read_body() or b"{}")
        prefix = data.get("path", "/")
        mode = data.get("mode", "ok")
        if mode not in VALID_MODES:
            return self._json(400, {"error": f"mode must be one of {VALID_MODES}"})
        rule = {"mode": mode,
                "delay": float(data.get("delay", 4.0)),
                "retry_after": float(data.get("retry_after", 1.0)),
                "require_kid": data.get("require_kid")}
        with self.state.lock:
            self.state.rules[prefix] = rule
        self._json(200, {"path": prefix, **rule})

    def _add_key(self) -> None:
        data = json.loads(self._read_body() or b"{}")
        kid, secret = data.get("kid"), data.get("secret")
        if not kid or not secret:
            return self._json(400, {"error": "kid and secret required"})
        with self.state.lock:
            self.state.keys[kid] = secret
        self._json(200, {"kid": kid, "keys": list(self.state.keys)})

    def _reset(self) -> None:
        data = json.loads(self._read_body() or b"{}")
        st = self.state
        with st.lock:
            st.seen.clear(); st.receipts.clear()
            st.counters.clear(); st.inflight.clear(); st.max_parallel.clear()
            st.duplicates = st.bad_signatures = 0
            if not data.get("keep_rules"):
                st.rules.clear()
        self._json(200, {"ok": True})

    # -- webhook entry -------------------------------------------------

    def _rule_for(self, path: str) -> dict:
        best = "/"
        for prefix, rule in self.state.rules.items():
            if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
                if best not in self.state.rules or len(prefix) > len(best):
                    best = prefix
        return self.state.rules.get(best, {"mode": "ok"})

    def _enter_parallel(self, path: str) -> None:
        st = self.state
        n = st.inflight.get(path, 0) + 1
        st.inflight[path] = n
        st.max_parallel[path] = max(st.max_parallel.get(path, 0), n)

    def _exit_parallel(self, path: str) -> None:
        st = self.state
        st.inflight[path] = max(st.inflight.get(path, 1) - 1, 0)

    def _send_empty(self, code: int, retry_after: float | None = None) -> None:
        self.send_response(code)
        if retry_after is not None:
            self.send_header("Retry-After", str(retry_after))
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _webhook(self, path: str) -> None:
        raw = self._read_body()
        st = self.state
        try:
            env = json.loads(raw)
        except json.JSONDecodeError:
            return self._json(400, {"error": "bad json"})

        with st.lock:
            rule = self._rule_for(path)
            mode = rule["mode"]
            kid = self.headers.get("X-Whub-Key-Id", "")
            counters = st._counters(path)
            counters["received"] += 1

            # 1) 签名校验。badsig 规则用于测试“旧/错签名版本不被接受”：
            #    签名错误时 401 + Retry-After（按临时失败处理，便于观察重试）。
            sig_ok, sig_reason = self._verify(raw, kid)
            if not sig_ok:
                st.bad_signatures += 1
                counters["bad_signature"] += 1
                if mode == "badsig":
                    counters["ratelimited"] += 1
                    return self._send_empty(401, rule["retry_after"])
                return self._json(401, {"error": f"bad signature: {sig_reason}"})
            if mode == "requirekid" and rule.get("require_kid") \
                    and kid != rule["require_kid"]:
                counters["ratelimited"] += 1
                return self._send_empty(401, rule["retry_after"])

            event_id = env.get("event_id")
            delivery_id = env.get("delivery_id")

            # 2) 幂等：同一 (path, event_id) 只确认一次；
            #    重试/重放带着相同 event_id + delivery_id 再来时直接返回首次结果。
            seen = st.seen.setdefault(path, {})
            if event_id in seen:
                st.duplicates += 1
                counters["duplicates"] += 1
                first = seen[event_id]
                body = json.dumps({"deduped": True,
                                   "first_code": first["code"]}).encode()
                self.send_response(first["code"])
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if mode == "down":
                # 临时下线：不占幂等名额，等恢复后同一条还能被正常确认
                counters["unavailable"] += 1
                return self._send_empty(503, rule["retry_after"])
            if mode == "ratelimit":
                counters["ratelimited"] += 1
                return self._send_empty(429, rule["retry_after"])
            if mode == "fail":
                # 永久失败：不占幂等名额（消息未被处理），人工 replay 可重新送达
                counters["permanent"] += 1
                return self._json(400, {"error": "permanent reject (poison)"})
            self._enter_parallel(path)

        # 3) delay 模式：锁外睡眠真实占住连接，制造并行并触发发送方超时
        if mode == "delay":
            time.sleep(rule["delay"])

        with st.lock:
            self._exit_parallel(path)
            counters = st._counters(path)
            counters["accepted"] += 1
            st.seen.setdefault(path, {})[event_id] = {"code": 200}
            # kid 即签名版本标识（key-1/key-2…），用于断言“旧事件旧签名”
            st.receipts.setdefault(path, []).append({
                "event_id": event_id,
                "delivery_id": delivery_id,
                "kid": kid,
                "sig_version": env.get("sig_version", 1),
                "path": path,
                "seq": env.get("seq"),
                "object_key": env.get("object_key"),
                "ts": time.time(),
            })
        self._json(200, {"ok": True, "event_id": event_id})

    def _verify(self, raw: bytes, kid: str) -> tuple[bool, str]:
        st = self.state
        auth = self.headers.get("X-Whub-Signature", "")
        ts_hdr = self.headers.get("X-Whub-Timestamp", "")
        ver_hdr = self.headers.get("X-Whub-Signature-Version", "v1")
        try:
            parts = dict(p.split("=", 1) for p in auth.split(",") if "=" in p)
            ts = parts["t"]
        except (KeyError, ValueError):
            return False, "malformed signature header"
        if ts != ts_hdr:
            return False, "timestamp mismatch"
        if abs(time.time() - int(ts)) > 300:
            return False, "timestamp expired"
        with st.lock:
            secret = st.keys.get(kid)
        if secret is None:
            return False, f"unknown kid {kid!r}"
        delivery_id = self.headers.get("X-Whub-Delivery-Id", "")
        event_id = self.headers.get("X-Whub-Event-Id", "")
        if ver_hdr == "v2" or "v2" in parts:
            # v2 签名文本：ts.event_id.delivery_id.body；十六进制摘要
            try:
                sig = parts["v2"]
            except KeyError:
                return False, "missing v2 tag"
            signing_text = f"{ts}.{event_id}.{delivery_id}."
            mac = hmac.new(secret.encode(),
                           signing_text.encode() + raw, hashlib.sha256)
            expect = mac.hexdigest()
        else:
            try:
                sig = parts["v1"]
            except KeyError:
                return False, "missing v1 tag"
            signing_text = f"{ts}.{delivery_id}."
            mac = hmac.new(secret.encode(),
                           signing_text.encode() + raw, hashlib.sha256)
            expect = base64.b64encode(mac.digest()).decode()
        if not hmac.compare_digest(expect, sig):
            return False, f"{ver_hdr} signature mismatch"
        return True, ver_hdr
