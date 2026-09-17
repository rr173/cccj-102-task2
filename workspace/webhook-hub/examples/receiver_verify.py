#!/usr/bin/env python3
"""接收方签名校验参考实现（零三方依赖）。

真正的合作方系统应：
1. 用 X-Whub-Key-Id 找到本地保存的对应版本密钥；
2. 按签名代次重算 HMAC-SHA256 并常量时间比较：
     v1: base64(HMAC("{ts}.{delivery_id}.{raw_body}"))
     v2: hex(HMAC("{ts}.{event_id}.{delivery_id}.{raw_body}"))
   代次由 X-Whub-Signature-Version 或签名头里的 v1=/v2= 标签决定；
3. 校验时间戳新鲜度防重放；
4. 以 X-Whub-Event-Id 作为幂等键，重复投递直接返回首次结果，绝不重复处理。

用法（对接本中枢）：
  python3 examples/receiver_verify.py 8070 key-1 whsec_xxx key-2 whsec_yyy
"""
import base64
import hashlib
import hmac
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_SKEW = 300  # 秒


class Receiver(BaseHTTPRequestHandler):
    keys = {}            # kid -> secret，由命令行注入
    processed = {}       # event_id -> 首次处理结果（幂等表）
    lock = threading.Lock()

    def log_message(self, *a):
        pass

    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))

        # 1) 取密钥版本
        kid = self.headers.get("X-Whub-Key-Id", "")
        secret = self.keys.get(kid)
        if secret is None:
            return self._reply(401, {"error": f"unknown key id {kid!r}"})

        # 2) 校验时间戳
        ts = self.headers.get("X-Whub-Timestamp", "")
        try:
            if abs(time.time() - int(ts)) > MAX_SKEW:
                return self._reply(401, {"error": "stale timestamp"})
        except ValueError:
            return self._reply(401, {"error": "bad timestamp"})

        # 3) 按签名代次重算并常量时间比较
        delivery_id = self.headers.get("X-Whub-Delivery-Id", "")
        event_id = self.headers.get("X-Whub-Event-Id", "")
        ver = self.headers.get("X-Whub-Signature-Version", "v1")
        tags = dict(p.split("=", 1) for p in
                    self.headers.get("X-Whub-Signature", "").split(",")
                    if "=" in p)
        if "v2" in tags or ver == "v2":
            signing_text = f"{ts}.{event_id}.{delivery_id}.".encode() + raw
            expect = hmac.new(secret.encode(), signing_text,
                              hashlib.sha256).hexdigest()
            got = tags.get("v2", "")
        else:
            signing_text = f"{ts}.{delivery_id}.".encode() + raw
            expect = base64.b64encode(
                hmac.new(secret.encode(), signing_text,
                         hashlib.sha256).digest()).decode()
            got = tags.get("v1", "")
        if not hmac.compare_digest(expect, got):
            return self._reply(401, {"error": f"{ver} signature mismatch"})

        env = json.loads(raw)
        event_id = env["event_id"]

        # 4) 幂等：同一事件重复到达（重试/崩溃重发/人工 replay）只处理一次
        with self.lock:
            if event_id in self.processed:
                return self._reply(200, {"deduped": True,
                                         "first": self.processed[event_id]})
            # ... 这里放真正的业务处理 ...
            self.processed[event_id] = {"object_key": env["object_key"],
                                        "at": time.time()}
        self._reply(200, {"ok": True, "event_id": event_id, "kid": kid})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8070
    args = sys.argv[2:]
    Receiver.keys = dict(zip(args[0::2], args[1::2]))
    print(f"receiver on :{port}, keys={list(Receiver.keys)}")
    ThreadingHTTPServer(("0.0.0.0", port), Receiver).serve_forever()
