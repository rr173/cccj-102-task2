"""出站 HTTP 投递：负载信封、按代次 HMAC-SHA256 签名、结果分类、指数退避。

签名代次（``sig_version``）与 target_url 一样在入队瞬间快照到 job，
failover / 轮换 / 迁移都不能改变它：v1 批次永远按 v1 签，v2 批次按 v2 签。

* v1：``X-Whub-Signature: t=<ts>,v1=<b64>``，签名串 ``{ts}.{delivery_id}.{body}``
* v2：``X-Whub-Signature: t=<ts>,v2=<hex>``，签名串 ``{ts}.{event_id}.{delivery_id}.{body}``
  并显式带 ``X-Whub-Signature-Version: v2``，密钥推导不同（kid 进入签名文本）。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

RETRY_STATUS = {408, 429, 500, 502, 503, 504}
RESPECT_RETRY_AFTER = {429, 503}


@dataclass
class SendResult:
    ok: bool
    retryable: bool
    code: int | None
    error: str | None
    retry_after: float | None


def sign(secret: str, signing_text: str) -> str:
    mac = hmac.new(secret.encode(), signing_text.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def sign_v2(secret: str, signing_text: str) -> str:
    mac = hmac.new(secret.encode(), signing_text.encode(), hashlib.sha256)
    return mac.hexdigest()


def build_request(target_url: str, *, delivery_id: str, event_id: str,
                  endpoint_id: str, kid: str, object_key: str,
                  seq: int, payload: str, secret: str,
                  sig_version: int = 1,
                  timestamp: float | None = None) -> urllib.request.Request:
    """构造带签名的出站请求；签名代次由入队快照 ``sig_version`` 决定。"""
    ts = int(timestamp if timestamp is not None else time.time())
    body = json.dumps({
        "event_id": event_id,
        "delivery_id": delivery_id,
        "endpoint_id": endpoint_id,
        "object_key": object_key,
        "seq": seq,
        "sig_version": sig_version,
        "payload": json.loads(payload),
        "sent_at": ts,
    }, separators=(",", ":")).encode()

    if sig_version >= 2:
        signing_text = f"{ts}.{event_id}.{delivery_id}." + body.decode()
        sig = sign_v2(secret, signing_text)
        sig_header = f"t={ts},v2={sig}"
        sig_ver = f"v{sig_version}"
    else:
        signing_text = f"{ts}.{delivery_id}." + body.decode()
        sig_header = f"t={ts},v1={sign(secret, signing_text)}"
        sig_ver = "v1"

    req = urllib.request.Request(target_url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Whub-Event-Id", event_id)
    req.add_header("X-Whub-Delivery-Id", delivery_id)
    req.add_header("X-Whub-Key-Id", kid)
    req.add_header("X-Whub-Timestamp", str(ts))
    req.add_header("X-Whub-Signature", sig_header)
    req.add_header("X-Whub-Signature-Version", sig_ver)
    return req


def send(req: urllib.request.Request, timeout: float = 2.5) -> SendResult:
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
            code = resp.getcode()
        return SendResult(200 <= code < 300, False, code, None, None)
    except urllib.error.HTTPError as e:
        e.read()
        retry_after = None
        if e.code in RESPECT_RETRY_AFTER:
            retry_after = _parse_retry_after(e.headers.get("Retry-After"))
        return SendResult(False, e.code in RETRY_STATUS, e.code, None, retry_after)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        reason = getattr(e, "reason", e)
        return SendResult(False, True, None, str(reason), None)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(float(value.strip()), 0.0)
    except ValueError:
        return None


def backoff_delay(attempts: int, *, base: float = 0.5, factor: float = 2.0,
                  cap: float = 30.0, retry_after: float | None = None) -> float:
    if retry_after is not None:
        return max(retry_after, 0.0)
    target = min(cap, base * (factor ** max(attempts - 1, 0)))
    return random.uniform(0, target)
