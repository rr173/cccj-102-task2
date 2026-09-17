"""出站 HTTP 投递：负载信封、HMAC-SHA256 签名、结果分类与指数退避。"""
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

# 2xx：成功（接收方返回 2xx 即视为已确认）。
# 408 / 429 / 5xx：临时失败，退避重试；429、503 优先尊重 Retry-After。
# 其余 4xx：永久失败，转人工（dead），队头阻塞等待 replay/skip，不会无限重试。
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


def build_request(target_url: str, *, delivery_id: str, event_id: str,
                  endpoint_id: str, kid: str, object_key: str,
                  seq: int, payload: str, secret: str,
                  timestamp: float | None = None) -> urllib.request.Request:
    """构造签名请求。

    签名内容固定为 ``{timestamp}.{delivery_id}.{raw_body}``，
    接收方必须用 delivery_id 幂等：同一 delivery_id 重试签名一致；
    人工重放复用同一 delivery_id，不可能产生第二次确认。
    """
    ts = int(timestamp if timestamp is not None else time.time())
    body = json.dumps({
        "event_id": event_id,
        "delivery_id": delivery_id,
        "endpoint_id": endpoint_id,
        "object_key": object_key,
        "seq": seq,
        "payload": json.loads(payload),
        "sent_at": ts,
    }, separators=(",", ":")).encode()
    signing_text = f"{ts}.{delivery_id}." + body.decode()
    sig = sign(secret, signing_text)
    req = urllib.request.Request(target_url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Whub-Event-Id", event_id)
    req.add_header("X-Whub-Delivery-Id", delivery_id)
    req.add_header("X-Whub-Key-Id", kid)
    req.add_header("X-Whub-Timestamp", str(ts))
    req.add_header("X-Whub-Signature", f"t={ts},v1={sig}")
    req.add_header("X-Whub-Signature-Version", "v1")
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
        # DNS 失败、连接拒绝（接收方下线）、读写超时都视为临时失败
        reason = getattr(e, "reason", e)
        return SendResult(False, True, None, str(reason), None)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        v = float(value.strip())
        return max(v, 0.0)
    except ValueError:
        return None


def backoff_delay(attempts: int, *, base: float = 0.5, factor: float = 2.0,
                  cap: float = 30.0, retry_after: float | None = None) -> float:
    """指数退避（满抖动），按端点独立计算，互不影响。

    attempts: 已发生的失败次数。接收方通过 Retry-After 明确给出等待秒数时，
    直接采用其值（限流场景以接收方意志为准），否则用 0~上限 间随机延迟。"""
    if retry_after is not None:
        return max(retry_after, 0.0)
    target = min(cap, base * (factor ** max(attempts - 1, 0)))
    return random.uniform(0, target)
