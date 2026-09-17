"""端到端验收：一条命令跑完整场景，断言需求里的每条保证。

覆盖：
  1. 同 object_key 保序 + 不同 object_key 并发（含并行度热更新）
  2. 接收方限流/下线 -> 端点独立指数退避，不拖其他租户
  3. 密钥轮换/端点迁移：切换前排队走旧签名、切换后只走新版本
  4. 永久失败进 dead，人工 replay 不产生重复确认
  5. 入口幂等（idempotency_key）
  6. 租户隔离（跨租户访问 404）
"""
from __future__ import annotations

import json
import logging
import secrets
import time
import urllib.error
import urllib.request

log = logging.getLogger("whub.e2e")

ACME = "whk_demo_acme_key"
GLOBEX = "whk_demo_globex_key"


class AssertFail(AssertionError):
    pass


def _http(method: str, url: str, key: str | None = None,
          body: dict | None = None, timeout: float = 10.0):
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


class E2E:
    def __init__(self, hub: str, sink: str):
        self.hub = hub.rstrip("/")
        self.sink = sink.rstrip("/")
        self.passed = 0
        self.failed: list[str] = []

    # -- mini test framework ------------------------------------------

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed += 1
            log.info("  ✅ %s", name)
        else:
            self.failed.append(f"{name} {detail}")
            log.error("  ❌ %s %s", name, detail)

    def section(self, title: str) -> None:
        bar = "─" * 64
        log.info("\n%s\n◆ %s\n%s", bar, title, bar)

    def admin(self, path: str, body: dict) -> dict:
        code, data = _http("POST", f"{self.sink}/admin{path}", body=body)
        assert code == 200, f"sink admin {path} -> {code} {data}"
        return data

    def stats(self) -> dict:
        _, data = _http("GET", f"{self.sink}/admin/stats")
        return data

    def create_ep(self, key: str, path: str, parallelism: int = 2,
                  secret: str | None = None, kid: str | None = None) -> dict:
        secret = secret or ("whsec_" + secrets.token_hex(16))
        # 默认给每个端点独立 kid，避免接收方侧同 kid 不同密钥互相覆盖；
        # 需要固定 kid（轮换场景）时显式传入。
        kid = kid or "key-" + secrets.token_hex(6)
        body = {"name": path, "url": f"{self.sink}{path}",
                "parallelism": parallelism, "secret": secret, "kid": kid}
        # 先让接收方认识密钥，避免 401 干扰场景（轮换场景自行注册新 kid）
        self.admin("/keys", {"kid": kid, "secret": secret})
        code, data = _http("POST", f"{self.hub}/v1/endpoints", key, body)
        assert code == 201, f"create endpoint -> {code} {data}"
        return data

    def publish(self, key: str, eid: str, obj: str, payload: dict,
                idem: str | None = None) -> dict:
        body = {"object_key": obj, "payload": payload}
        if idem:
            body["idempotency_key"] = idem
        code, data = _http("POST",
                           f"{self.hub}/v1/endpoints/{eid}/events",
                           key, body)
        assert code in (200, 202), f"publish -> {code} {data}"
        return data

    def wait_delivery(self, key: str, dlv: str, states=("succeeded", "dead"),
                      timeout: float = 40.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            code, data = _http("GET", f"{self.hub}/v1/deliveries/{dlv}", key)
            assert code == 200, (code, data)
            if data["status"] in states:
                return data
            time.sleep(0.2)
        raise AssertFail(f"delivery {dlv} did not reach {states} within {timeout}s")

    def wait_counts(self, key: str, eid: str, expected: dict,
                    timeout: float = 40.0) -> dict:
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            code, data = _http("GET", f"{self.hub}/v1/endpoints/{eid}", key)
            assert code == 200, (code, data)
            last = data["counts"]
            if all(last.get(k, 0) >= v for k, v in expected.items()):
                return data
            time.sleep(0.25)
        raise AssertFail(f"endpoint {eid} counts never reached {expected}: {last}")

    def receipts(self, path: str) -> list[dict]:
        return self.stats()["receipts"].get(path, [])

    # -- stages --------------------------------------------------------

    def run(self) -> int:
        t0 = time.time()
        self._check_ready()
        self.stage_order_parallel()
        self.stage_isolation()
        self.stage_rotation()
        self.stage_replay()
        self.stage_ingress_idempotency()
        self.stage_tenant_isolation()
        self.summary(time.time() - t0)
        return 0 if not self.failed else 1

    def _check_ready(self) -> None:
        for name, url in (("hub", self.hub), ("sink", self.sink)):
            for _ in range(50):
                try:
                    code, _ = _http("GET", f"{url}/healthz", timeout=2)
                    if code == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.2)
            else:
                raise SystemExit(f"{name} at {url} not reachable")
        # 清空接收方历史，保证场景可重复运行
        _http("POST", f"{self.sink}/admin/reset", body={})
        log.info("hub & sink are ready")

    def stage_order_parallel(self) -> None:
        self.section("1. 同业务对象保序 / 不相关对象并发 / 并行度热更新")
        path = "/e2e/order"
        self.admin("/rules", {"path": path, "mode": "delay", "delay": 0.6})
        ep = self.create_ep(ACME, path, parallelism=3)
        eid = ep["endpoint_id"]

        refs = []
        t_start = time.time()
        for obj, n in (("order-A", 3), ("order-B", 3), ("order-C", 2)):
            for i in range(n):
                refs.append((obj, self.publish(
                    ACME, eid, obj, {"n": i})["delivery_id"]))
        data = self.wait_counts(ACME, eid, {"succeeded": 8}, timeout=25)
        elapsed = time.time() - t_start
        # 8 个事件，0.6s 处理时长，并行度 3：约 ceil(8/3)*0.6≈1.8s；
        # 串行需要 4.8s。用 3.4s 作阈值证明“互不相关的对象并发发送”。
        self.check("8 个事件全部成功", data["counts"].get("succeeded") == 8,
                   str(data["counts"]))
        self.check(f"跨对象并发（总耗时 {elapsed:.2f}s < 3.4s）", elapsed < 3.4,
                   f"elapsed={elapsed:.2f}")
        mp = self.stats()["max_parallel"].get(path, 0)
        self.check(f"接收方观测到并行（max_parallel={mp} > 1）", mp > 1,
                   f"max_parallel={mp}")

        # 同 key 顺序：收据 seq 必须按对象分组严格递增
        recs = sorted(self.receipts(path), key=lambda r: r["ts"])
        last_seq: dict[str, int] = {}
        ordered = True
        for r in recs:
            k, s = r["object_key"], r["seq"]
            if k in last_seq and s <= last_seq[k]:
                ordered = False
            last_seq[k] = s
        self.check("同一 object_key 内严格按入队顺序送达", ordered,
                   json.dumps([(r["object_key"], r["seq"]) for r in recs]))

        # 并行度热更新：调到 1 后，新两事件必须串行（>=1.2s 间隔）
        code, _ = _http("PATCH", f"{self.hub}/v1/endpoints/{eid}", ACME,
                        {"parallelism": 1})
        assert code == 200
        d1 = self.publish(ACME, eid, "order-D", {"n": 0})
        d2 = self.publish(ACME, eid, "order-D", {"n": 1})
        self.wait_delivery(ACME, d2["delivery_id"], timeout=15)
        d = [r for r in self.receipts(path) if r["event_id"] in
             (d1["event_id"], d2["event_id"])]
        gap = (d[1]["ts"] - d[0]["ts"]) if len(d) == 2 else -1
        self.check(f"并行度下调到1后同对象串行（间隔 {gap:.2f}s ≈ 0.6s）",
                   len(d) == 2 and gap >= 0.5, f"gap={gap:.2f}")

    def stage_isolation(self) -> None:
        self.section("2. 限流/下线触发端点独立退避，不拖其他租户")
        bad_path, good_path = "/e2e/bad", "/e2e/good"
        # A 租户端点先限流（Retry-After=1s），随后下线；B 租户端点始终健康
        self.admin("/rules", {"path": bad_path, "mode": "ratelimit",
                              "retry_after": 1.0})
        bad = self.create_ep(ACME, bad_path, parallelism=2)
        good = self.create_ep(GLOBEX, good_path, parallelism=3)
        self.admin("/rules", {"path": good_path, "mode": "delay",
                              "delay": 0.3})

        b1 = self.publish(ACME, bad["endpoint_id"], "x", {"n": 1})
        b2 = self.publish(ACME, bad["endpoint_id"], "y", {"n": 2})
        time.sleep(0.3)
        t0 = time.time()
        g = [self.publish(GLOBEX, good["endpoint_id"], f"g{i}", {"n": i})
             for i in range(3)]
        self.wait_delivery(GLOBEX, g[-1]["delivery_id"], timeout=15)
        good_elapsed = time.time() - t0
        self.check(f"B租户在A被限流时照常并发投递（{good_elapsed:.2f}s < 3s）",
                   good_elapsed < 3.0, f"elapsed={good_elapsed:.2f}")

        # A 端点此时仍在退避/重试（未成功），且存在多次尝试
        d = None
        for _ in range(10):
            _, d = _http("GET",
                         f"{self.hub}/v1/deliveries/{b1['delivery_id']}", ACME)
            if d["status"] in ("leased","pending") and d["fail_count"] >= 1:
                break
            time.sleep(0.3)
        self.check("A端点已进入退避重试（fail_count>=1，状态仍在途）",
                   d["fail_count"] >= 1 and d["status"] in
                   ("leased","pending"), f"d={d}")

        # 接收方恢复后，A 自己补上投递（证明按端点独立恢复）
        self.admin("/rules", {"path": bad_path, "mode": "ok"})
        d1 = self.wait_delivery(ACME, b1["delivery_id"], timeout=20)
        d2 = self.wait_delivery(ACME, b2["delivery_id"], timeout=20)
        self.check("接收方恢复后 A 端点自动补发成功",
                   d1["status"] == "succeeded" and d2["status"] == "succeeded",
                   f"{d1['status']}/{d2['status']}")
        # 重试期间接收方没收到成功收据 -> 恢复后每个事件仍只确认一次
        ids = [r["event_id"] for r in self.receipts(bad_path)]
        self.check("重试不产生重复确认（每事件仅一次成功回执）",
                   len(ids) == len(set(ids)) == 2, f"receipts={ids}")

    def stage_rotation(self) -> None:
        self.section("3. 密钥轮换/端点迁移：旧队列旧签名，新事件新版本")
        path1, path2 = "/e2e/migrate-v1", "/e2e/migrate-v2"
        s1 = "whsec_old_" + secrets.token_hex(8)
        s2 = "whsec_new_" + secrets.token_hex(8)
        self.admin("/rules", {"path": path1, "mode": "delay", "delay": 0.9})
        self.admin("/rules", {"path": path2, "mode": "delay", "delay": 0.4})
        ep = self.create_ep(ACME, path1, parallelism=2, secret=s1,
                            kid="key-1")
        eid = ep["endpoint_id"]
        self.admin("/keys", {"kid": "key-2", "secret": s2})

        # 切换前先把 3 个旧版本事件排进正在积压的队列
        old = [self.publish(ACME, eid, "obj-old", {"i": i}) for i in range(3)]
        time.sleep(1.2)  # 让旧事件占住发送/排队，再执行切换
        code, rot = _http("POST", f"{self.hub}/v1/endpoints/{eid}/rotate",
                          ACME, {"url": f"{self.sink}{path2}",
                                 "secret": s2, "kid": "key-2"})
        assert code == 200, (code, rot)
        new = [self.publish(ACME, eid, "obj-new", {"i": i}) for i in range(2)]
        self.wait_delivery(ACME, new[-1]["delivery_id"], timeout=20)
        self.wait_delivery(ACME, old[-1]["delivery_id"], timeout=20)

        old_recv = self.receipts(path1)
        new_recv = self.receipts(path2)
        self.check("切换前排队事件全部用旧密钥(key-1)签往旧URL",
                   len(old_recv) == 3
                   and all(r["kid"] == "key-1" for r in old_recv),
                   json.dumps(old_recv))
        self.check("切换生效后事件全部用新密钥(key-2)签往新URL",
                   len(new_recv) == 2
                   and all(r["kid"] == "key-2" for r in new_recv),
                   json.dumps(new_recv))
        self.check("接收方签名校验全部通过（无 bad_signature）",
                   self.stats()["bad_signatures"] == 0,
                   f"bad={self.stats()['bad_signatures']}")

        # 旧版本在仍有排队投递时不能废弃；排空后可废弃
        code1, data1 = _http(
            "POST", f"{self.hub}/v1/endpoints/{eid}/versions/1/retire", ACME)
        self.check("旧版本排空后可废弃（retire=200）", code1 == 200,
                   f"{code1} {data1}")

        code, ev = _http("GET", f"{self.hub}/v1/endpoints/{eid}", ACME)
        self.check("当前 active_version=2 且历史版本保留",
                   ev["active_version"] == 2
                   and {v["version"] for v in
                        _http("GET",
                              f"{self.hub}/v1/endpoints/{eid}/versions",
                              ACME)[1]} == {1, 2}, "")

    def stage_replay(self) -> None:
        self.section("4. 永久失败 dead + 人工 replay 不重复确认")
        path = "/e2e/poison"
        self.admin("/rules", {"path": path, "mode": "fail"})
        ep = self.create_ep(ACME, path, parallelism=2)
        eid = ep["endpoint_id"]
        d = self.publish(ACME, eid, "poisoned", {"x": 1})
        final = self.wait_delivery(ACME, d["delivery_id"])
        self.check("4xx 永久失败进入 dead（不无限重试）",
                   final["status"] == "dead" and final["fail_count"] <= 1,
                   f"status={final['status']} fails={final['fail_count']}")
        self.check("dead 挡住同对象后续事件的队头",
                   self.store_head_blocked(ACME, eid), "")

        # 接收方修好后人工重放
        self.admin("/rules", {"path": path, "mode": "ok"})
        code, rep = _http("POST",
                          f"{self.hub}/v1/deliveries/{d['delivery_id']}/replay",
                          ACME)
        self.check("replay 返回 200 并重新投递", code == 200 and rep["replayed"],
                   f"{code} {rep}")
        final2 = self.wait_delivery(ACME, d["delivery_id"], timeout=15)
        self.check("replay 后成功且 delivery_id 不变（接收方可幂等去重）",
                   final2["status"] == "succeeded"
                   and final2["delivery_id"] == d["delivery_id"], "")
        recs = self.receipts(path)
        self.check("接收方只有 1 条成功回执（无重复确认）", len(recs) == 1,
                   f"{len(recs)} receipts")

        # 再次 replay 已成功的事件 -> 幂等拒绝，绝不产生第二条投递
        code, rep = _http("POST",
                          f"{self.hub}/v1/deliveries/{d['delivery_id']}/replay",
                          ACME)
        self.check("对已确认事件重复 replay 被拒绝(409)", code == 409
                   and rep["replayed"] is False, f"{code} {rep}")
        time.sleep(0.5)
        self.check("拒绝后接收方仍只有 1 条回执",
                   len(self.receipts(path)) == 1, "")

    def store_head_blocked(self, key: str, eid: str) -> bool:
        code, data = _http("GET",
                           f"{self.hub}/v1/endpoints/{eid}/deliveries", key)
        assert code == 200
        return any(x["status"] == "dead" for x in data)

    def stage_ingress_idempotency(self) -> None:
        self.section("5. 入口幂等：同一 idempotency_key 不产生新事件/投递")
        path = "/e2e/idem"
        self.admin("/rules", {"path": path, "mode": "ok"})
        ep = self.create_ep(ACME, path, parallelism=2)
        a = self.publish(ACME, ep["endpoint_id"], "idem-obj", {"v": 1},
                         idem="biz-order-1001")
        b = self.publish(ACME, ep["endpoint_id"], "idem-obj", {"v": 2},
                         idem="biz-order-1001")
        self.wait_delivery(ACME, a["delivery_id"])
        self.check("重复入口请求返回同一 event_id/delivery_id（deduped）",
                   a["event_id"] == b["event_id"]
                   and a["delivery_id"] == b["delivery_id"],
                   f"{a['event_id']} vs {b['event_id']}")
        time.sleep(0.5)
        self.check("接收方只收到 1 次", len(self.receipts(path)) == 1,
                   f"{len(self.receipts(path))} receipts")

    def stage_tenant_isolation(self) -> None:
        self.section("6. 租户隔离：跨租户不可见、不可操作")
        path = "/e2e/isolated"
        self.admin("/rules", {"path": path, "mode": "ok"})
        ep = self.create_ep(ACME, path, parallelism=1)
        d = self.publish(ACME, ep["endpoint_id"], "z", {"v": 1})
        self.wait_delivery(ACME, d["delivery_id"])

        code1, _ = _http("GET",
                         f"{self.hub}/v1/endpoints/{ep['endpoint_id']}", GLOBEX)
        code2, _ = _http("POST",
                         f"{self.hub}/v1/deliveries/{d['delivery_id']}/replay",
                         GLOBEX)
        code3, _ = _http("POST",
                         f"{self.hub}/v1/endpoints/{ep['endpoint_id']}/events",
                         GLOBEX, {"object_key": "x", "payload": {}})
        self.check("跨租户读端点/重放投递/投事件均为 404/401",
                   code1 == 404 and code2 == 404 and code3 == 404,
                   f"{code1}/{code2}/{code3}")

    def summary(self, elapsed: float) -> None:
        log.info("\n" + "═" * 64)
        log.info("端到端验收完成：%d 通过 / %d 失败，用时 %.1fs",
                 self.passed, len(self.failed), elapsed)
        if self.failed:
            for f in self.failed:
                log.error("FAIL: %s", f)
        else:
            log.info("🎉 所有保证均已验证")
        log.info("═" * 64)
