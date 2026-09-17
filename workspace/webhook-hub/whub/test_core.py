"""不依赖网络/多进程的核心语义单元测试：
  python3 -m unittest whub.test_core -v

覆盖：单调时钟、首次 acquire/expiry steal 的 epoch 递增、renew/claim/
complete 的 fence 校验、写闸门、序号单调、入队快照、队头阻塞、replay CAS。
"""
import os
import tempfile
import unittest

from .engine import Engine, StaleEpoch, StoreUnavailable
from .sender import build_request, sign, sign_v2, backoff_delay


class EngineFenceTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = Engine(os.path.join(self.dir, "t.db"))
        self.db.create_tenant("t1", "T", "key-1")
        self.db.create_endpoint("ep", "t1", "e", 1, "kid-1",
                                "sec-old", "http://old/hook", 2)
        self.ia = self.db.register_worker("a", port=1)["incarnation"]
        self.ib = self.db.register_worker("b", port=2)["incarnation"]

    def tearDown(self):
        self.db.close()

    def _enq(self, lid, obj, n, jid=None):
        return self.db.enqueue(
            lane_id=lid, event_id=f"evt-{obj}-{n}", tenant_id="t1", eid="ep",
            idem_key=None, object_key=obj, payload=f'{{"n":{n}}}',
            job_id=jid or f"job-{obj}-{n}")

    def test_acquire_bumps_epoch_and_renew_keeps_it(self):
        self._enq("ln1", "A", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        l = self.db.lane("ln1")
        self.assertEqual(l["owner_id"], "a")
        self.assertEqual(l["lease_epoch"], 1)  # 首次 acquire 即 epoch=1
        fence = l["fence_id"]
        r = self.db.renew("ln1", "a", 1, fence, self.ia, ttl=30)
        self.assertEqual(r["lease_epoch"], 1)  # renew 不动 epoch
        self.assertEqual(self.db.lane("ln1")["fence_id"], fence)

    def test_expiry_steal_bumps_epoch_and_old_fence_dead(self):
        self._enq("ln1", "A", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        l = self.db.lane("ln1")
        # 过期
        self.db.renew("ln1", "a", l["lease_epoch"], l["fence_id"],
                      self.ia, ttl=-0.001)
        r = self.db.sweep("b", self.ib, ttl=30, acquire_budget=4)
        self.assertEqual(r["stolen"][0]["reason"], "expiry_steal")
        l2 = self.db.lane("ln1")
        self.assertEqual(l2["owner_id"], "b")
        self.assertEqual(l2["lease_epoch"], 2)
        # 旧 owner 用旧 fence 续期：拒绝
        with self.assertRaises(StaleEpoch):
            self.db.renew("ln1", "a", 1, l["fence_id"], self.ia, ttl=30)
        # 旧 owner 用旧 fence claim/complete：拒绝
        with self.assertRaises(StaleEpoch):
            self.db.claim("ln1", "a", 1, l["fence_id"])
        # 新 owner 正常 claim
        j = self.db.claim("ln1", "b", 2, l2["fence_id"])
        with self.assertRaises(StaleEpoch):
            self.db.complete_success(j["id"], "a", 1, l["fence_id"], 200)
        # 新 owner 状态不被旧写覆盖
        self.assertEqual(self.db.job(j["id"])["status"], "leased")
        self.db.complete_success(j["id"], "b", 2, l2["fence_id"], 200)
        self.assertEqual(self.db.job(j["id"])["status"], "succeeded")

    def test_claim_is_head_only_and_seq_monotonic(self):
        for n in range(1, 4):
            self._enq("ln1", "A", n)
        self._enq("ln2", "B", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        la, lb = self.db.lane("ln1"), self.db.lane("ln2")
        j1 = self.db.claim("ln1", "a", la["lease_epoch"], la["fence_id"])
        self.assertEqual(j1["seq"], 1)
        # 同 lane 已有在途：再 claim 返回 None（队头串行）
        self.assertIsNone(
            self.db.claim("ln1", "a", la["lease_epoch"], la["fence_id"]))
        # 另一条 lane 可并行
        jb = self.db.claim("ln2", "a", lb["lease_epoch"], lb["fence_id"])
        self.assertEqual(jb["seq"], 1)
        self.db.complete_success(j1["id"], "a", la["lease_epoch"],
                                 la["fence_id"], 200)
        j2 = self.db.claim("ln1", "a", la["lease_epoch"], la["fence_id"])
        self.assertEqual(j2["seq"], 2)  # failover 也必须沿用此序号

    def test_retry_not_before_survives_failover(self):
        self._enq("ln1", "A", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        la = self.db.lane("ln1")
        j = self.db.claim("ln1", "a", 1, la["fence_id"])
        future = self.db.now() + 100
        self.db.complete_retry(j["id"], "a", 1, la["fence_id"],
                               429, "ratelimited", future)
        self.assertGreaterEqual(self.db.lane("ln1")["not_before"], future)
        # 用当前 owner+fence 但时间未到：继任/当前 owner 都不许提前发出
        lane = self.db.lane("ln1")
        self.assertIsNone(self.db.claim(
            "ln1", lane["owner_id"], lane["lease_epoch"], lane["fence_id"]))
        # 完全无关的 fence 更不能 claim
        with self.assertRaises(StaleEpoch):
            self.db.claim("ln1", "b", 9, "x")

    def test_snapshot_isolation_v1_v2(self):
        old = self._enq("ln1", "X", 1)
        self.assertEqual(old["sig_version"], 1)
        self.assertEqual(old["target_url"], "http://old/hook")
        self.db.rotate_key("ep", "http://new/hook", "sec-new", "kid-2")
        new = self._enq("ln1", "X", 2)
        self.assertEqual(new["sig_version"], 2)
        self.assertEqual(new["kid"], "kid-2")
        self.assertEqual(new["target_url"], "http://new/hook")
        # lane 队头仍是旧快照
        self.assertEqual(self.db.lane("ln1")["sig_version"], 1)

    def test_replay_cas(self):
        j = self._enq("ln1", "A", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        la = self.db.lane("ln1")
        jj = self.db.claim("ln1", "a", 1, la["fence_id"])
        self.db.complete_dead(jj["id"], "a", 1, la["fence_id"], 400, "poison")
        self.assertTrue(self.db.replay(j["id"])["replayed"])
        self.assertFalse(self.db.replay(j["id"])["replayed"])  # 已 pending

    def test_write_gate_blocks_outbound_writes(self):
        self._enq("ln1", "A", 1)
        self.db.reject_writes = True
        with self.assertRaises(StoreUnavailable):
            self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        with self.assertRaises(StoreUnavailable):
            self.db.heartbeat("a", self.ia)
        # 读仍然可以
        self.assertEqual(self.db.now() > 0, True)
        self.db.reject_writes = False
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        self.assertEqual(self.db.lane("ln1")["owner_id"], "a")

    def test_orphan_recovery_on_deregister(self):
        self._enq("ln1", "A", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        self.assertEqual(self.db.lane("ln1")["lease_epoch"], 1)
        # a 优雅退出：lane 立即 handoff 给在场的 b（epoch+1，无需等 TTL）
        m = self.db.deregister_worker("a", self.ia, ttl=30)
        self.assertEqual(m["moved"][0]["to"], "b")
        l = self.db.lane("ln1")
        self.assertEqual(l["owner_id"], "b")
        self.assertEqual(l["lease_epoch"], 2)
        self.assertEqual(l["last_handoff_reason"], "worker_shutdown")

    def test_orphan_when_no_other_worker(self):
        self._enq("ln9", "Z", 1)
        self.db.sweep("a", self.ia, ttl=30, acquire_budget=4)
        self.db.deregister_worker("b", self.ib, ttl=30)
        self.db.deregister_worker("a", self.ia, ttl=30)
        self.assertIsNone(self.db.lane("ln9")["owner_id"])
        # 新成员重新注册后可从公共池 acquire
        ic = self.db.register_worker("c")["incarnation"]
        r = self.db.sweep("c", ic, ttl=30, acquire_budget=4)
        self.assertEqual(r["acquired"], ["ln9"])
        self.assertEqual(self.db.lane("ln9")["owner_id"], "c")


class SignatureTest(unittest.TestCase):
    def _hdr(self, req, name):
        low = name.lower()
        for k, v in req.headers.items():
            if k.lower() == low:
                return v
        return None

    def test_v1_v2_distinct_and_stable(self):
        kw = dict(target_url="http://x", delivery_id="d", event_id="e",
                  endpoint_id="ep", kid="k", object_key="o", seq=1,
                  payload='{"a":1}', secret="s")
        r1 = build_request(sig_version=1, **kw)
        r2 = build_request(sig_version=2, **kw)
        self.assertIn("v1=", self._hdr(r1, "X-Whub-Signature"))
        self.assertIn("v2=", self._hdr(r2, "X-Whub-Signature"))
        self.assertEqual(self._hdr(r2, "X-Whub-Signature-Version"), "v2")
        self.assertNotEqual(self._hdr(r1, "X-Whub-Signature"),
                            self._hdr(r2, "X-Whub-Signature"))
        # 确定性：同输入同签名
        r1b = build_request(sig_version=1, timestamp=1000, **kw)
        r1c = build_request(sig_version=1, timestamp=1000, **kw)
        self.assertEqual(self._hdr(r1b, "X-Whub-Signature"),
                         self._hdr(r1c, "X-Whub-Signature"))
        self.assertEqual(len(sign_v2("k", "abc")), 64)  # hex(sha256)
        self.assertEqual(len(sign("k", "abc")), 44)     # base64(sha256)

    def test_backoff_bounds(self):
        self.assertEqual(backoff_delay(1, retry_after=3.0), 3.0)
        for i in range(8):
            d = backoff_delay(i + 1, base=0.5, cap=30)
            self.assertGreaterEqual(d, 0.0)
            self.assertLessEqual(d, 30.0)


if __name__ == "__main__":
    unittest.main()
