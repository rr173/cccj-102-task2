"""不依赖网络的核心语义单元测试：
  python3 -m unittest whub.test_core -v
"""
import os
import tempfile
import time
import unittest

from .db import Store
from .sender import backoff_delay, sign, build_request


class HubCoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = Store(os.path.join(self.dir, "t.db"))
        self.db.create_tenant("t1", "T", "key-1")
        self.db.create_endpoint("ep", "t1", "e", 1, "kid-1",
                                "sec-old", "http://old/hook", 2)

    def tearDown(self):
        self.db.close()

    def _enq(self, obj, n):
        return self.db.enqueue(f"evt-{obj}-{n}", "t1", "ep",
                               None, obj, f'{{"n":{n}}}', f"dlv-{obj}-{n}")

    def test_head_of_line_per_object(self):
        for n in range(3):
            self._enq("A", n)
        for n in range(2):
            self._enq("B", n)

        # 并行度 2：每 key 队头可领，共 2 条，且必为各 key 的最小 seq
        rows = self.db.claim_due("ep", 2)
        self.assertEqual({r["object_key"] for r in rows}, {"A", "B"})
        heads = {r["object_key"]: r["seq"] for r in rows}
        self.assertEqual(heads, {"A": 1, "B": 4})

        # A1 在途时，A 的后续不可领；B1 成功后 B2 立即可领
        self.db.mark_succeeded("dlv-B-0", 200)
        rows = self.db.claim_due("ep", 2)
        self.assertEqual([r["id"] for r in rows], ["dlv-B-1"])

    def test_rotation_snapshot_isolation(self):
        # 切换前入队：固定旧版本/旧 URL
        old = self._enq("X", 1)
        self.assertEqual(old["sig_version"], 1)
        self.assertEqual(old["target_url"], "http://old/hook")
        v2 = self.db.rotate_key("ep", "http://new/hook", "sec-new", "kid-2")
        self.assertEqual(v2["version"], 2)
        # 切换后入队：只走新版本
        new = self._enq("X", 2)
        self.assertEqual(new["sig_version"], 2)
        self.assertEqual(new["kid"], "kid-2")
        self.assertEqual(new["target_url"], "http://new/hook")
        # 旧 delivery 的签名密钥仍可取到旧值，且互不相同
        old_secret = self.db.delivery_secret("ep", 1)["secret"]
        new_secret = self.db.delivery_secret("ep", 2)["secret"]
        self.assertEqual(old_secret, "sec-old")
        self.assertEqual(new_secret, "sec-new")
        # 旧版本仍有排队投递时不能废弃
        self.assertFalse(self.db.retire_version("ep", 1)["retired"])
        self.db.mark_succeeded(old["id"], 200)
        self.db.mark_succeeded(new["id"], 200)
        self.assertTrue(self.db.retire_version("ep", 1)["retired"])

    def test_endpoint_backoff_blocks_claim(self):
        self._enq("A", 1)
        self._enq("B", 1)
        self.db.mark_retry("dlv-A-1", 429, "rate limited",
                           time.time() + 60)
        # 端点级退避：即使 B 从未失败，也不会在退避期间被领取
        self.assertEqual(self.db.claim_due("ep", 4), [])

    def test_replay_cas_no_double_ack(self):
        d = self._enq("A", 1)
        self.db.mark_dead(d["id"], 400, "poison")
        # 成功重放只能发生一次
        r1 = self.db.replay(d["id"])
        self.assertTrue(r1["replayed"])
        # 已在途时再 replay 是 no-op
        self.db.claim_due("ep", 1)
        r2 = self.db.replay(d["id"])
        self.assertFalse(r2["replayed"])
        # 成功后 replay 必须被拒绝，不能产生新投递
        self.db.mark_succeeded(d["id"], 200)
        r3 = self.db.replay(d["id"])
        self.assertFalse(r3["replayed"])
        self.assertEqual(r3["status"], "succeeded")
        self.assertEqual(self.db.delivery(d["id"])["status"], "succeeded")

    def test_lease_recovery_on_restart(self):
        self._enq("A", 1)
        rows = self.db.claim_due("ep", 1)
        self.assertEqual(len(rows), 1)
        # 模拟旧进程在租约未到期时崩溃、新进程立即启动（水位线恢复）
        n = self.db.reap_expired_leases(started_before=time.time())
        self.assertEqual(n, 1)
        self.assertEqual(self.db.delivery("dlv-A-1")["status"], "pending")

    def test_signature_deterministic_and_versioned(self):
        def hdr(req, name):
            # urllib 对自定义头的大小写规范化与原始写法不同，做不敏感查找
            low = name.lower()
            for k, v in req.headers.items():
                if k.lower() == low:
                    return v
            return None

        req1 = build_request("http://x", delivery_id="d", event_id="e",
                             endpoint_id="ep", kid="kid-1", object_key="o",
                             seq=1, payload='{"a":1}', secret="s1")
        ts = int(hdr(req1, "X-Whub-Timestamp"))
        req2 = build_request("http://x", delivery_id="d", event_id="e",
                             endpoint_id="ep", kid="kid-1", object_key="o",
                             seq=1, payload='{"a":1}', secret="s1",
                             timestamp=ts)
        # 同 delivery_id/同时间戳/同负载 -> 签名一致（重试可被接收方幂等识别）
        self.assertEqual(hdr(req1, "X-Whub-Signature"),
                         hdr(req2, "X-Whub-Signature"))
        # 不同密钥 -> 签名不同
        req3 = build_request("http://x", delivery_id="d", event_id="e",
                             endpoint_id="ep", kid="kid-2", object_key="o",
                             seq=1, payload='{"a":1}', secret="s2",
                             timestamp=ts)
        self.assertNotEqual(hdr(req1, "X-Whub-Signature"),
                            hdr(req3, "X-Whub-Signature"))
        self.assertEqual(sign("k", "abc"), sign("k", "abc"))

    def test_backoff_respects_retry_after(self):
        self.assertAlmostEqual(
            backoff_delay(1, retry_after=3.0), 3.0)
        for i in range(8):
            d = backoff_delay(i + 1, base=0.5, cap=30)
            self.assertGreaterEqual(d, 0.0)
            self.assertLessEqual(d, 30.0)


if __name__ == "__main__":
    unittest.main()
