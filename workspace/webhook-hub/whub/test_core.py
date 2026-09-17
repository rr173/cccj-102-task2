"""不依赖网络/多进程的核心语义单元测试（fence 化持久层）：
  python3 -m unittest whub.test_core -v

跨进程行为（kill -9 / SIGSTOP / store outage）见 whub.acceptance。
"""
import os
import tempfile
import time
import unittest

from whub.db import Store, StaleEpoch, LeaseNotOwned, StoreUnavailable
from whub.sender import backoff_delay, sign, build_request


class HubCoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = Store(os.path.join(self.dir, "t.db"))
        self.db.create_tenant("t1", "T", "key-1")
        self.db.create_endpoint("ep", "t1", "e", 1, "kid-1",
                                "sec-old", "http://old/hook", 2)
        self.db.register_worker("w1", "inc1", 100)
        self.db.register_worker("w2", "inc2", 200)
        lease = self.db.acquire_from_pool("ep", "w1", ttl=60)
        self.epoch, self.fence = lease["lease_epoch"], lease["fence_id"]

    def tearDown(self):
        self.db.close()

    def _enq(self, obj, n):
        return self.db.enqueue(f"evt-{obj}-{n}", "t1", "ep",
                               None, obj, f'{{"n":{n}}}', f"dlv-{obj}-{n}")

    def _claim(self, limit=2):
        l = self.db.renew_lane("ep", "w1", self.epoch, self.fence, 60)
        self.epoch, self.fence = l["lease_epoch"], l["fence_id"]
        return self.db.claim_due("ep", limit, "w1", self.epoch, self.fence)

    def _ok(self, dlv):
        self.db.mark_succeeded(dlv, 200, worker_id="w1",
                               epoch=self.epoch, fence=self.fence)

    def test_head_of_line_per_object(self):
        for n in range(3):
            self._enq("A", n)
        for n in range(2):
            self._enq("B", n)
        rows = self._claim(2)
        self.assertEqual({r["object_key"] for r in rows}, {"A", "B"})
        heads = {r["object_key"]: r["seq"] for r in rows}
        self.assertEqual(heads, {"A": 1, "B": 4})
        self._ok("dlv-B-0")
        rows = self._claim(2)
        self.assertEqual([r["id"] for r in rows], ["dlv-B-1"])

    def test_rotation_snapshot_isolation(self):
        old = self._enq("X", 1)
        self.assertEqual(old["sig_version"], 1)
        self.assertEqual(old["target_url"], "http://old/hook")
        v2 = self.db.rotate_key("ep", "http://new/hook", "sec-new", "kid-2")
        self.assertEqual(v2["version"], 2)
        new = self._enq("X", 2)
        self.assertEqual(new["sig_version"], 2)
        self.assertEqual(new["kid"], "kid-2")
        self.assertEqual(new["target_url"], "http://new/hook")
        self.assertEqual(
            self.db.delivery_secret("ep", 1)["secret"], "sec-old")
        self.assertEqual(
            self.db.delivery_secret("ep", 2)["secret"], "sec-new")
        self.assertFalse(self.db.retire_version("ep", 1)["retired"])
        for d_id in (old["id"], new["id"]):
            rows = self.db.claim_due("ep", 1, "w1", self.epoch, self.fence)
            assert rows, f"claim failed for {d_id}"
            self.db.mark_succeeded(rows[0]["id"], 200, worker_id="w1",
                                   epoch=self.epoch, fence=self.fence)
        self.assertTrue(self.db.retire_version("ep", 1)["retired"])

    def test_endpoint_backoff_blocks_claim(self):
        self._enq("A", 1)
        self._enq("B", 1)
        rows = self._claim(2)
        dlv = rows[0]["id"]
        self.db.mark_retry(dlv, 429, "rate limited", 60,
                           worker_id="w1", epoch=self.epoch, fence=self.fence)
        self.assertEqual(self._claim(4), [])

    def test_replay_cas_no_double_ack(self):
        d = self._enq("A", 1)
        rows = self._claim(1)
        assert rows
        did = rows[0]["id"]
        self.db.mark_dead(did, 400, "poison", worker_id="w1",
                          epoch=self.epoch, fence=self.fence)
        # 人工 replay：dead -> pending
        r1 = self.db.replay(d["id"])
        self.assertTrue(r1["replayed"])
        self.assertEqual(r1["status"], "pending")
        # 已在投递路径上再 replay：no-op
        rows = self.db.claim_due("ep", 1, "w1", self.epoch, self.fence)
        self.assertTrue(rows)
        r2 = self.db.replay(d["id"])
        self.assertFalse(r2["replayed"])
        # 成功后 replay 必须拒绝，不能二次确认
        self._ok(did)
        r3 = self.db.replay(d["id"])
        self.assertFalse(r3["replayed"])
        self.assertEqual(r3["status"], "succeeded")
        self.assertEqual(self.db.delivery(d["id"])["status"], "succeeded")

    def test_stale_fence_rejects_claim_success_retry(self):
        self._enq("A", 1)
        # 错误 fence
        with self.assertRaises(StaleEpoch):
            self.db.claim_due("ep", 1, "w1", self.epoch, "fnc_bogus")
        rows = self._claim(1)
        self.assertEqual(len(rows), 1)
        with self.assertRaises(StaleEpoch):
            self.db.mark_succeeded(rows[0]["id"], 200, worker_id="w1",
                                   epoch=self.epoch + 9, fence="x")
        with self.assertRaises(StaleEpoch):
            self.db.mark_dead(rows[0]["id"], 400, "x", worker_id="w2",
                              epoch=self.epoch, fence=self.fence)
        # 正确 fence 才能成功
        self._ok(rows[0]["id"])
        self.assertEqual(self.db.delivery(rows[0]["id"])["status"],
                         "succeeded")

    def test_epoch_state_machine_acquire_renew_steal_release(self):
        # 该用例需要短 TTL 观察过期：单独 acquire 覆盖 setUp 的长租约
        self.db.release_lane("ep", "w1", self.epoch, self.fence,
                             reason="release")
        lease = self.db.acquire_from_pool("ep", "w1", ttl=0.4)
        self.epoch, self.fence = lease["lease_epoch"], lease["fence_id"]
        # 首次 acquire epoch=1；release+acquire 后此处 epoch=3
        self.assertEqual(self.epoch, 3)
        # 续期只延长，不增 epoch
        l = self.db.renew_lane("ep", "w1", self.epoch, self.fence, 0.4)
        self.assertEqual(l["lease_epoch"], 3)
        # 他人在未过期时偷不走
        self.assertIsNone(
            self.db.steal_expired_lane("ep", "w2", ttl=60))
        # 错误 fence 不能 release
        with self.assertRaises(StaleEpoch):
            self.db.release_lane("ep", "w1", self.epoch, "bad")
        # release bump epoch（3->4），lane 回公共池
        self.assertTrue(self.db.release_lane(
            "ep", "w1", self.epoch, self.fence, reason="release"))
        pool = self.db.pool_lanes()
        self.assertEqual([r["lane_id"] for r in pool], ["ep"])
        self.assertEqual(pool[0]["lease_epoch"], 4)
        # w2 从公共池 acquire -> epoch=5
        l2 = self.db.acquire_from_pool("ep", "w2", ttl=0.4)
        self.assertEqual((l2["owner_id"], l2["lease_epoch"]), ("w2", 5))
        # w1 旧 fence 续期必然失败
        with self.assertRaises(StaleEpoch):
            self.db.renew_lane("ep", "w1", self.epoch, self.fence, 60)
        # TTL 到期后 w2 自己续期也被拒；w1 expiry steal -> epoch=6
        time.sleep(0.6)
        with self.assertRaises(StaleEpoch):
            self.db.renew_lane("ep", "w2", 5, l2["fence_id"], 60)
        l3 = self.db.steal_expired_lane("ep", "w1", ttl=60)
        self.assertEqual((l3["owner_id"], l3["lease_epoch"]), ("w1", 6))

    def test_steal_requeues_inflight_preserving_snapshots(self):
        # 先换成短 TTL 租约（setUp 的租约是 60s，不会过期）
        self.db.release_lane("ep", "w1", self.epoch, self.fence,
                             reason="release")
        l0 = self.db.acquire_from_pool("ep", "w1", ttl=0.4)
        self.epoch, self.fence = l0["lease_epoch"], l0["fence_id"]
        self._enq("A", 1)
        rows = self.db.claim_due("ep", 1, "w1", self.epoch, self.fence)
        self.assertEqual(len(rows), 1)
        dlv = rows[0]
        # 租约过期后 w2 expiry steal
        time.sleep(0.6)
        l2 = self.db.steal_expired_lane("ep", "w2", ttl=60)
        self.assertEqual(l2["owner_id"], "w2")
        d = self.db.delivery(dlv["id"])
        self.assertEqual(d["status"], "pending")
        # 快照水位原样保留
        self.assertEqual(d["sig_version"], dlv["sig_version"])
        self.assertEqual(d["target_url"], dlv["target_url"])
        self.assertEqual(d["seq"], dlv["seq"])
        # w2 用自己的 fence 领取并成功；w1 旧 fence 无法覆盖
        rows2 = self.db.claim_due("ep", 1, "w2", l2["lease_epoch"],
                                  l2["fence_id"])
        self.assertEqual(len(rows2), 1)
        self.db.mark_succeeded(rows2[0]["id"], 200, worker_id="w2",
                               epoch=l2["lease_epoch"],
                               fence=l2["fence_id"])
        with self.assertRaises(StaleEpoch):
            self.db.mark_succeeded(rows2[0]["id"], 200, worker_id="w1",
                                   epoch=self.epoch, fence=self.fence)

    def test_restart_same_workerid_bumps_epoch(self):
        # 同 ID 重新注册（新 incarnation）：其名下 lane 立即回公共池，epoch+1
        self.db.register_worker("w1", "inc1-restarted", 101)
        l = self.db.lease_view("ep")
        self.assertIsNone(l["owner_id"])
        self.assertEqual(l["lease_epoch"], 2)

    def test_store_outage_marker_blocks_writes(self):
        marker = os.path.join(self.dir, "outage")
        s = Store(os.path.join(self.dir, "t2.db"), outage_marker=marker)
        s.create_tenant("t", "T", "k")
        s.create_endpoint("ep", "t", "e", 1, "kid", "sec", "http://x", 1)
        s.register_worker("w", "i", 1)
        with open(marker, "w") as f:
            f.write("x")
        with self.assertRaises(StoreUnavailable):
            s.heartbeat_worker("w")
        with self.assertRaises(StoreUnavailable):
            s.ping_write()
        os.remove(marker)
        self.assertTrue(s.ping_write())
        s.close()

    def test_coordinator_election_mutual_exclusion(self):
        self.assertTrue(self.db.coord_acquire("w1", 5))
        self.assertFalse(self.db.coord_acquire("w2", 5))
        self.assertTrue(self.db.coord_renew("w1", 5))
        # 过期后他人当选
        time.sleep(0.0)
        # 用极短 ttl 制造过期
        self.assertTrue(self.db.coord_acquire("w1", 0.05))
        time.sleep(0.1)
        self.assertTrue(self.db.coord_acquire("w2", 5))

    def test_signature_deterministic_and_versioned(self):
        def hdr(req, name):
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
        self.assertEqual(hdr(req1, "X-Whub-Signature"),
                         hdr(req2, "X-Whub-Signature"))
        req3 = build_request("http://x", delivery_id="d", event_id="e",
                             endpoint_id="ep", kid="kid-2", object_key="o",
                             seq=1, payload='{"a":1}', secret="s2",
                             timestamp=ts)
        self.assertNotEqual(hdr(req1, "X-Whub-Signature"),
                            hdr(req3, "X-Whub-Signature"))
        self.assertEqual(sign("k", "abc"), sign("k", "abc"))

    def test_backoff_respects_retry_after(self):
        self.assertAlmostEqual(backoff_delay(1, retry_after=3.0), 3.0)
        for i in range(8):
            d = backoff_delay(i + 1, base=0.5, cap=30)
            self.assertGreaterEqual(d, 0.0)
            self.assertLessEqual(d, 30.0)


if __name__ == "__main__":
    unittest.main()
