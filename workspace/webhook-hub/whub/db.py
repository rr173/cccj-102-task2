"""持久化层：SQLite 数据库与“每业务对象保序、跨对象并发”的投递队列。

所有状态（端点、密钥版本、事件、投递记录、退避游标）都落库，
进程重启后能从上次状态继续；inflight 的投递依赖租约（lease_until）回收。
"""
from __future__ import annotations

import sqlite3
import threading
import time
import secrets
from typing import Any, Optional

# 终态：不再被调度器领取，只能被人工 replay 重新激活。
TERMINAL_STATES = ("succeeded", "dead", "canceled")

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    api_key     TEXT NOT NULL UNIQUE,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL REFERENCES tenants(id),
    name          TEXT NOT NULL,
    -- 正在使用的密钥版本号；事件入队时把该版本（含 url/secret）快照到 delivery
    active_version INTEGER NOT NULL DEFAULT 0,
    parallelism   INTEGER NOT NULL DEFAULT 1,
    disabled      INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoint_versions (
    endpoint_id TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    version     INTEGER NOT NULL,
    kid         TEXT NOT NULL,
    secret      TEXT NOT NULL,
    url         TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active',   -- active | retired
    created_at  REAL NOT NULL,
    retired_at  REAL,
    PRIMARY KEY (endpoint_id, version)
);

CREATE TABLE IF NOT EXISTS events (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    endpoint_id    TEXT NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT,
    object_key     TEXT NOT NULL,          -- 业务对象标识：同 key 严格保序
    payload        TEXT NOT NULL,
    created_at     REAL NOT NULL,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id             TEXT PRIMARY KEY,
    endpoint_id    TEXT NOT NULL REFERENCES endpoints(id),
    event_id       TEXT NOT NULL REFERENCES events(id),
    object_key     TEXT NOT NULL,
    seq            INTEGER NOT NULL,       -- 端点内入队序号，保序判定依据
    -- 入队时刻不可变的签名快照：密钥轮换后旧排队事件仍按旧版本签名
    sig_version    INTEGER NOT NULL,
    target_url     TEXT NOT NULL,
    kid            TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',  -- pending|inflight|succeeded|dead|canceled
    attempts       INTEGER NOT NULL DEFAULT 0,
    fail_count     INTEGER NOT NULL DEFAULT 0,
    last_status    INTEGER,
    last_error     TEXT,
    -- 端点级独立退避：下一次可被领取的时间（由任何一次临时失败推进）
    not_before     REAL NOT NULL DEFAULT 0,
    leased_until   REAL,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL,
    UNIQUE (endpoint_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_deliv_claim
    ON deliveries(endpoint_id, status, object_key, seq);
CREATE INDEX IF NOT EXISTS idx_deliv_event ON deliveries(event_id);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, endpoint_id);
"""


class Store:
    """薄 SQL 封装。单连接 + 互斥锁（业务量级下足够；换 Postgres 可保持同样接口）。"""

    def __init__(self, path: str):
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.commit()
            self.conn.close()

    # ---- 租户 / 端点 -------------------------------------------------

    def create_tenant(self, tid: str, name: str, api_key: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO tenants(id,name,api_key,created_at) VALUES(?,?,?,?)",
                (tid, name, api_key, time.time()),
            )
            self.conn.commit()

    def tenant_by_key(self, api_key: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM tenants WHERE api_key=?", (api_key,)
            ).fetchone()

    def tenant(self, tenant_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM tenants WHERE id=?", (tenant_id,)
            ).fetchone()

    def create_endpoint(self, eid: str, tenant_id: str, name: str,
                        version: int, kid: str, secret: str, url: str,
                        parallelism: int) -> None:
        now = time.time()
        with self._lock:
            self.conn.execute(
                "INSERT INTO endpoints(id,tenant_id,name,active_version,parallelism,"
                "created_at) VALUES(?,?,?,?,?,?)",
                (eid, tenant_id, name, version, parallelism, now),
            )
            self.conn.execute(
                "INSERT INTO endpoint_versions(endpoint_id,version,kid,secret,url,"
                "status,created_at) VALUES(?,?,?,?,?,'active',?)",
                (eid, version, kid, secret, url, now),
            )
            self.conn.commit()

    def endpoint(self, eid: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM endpoints WHERE id=?", (eid,)
            ).fetchone()

    def endpoints_for_tenant(self, tenant_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM endpoints WHERE tenant_id=? ORDER BY created_at",
                (tenant_id,)))

    def active_endpoint_ids(self) -> list[str]:
        with self._lock:
            return [r["id"] for r in self.conn.execute(
                "SELECT id FROM endpoints WHERE disabled=0")]

    def set_parallelism(self, eid: str, n: int) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE endpoints SET parallelism=? WHERE id=?", (n, eid))
            self.conn.commit()

    def set_disabled(self, eid: str, disabled: bool) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE endpoints SET disabled=? WHERE id=?",
                (1 if disabled else 0, eid))
            self.conn.commit()

    def version(self, eid: str, version: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, version)).fetchone()

    def versions(self, eid: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? ORDER BY version",
                (eid,)))

    def rotate_key(self, eid: str, url: str, secret: Optional[str],
                   kid: Optional[str]) -> sqlite3.Row:
        """轮换签名密钥（可同时迁移 URL）。

        仅提升 active_version 指针；已排队的 delivery 持有旧版本快照不受影响，
        之后入队的事件只走新版本。"""
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM endpoints WHERE id=?", (eid,)).fetchone()
            if row is None:
                raise KeyError("endpoint not found")
            nv = row["active_version"] + 1
            now = time.time()
            secret = secret or ("whsec_" + secrets.token_hex(24))
            kid = kid or f"key-{nv:x}"
            self.conn.execute(
                "INSERT INTO endpoint_versions(endpoint_id,version,kid,secret,url,"
                "status,created_at) VALUES(?,?,?,?,?,'active',?)",
                (eid, nv, kid, secret, url, now))
            self.conn.execute(
                "UPDATE endpoints SET active_version=? WHERE id=?", (nv, eid))
            self.conn.commit()
            return self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, nv)).fetchone()

    def retire_version(self, eid: str, version: int) -> dict:
        """废弃旧密钥版本：只允许在没有 pending/inflight 的旧版本投递时执行。"""
        with self._lock:
            v = self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, version)).fetchone()
            if v is None:
                raise KeyError("version not found")
            inflight = self.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE endpoint_id=? "
                "AND sig_version=? AND status IN ('pending','inflight')",
                (eid, version)).fetchone()["c"]
            if inflight:
                return {"retired": False, "inflight": inflight}
            self.conn.execute(
                "UPDATE endpoint_versions SET status='retired', retired_at=? "
                "WHERE endpoint_id=? AND version=?",
                (time.time(), eid, version))
            self.conn.commit()
            return {"retired": True, "inflight": 0}

    # ---- 事件入队 ----------------------------------------------------

    def find_event(self, tenant_id: str, idem_key: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM events WHERE tenant_id=? AND idempotency_key=?",
                (tenant_id, idem_key)).fetchone()

    def event(self, event_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)).fetchone()

    def enqueue(self, event_id: str, tenant_id: str, eid: str,
                idem_key: Optional[str], object_key: str,
                payload: str, delivery_id: str) -> sqlite3.Row:
        """原子地写入事件 + 生成一条不可变签名快照的 delivery。

        快照在此时读取 active_version / url / secret：
        这就是“切换前排队走旧签名，切换后入队只走新签名”的保证点。"""
        now = time.time()
        with self._lock:
            ep = self.conn.execute(
                "SELECT * FROM endpoints WHERE id=?", (eid,)).fetchone()
            if ep is None:
                raise KeyError("endpoint not found")
            if ep["disabled"]:
                raise RuntimeError("endpoint disabled")
            v = self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, ep["active_version"])).fetchone()
            cur = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 s FROM deliveries WHERE endpoint_id=?",
                (eid,)).fetchone()
            seq = cur["s"]
            self.conn.execute(
                "INSERT INTO events(id,tenant_id,endpoint_id,idempotency_key,"
                "object_key,payload,created_at) VALUES(?,?,?,?,?,?,?)",
                (event_id, tenant_id, eid, idem_key, object_key, payload, now))
            self.conn.execute(
                "INSERT INTO deliveries(id,endpoint_id,event_id,object_key,seq,"
                "sig_version,target_url,kid,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?, 'pending',?,?)",
                (delivery_id, eid, event_id, object_key, seq, v["version"],
                 v["url"], v["kid"], now, now))
            self.conn.commit()
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()

    # ---- 调度器领取 / 结果回写 ---------------------------------------

    def claim_due(self, eid: str, limit: int, now: Optional[float] = None
                  ) -> list[sqlite3.Row]:
        """领取应当现在发送的投递，同时强制“同一 object_key 队头阻塞”。

        规则：
        1. 端点级退避：仍有未完成投递未到 not_before 时，整个端点暂停领取；
        2. 每个 object_key 只有 seq 最小的那条可被领取（前面没发完，后面等着）；
        3. 不同 object_key 互不阻塞，由并行度令牌控制总并发。
        """
        now = now or time.time()
        with self._lock:
            paused = self.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE endpoint_id=? "
                "AND status IN ('pending','inflight') AND not_before>?",
                (eid, now)).fetchone()["c"]
            if paused:
                return []
            # 每个 key 的队头 seq。dead 也纳入：永久失败的事件挡住同对象后续
            # 事件，直到人工 replay 或 skip，保证同一业务对象的先后关系不被跳过。
            heads = list(self.conn.execute(
                "SELECT object_key, MIN(seq) h FROM deliveries "
                "WHERE endpoint_id=? AND status NOT IN ('succeeded','canceled') "
                "GROUP BY object_key", (eid,)))
            out: list[sqlite3.Row] = []
            for h in heads:  # 每个 key 最多贡献队头一条，天然跨 key 公平
                if len(out) >= limit:
                    break
                row = self.conn.execute(
                    "SELECT * FROM deliveries WHERE endpoint_id=? AND seq=? "
                    "AND status='pending' AND not_before<=?",
                    (eid, h["h"], now)).fetchone()
                if row is None:
                    # 队头正在退避/已 dead：本 key 被它阻塞，跳过即可
                    continue
                cur = self.conn.execute(
                    "UPDATE deliveries SET status='inflight', leased_until=?, "
                    "updated_at=? WHERE id=? AND status='pending'",
                    (now + 60, now, row["id"]))
                if cur.rowcount:
                    out.append(self.conn.execute(
                        "SELECT * FROM deliveries WHERE id=?",
                        (row["id"],)).fetchone())
            if out:
                self.conn.commit()
            return out

    def reap_expired_leases(self, started_before: float | None = None) -> int:
        """崩溃恢复：把孤儿 inflight 投递退回 pending。

        - 正常运行：回收租约已过期（leased_until < now）的投递；
        - 进程重启时传入启动时刻 started_before：updated_at 早于该时刻的
          inflight 一定是上一个进程留下的孤儿，立即回收，不必等租约到期
          （退避等待中时租约故意延长到 not_before 之后）。
        重发仍带同一 event_id/delivery_id，接收方按幂等去重即可。"""
        now = time.time()
        with self._lock:
            if started_before is not None:
                cur = self.conn.execute(
                    "UPDATE deliveries SET status='pending', leased_until=NULL, "
                    "updated_at=? WHERE status='inflight' AND updated_at<?",
                    (now, started_before))
            else:
                cur = self.conn.execute(
                    "UPDATE deliveries SET status='pending', leased_until=NULL, "
                    "updated_at=? WHERE status='inflight' AND leased_until<?",
                    (now, now))
            n = cur.rowcount
            self.conn.commit()
            return n

    def mark_succeeded(self, delivery_id: str, code: int) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE deliveries SET status='succeeded', attempts=attempts+1, "
                "last_status=?, last_error=NULL, not_before=0, leased_until=NULL, "
                "updated_at=? WHERE id=? AND status IN ('inflight','pending')",
                (code, time.time(), delivery_id))
            self.conn.commit()

    def mark_retry(self, delivery_id: str, code: Optional[int], error: str,
                   not_before: float) -> None:
        """临时失败：保持在发送路径上（inflight + 退避截止时间）。

        not_before 是端点级退避——claim_due 里该端点任何一条未完成投递尚未到点，
        整个端点暂停领取，从而不冲击下线/限流的接收方，且与其他端点完全隔离。"""
        with self._lock:
            # 租约直接延长到退避结束之后：等待期间不会被 reaper 收回造成双发；
            # 若进程崩溃，退避结束后租约自然过期，投递会被重新领取。
            self.conn.execute(
                "UPDATE deliveries SET attempts=attempts+1, fail_count=fail_count+1,"
                " last_status=?, last_error=?, not_before=?, leased_until=?, "
                "updated_at=? WHERE id=?",
                (code, error, not_before, not_before + 60, time.time(), delivery_id))
            self.conn.execute(
                "UPDATE deliveries SET not_before=? WHERE endpoint_id="
                "(SELECT endpoint_id FROM deliveries WHERE id=?) "
                "AND status IN ('pending','inflight') AND not_before<?",
                (not_before, delivery_id, not_before))
            self.conn.commit()

    def mark_dead(self, delivery_id: str, code: int, error: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE deliveries SET status='dead', attempts=attempts+1, "
                "last_status=?, last_error=?, leased_until=NULL, updated_at=? "
                "WHERE id=?", (code, error, time.time(), delivery_id))
            self.conn.commit()

    def release(self, delivery_id: str) -> None:
        """投递让出当前轮次（等待端点退避结束后重新被领取）。"""
        with self._lock:
            self.conn.execute(
                "UPDATE deliveries SET status='pending', leased_until=NULL, "
                "updated_at=? WHERE id=? AND status='inflight'",
                (time.time(), delivery_id))
            self.conn.commit()

    def replay(self, delivery_id: str) -> dict:
        """人工重放（CAS，防重复确认）。

        - succeeded：幂等 no-op，返回 conflict，绝不产生第二条成功回执；
        - pending/inflight：同样 no-op（已经在投递路径上）；
        - dead/canceled：重置计数并退回 pending，投递仍用原 delivery_id，
          接收方按 event_id 去重，重试/重放天然不会重复确认。"""
        now = time.time()
        with self._lock:
            d = self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if d is None:
                raise KeyError("delivery not found")
            if d["status"] in ("pending", "inflight"):
                return {"status": d["status"], "replayed": False,
                        "reason": "already in flight"}
            if d["status"] == "succeeded":
                return {"status": "succeeded", "replayed": False,
                        "reason": "already acknowledged; replay refused"}
            self.conn.execute(
                "UPDATE deliveries SET status='pending', attempts=0, fail_count=0,"
                " last_error=NULL, not_before=0, leased_until=NULL, updated_at=? "
                "WHERE id=? AND status IN ('dead','canceled')",
                (now, delivery_id))
            changed = self.conn.execute(
                "SELECT changes() c").fetchone()["c"]
            self.conn.commit()
            return {"status": "pending", "replayed": bool(changed)}

    def skip(self, delivery_id: str) -> bool:
        with self._lock:
            self.conn.execute(
                "UPDATE deliveries SET status='canceled', leased_until=NULL, "
                "updated_at=? WHERE id=? AND status IN ('dead','pending','inflight')",
                (time.time(), delivery_id))
            self.conn.commit()
            return self.conn.execute("SELECT changes() c").fetchone()["c"] > 0

    # ---- 查询 --------------------------------------------------------

    def delivery(self, delivery_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()

    def delivery_for_event(self, event_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE event_id=?", (event_id,)
            ).fetchone()

    def delivery_secret(self, eid: str, version: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT secret,kid,url,version FROM endpoint_versions "
                "WHERE endpoint_id=? AND version=?", (eid, version)).fetchone()

    def list_deliveries(self, eid: str, limit: int = 50,
                        status: Optional[str] = None) -> list[sqlite3.Row]:
        q = ("SELECT d.*, e.object_key ev_object FROM deliveries d "
             "JOIN events e ON e.id=d.event_id WHERE d.endpoint_id=?")
        args: list[Any] = [eid]
        if status:
            q += " AND d.status=?"
            args.append(status)
        q += " ORDER BY d.seq DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            return list(self.conn.execute(q, args))

    def counts(self, eid: str) -> dict:
        with self._lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) c FROM deliveries "
                "WHERE endpoint_id=? GROUP BY status", (eid,)).fetchall()
            out = {r["status"]: r["c"] for r in rows}
            nxt = self.conn.execute(
                "SELECT MIN(not_before) nb FROM deliveries WHERE endpoint_id=? "
                "AND status IN ('pending','inflight')", (eid,)).fetchone()
            out["next_eligible_at"] = nxt["nb"] if nxt and nxt["nb"] else 0
            return out

    def pending_version_counts(self, eid: str) -> dict:
        with self._lock:
            rows = self.conn.execute(
                "SELECT sig_version v, COUNT(*) c FROM deliveries "
                "WHERE endpoint_id=? AND status IN ('pending','inflight') "
                "GROUP BY sig_version", (eid,)).fetchall()
            return {str(r["v"]): r["c"] for r in rows}
