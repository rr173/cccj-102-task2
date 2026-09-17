"""持久化层：SQLite + 跨进程 lane lease / epoch / fence 协议。

仲裁只发生在 SQLite 的即时事务（BEGIN IMMEDIATE）里，绝不使用线程锁、
PID 文件或单进程内存表——多个 OS process 同时打开同一个 WAL 数据库即可参与竞争。

时间：所有期限只使用数据库时钟 ``julianday('now')*86400``（UNIX 秒，亚毫秒），
任何 UPDATE 的可见性/过期判定都在 SQL 一侧完成，worker 的本地时钟不参与仲裁，
因此 stop-the-world 恢复、时钟漂移都无法用旧 fence 续命。

租约协议（每 lane 一行于 ``lane_leases``）：
- 首次 acquire：插入 lane 行，epoch=1；
- expiry steal：WHERE expires_at < db_now 的行被他人改名，epoch+1、fence 重发；
- renew：必须同时匹配 owner_id+lease_epoch+fence_id 且租约未过期，只延长 expires_at；
- handoff/release：fence 条件一致，epoch+1；
- claim/success/retry/dead/release_delivery：所有 worker 侧状态变更都带
  相同的 (owner_id, lease_epoch, fence_id) 三元组条件，任何不匹配 => StaleEpoch。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import secrets
from typing import Any, Optional

# 终态：不再被调度器领取，只能被人工 replay 重新激活。
TERMINAL_STATES = ("succeeded", "dead", "canceled")

# 数据库时钟（UNIX 秒浮点）。julianday 以天为单位，乘 86400。
# 1970-01-01 的儒略日 = 2440587.5。
DB_NOW = "(julianday('now') - 2440587.5) * 86400.0"


class LeaseError(Exception):
    """租约协议错误基类；error_code 即 API 的稳定 JSON 错误码。"""

    error_code = "lease_error"
    http_status = 409

    def __init__(self, message: str = "", *, lane_id: str = "",
                 old_epoch: int | None = None, new_epoch: int | None = None):
        super().__init__(message or self.error_code)
        self.lane_id = lane_id
        self.old_epoch = old_epoch
        self.new_epoch = new_epoch


class StaleEpoch(LeaseError):
    """fence/epoch 不匹配，或租约已过期：写操作被数据库拒绝。"""

    error_code = "stale_epoch"
    http_status = 409


class LeaseNotOwned(LeaseError):
    """调用方不是该 lane 当前 owner。"""

    error_code = "lease_not_owned"
    http_status = 409


class StoreUnavailable(Exception):
    """durable store 拒绝写入（故障注入或磁盘/SQL 致命错误）。"""

    error_code = "store_unavailable"
    http_status = 503


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
    status      TEXT NOT NULL DEFAULT 'active',
    created_at  REAL NOT NULL,
    retired_at  REAL,
    PRIMARY KEY (endpoint_id, version)
);

CREATE TABLE IF NOT EXISTS events (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    endpoint_id    TEXT NOT NULL REFERENCES endpoints(id),
    idempotency_key TEXT,
    object_key     TEXT NOT NULL,
    payload        TEXT NOT NULL,
    created_at     REAL NOT NULL,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id             TEXT PRIMARY KEY,
    endpoint_id    TEXT NOT NULL REFERENCES endpoints(id),
    event_id       TEXT NOT NULL REFERENCES events(id),
    object_key     TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    sig_version    INTEGER NOT NULL,       -- 入队快照，failover 后绝不改写
    target_url     TEXT NOT NULL,          -- 入队快照，failover 后绝不改写
    kid            TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INTEGER NOT NULL DEFAULT 0,
    fail_count     INTEGER NOT NULL DEFAULT 0,
    last_status    INTEGER,
    last_error     TEXT,
    not_before     REAL NOT NULL DEFAULT 0,  -- 端点级退避水位（DB 时钟语义）
    leased_until   REAL,
    -- 领取时拍下的 fence 三元组；owner 被 steal/handoff 后旧持有者全部失配
    fence_owner    TEXT,
    fence_epoch    INTEGER,
    fence_id       TEXT,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL,
    UNIQUE (endpoint_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_deliv_claim
    ON deliveries(endpoint_id, status, object_key, seq);
CREATE INDEX IF NOT EXISTS idx_deliv_event ON deliveries(event_id);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, endpoint_id);

-- 每条 delivery lane（= 端点）一行：持久化协议至少暴露
-- lane_id / owner_id / lease_epoch / expires_at / draining / updated_at。
CREATE TABLE IF NOT EXISTS lane_leases (
    lane_id            TEXT PRIMARY KEY,
    owner_id           TEXT,                       -- NULL = 公共池
    lease_epoch        INTEGER NOT NULL DEFAULT 0,
    fence_id           TEXT NOT NULL DEFAULT '',
    expires_at         REAL NOT NULL DEFAULT 0,
    draining           INTEGER NOT NULL DEFAULT 0,
    drain_deadline     REAL NOT NULL DEFAULT 0,
    last_handoff_reason TEXT NOT NULL DEFAULT 'pool',
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id    TEXT PRIMARY KEY,
    incarnation TEXT NOT NULL,
    pid          INTEGER NOT NULL,
    started_at   REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    draining     INTEGER NOT NULL DEFAULT 0,
    drain_deadline REAL NOT NULL DEFAULT 0,
    stopping     INTEGER NOT NULL DEFAULT 0
);

-- 所有权变更审计链：一次一行，old/new owner+epoch+reason 完整可还原。
CREATE TABLE IF NOT EXISTS lease_audit (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         REAL NOT NULL,
    lane_id    TEXT NOT NULL,
    old_owner  TEXT,
    new_owner  TEXT,
    old_epoch  INTEGER NOT NULL,
    new_epoch  INTEGER NOT NULL,
    reason     TEXT NOT NULL,
    by_worker  TEXT NOT NULL
);

-- rebalance 协调器的 DB 选举租约；以及手动触发请求（取走即删）。
CREATE TABLE IF NOT EXISTS coord_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    leader     TEXT NOT NULL DEFAULT '',
    expires_at REAL NOT NULL DEFAULT 0,
    epoch      INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO coord_state(id, expires_at) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS rebalance_requests (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_at REAL NOT NULL,
    max_moves  INTEGER NOT NULL
);

-- 计数器指标（可按 worker 汇总；lane='' 为全局）。
CREATE TABLE IF NOT EXISTS metrics_counters (
    worker_id TEXT NOT NULL,
    name      TEXT NOT NULL,
    n         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (worker_id, name)
);
"""


def new_fence() -> str:
    return "fnc_" + secrets.token_hex(12)


class Store:
    """薄 SQL 封装。

    跨进程正确性来自 SQLite 即时事务 + 条件 UPDATE（fence 谓词），
    进程内仅用一把锁串行化本连接上的语句（sqlite3 连接不可跨线程并发使用），
    它**不参与**跨进程仲裁。
    """

    def __init__(self, path: str, *, outage_marker: str = "",
                 role: str = "hub"):
        self.path = path
        self.outage_marker = outage_marker
        self.role = role
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=10000;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        cols = {r["name"] for r in self.conn.execute(
            "PRAGMA table_info(deliveries)")}
        with self._lock:
            added = False
            for col, ddl in (("fence_owner", "TEXT"), ("fence_epoch", "INTEGER"),
                             ("fence_id", "TEXT")):
                if col not in cols:
                    self.conn.execute(
                        f"ALTER TABLE deliveries ADD COLUMN {col} {ddl}")
                    added = True
            if added:
                self.conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self.conn.commit()
            except sqlite3.Error:
                pass
            self.conn.close()

    # ---- 故障注入 ----------------------------------------------------

    def _begin(self) -> None:
        """开启即时事务；先清理可能由 SELECT 隐式打开的只读事务。

        Python sqlite3 默认在第一条语句处隐式 BEGIN，故在任何显式
        BEGIN IMMEDIATE 之前先 rollback 到 autocommit 干净点。"""
        with self._lock:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # 本就没有活动事务
            self.conn.execute("BEGIN IMMEDIATE")

    def _outage(self) -> bool:
        return bool(self.outage_marker) and os.path.exists(self.outage_marker)

    def _write(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        """所有写操作的唯一入口：outage 期间拒绝写入（不产生任何出站副作用的前提）。"""
        if self._outage():
            raise StoreUnavailable(
                f"durable store write rejected (outage marker "
                f"{self.outage_marker!r} present)")
        with self._lock:
            try:
                return self.conn.execute(sql, args)
            except sqlite3.OperationalError as e:
                # database is locked / readonly / I/O：与注入故障同样对待
                msg = str(e).lower()
                if any(k in msg for k in
                       ("locked", "readonly", "i/o", "disk", "no such")):
                    raise StoreUnavailable(str(e)) from e
                raise

    def _commit(self) -> None:
        if self._outage():
            raise StoreUnavailable("commit rejected during store outage")
        with self._lock:
            try:
                self.conn.commit()
            except sqlite3.Error as e:
                raise StoreUnavailable(str(e)) from e

    def db_now(self) -> float:
        with self._lock:
            return float(self.conn.execute(
                f"SELECT {DB_NOW} AS t").fetchone()["t"])

    def ping_write(self) -> bool:
        """探测写能力（BEGIN IMMEDIATE 后立即 ROLLBACK，不落任何数据）。

        store outage 期间必然抛 StoreUnavailable；恢复后返回 True。
        worker 只凭这个数据库事实决定是否恢复出站副作用，不看内存状态。"""
        if self._outage():
            raise StoreUnavailable("write probe rejected: outage active")
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute("CREATE TEMP TABLE IF NOT EXISTS _wprobe(x)")
                self.conn.rollback()
                return True
            except sqlite3.Error as e:
                try:
                    self.conn.rollback()
                except sqlite3.Error:
                    pass
                raise StoreUnavailable(str(e)) from e

    # ---- 租户 / 端点 -------------------------------------------------

    def create_tenant(self, tid: str, name: str, api_key: str) -> None:
        self._write(
            "INSERT INTO tenants(id,name,api_key,created_at) "
            f"VALUES(?,?,?,{DB_NOW})", (tid, name, api_key))
        self._commit()

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
        self._write(
            "INSERT INTO endpoints(id,tenant_id,name,active_version,parallelism,"
            f"created_at) VALUES(?,?,?,?,?,{DB_NOW})",
            (eid, tenant_id, name, version, parallelism))
        self._write(
            "INSERT INTO endpoint_versions(endpoint_id,version,kid,secret,url,"
            f"status,created_at) VALUES(?,?,?,?,?,'active',{DB_NOW})",
            (eid, version, kid, secret, url))
        # 新 lane 直接在公共池，等任意 worker 竞争 acquire
        self._write(
            "INSERT INTO lane_leases(lane_id,owner_id,lease_epoch,fence_id,"
            f"expires_at,draining,last_handoff_reason,created_at,updated_at) "
            f"VALUES(?,NULL,0,'',0,0,'pool',{DB_NOW},{DB_NOW})", (eid,))
        self._commit()

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
        self._write("UPDATE endpoints SET parallelism=? WHERE id=?", (n, eid))
        self._commit()

    def set_disabled(self, eid: str, disabled: bool) -> None:
        self._write("UPDATE endpoints SET disabled=? WHERE id=?",
                    (1 if disabled else 0, eid))
        self._commit()

    def version(self, eid: str, version: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, version)).fetchone()

    def versions(self, eid: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? "
                "ORDER BY version", (eid,)))

    def rotate_key(self, eid: str, url: str, secret: Optional[str],
                   kid: Optional[str]) -> sqlite3.Row:
        """轮换签名密钥（可同时迁移 URL）：只提升 active_version 指针。"""
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM endpoints WHERE id=?", (eid,)).fetchone()
            if row is None:
                raise KeyError("endpoint not found")
            nv = row["active_version"] + 1
            secret = secret or ("whsec_" + secrets.token_hex(24))
            kid = kid or f"key-{nv:x}"
            self._write(
                "INSERT INTO endpoint_versions(endpoint_id,version,kid,secret,url,"
                f"status,created_at) VALUES(?,?,?,?,?,'active',{DB_NOW})",
                (eid, nv, kid, secret, url))
            self._write(
                "UPDATE endpoints SET active_version=? WHERE id=?", (nv, eid))
            self._commit()
            return self.conn.execute(
                "SELECT * FROM endpoint_versions WHERE endpoint_id=? AND version=?",
                (eid, nv)).fetchone()

    def retire_version(self, eid: str, version: int) -> dict:
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
            self._write(
                f"UPDATE endpoint_versions SET status='retired', "
                f"retired_at={DB_NOW} WHERE endpoint_id=? AND version=?",
                (eid, version))
            self._commit()
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
        """原子写入事件 + 持有不可变签名快照 (sig_version/target_url/kid) 的 delivery。

        failover 后继任者必须沿用这行的 object_key/seq/not_before/target_url/
        sig_version——它们在任何 lease 状态变更里都不会被改写。"""
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
            seq = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 s FROM deliveries "
                "WHERE endpoint_id=?", (eid,)).fetchone()["s"]
            self._write(
                "INSERT INTO events(id,tenant_id,endpoint_id,idempotency_key,"
                f"object_key,payload,created_at) VALUES(?,?,?,?,?,?,{DB_NOW})",
                (event_id, tenant_id, eid, idem_key, object_key, payload))
            self._write(
                "INSERT INTO deliveries(id,endpoint_id,event_id,object_key,seq,"
                "sig_version,target_url,kid,status,created_at,updated_at) "
                f"VALUES(?,?,?,?,?,?,?,?,'pending',{DB_NOW},{DB_NOW})",
                (delivery_id, eid, event_id, object_key, seq, v["version"],
                 v["url"], v["kid"]))
            self._commit()
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()

    # ---- workers 注册表 ----------------------------------------------

    def register_worker(self, worker_id: str, incarnation: str,
                        pid: int) -> None:
        """成员注册（重启复用同 id 时强制新 incarnation）。

        同名旧 worker 仍占着的 lane 立即按“崩溃/重启”处理：epoch+1 还回公共池，
        旧进程哪怕只是被 SIGSTOP，恢复后续期也必然 stale_epoch。"""
        with self._lock:
            self._begin()
            try:
                self._write(
                    "INSERT INTO workers(worker_id,incarnation,pid,started_at,"
                    "heartbeat_at,draining,stopping) "
                    f"VALUES(?,?,?,{DB_NOW},{DB_NOW},0,0) "
                    "ON CONFLICT(worker_id) DO UPDATE SET "
                    "incarnation=excluded.incarnation, pid=excluded.pid, "
                    f"started_at={DB_NOW}, heartbeat_at={DB_NOW}, "
                    "draining=0, drain_deadline=0, stopping=0",
                    (worker_id, incarnation, pid))
                rows = list(self._write(
                    "SELECT lane_id, lease_epoch FROM lane_leases "
                    "WHERE owner_id=?", (worker_id,)))
                for r in rows:
                    self._bump_lane_to_pool(r["lane_id"], r["lease_epoch"],
                                            reason="worker_restart",
                                            by=worker_id, conn_held=True)
                self._commit()
            except Exception:
                self.conn.rollback()
                raise

    def heartbeat_worker(self, worker_id: str) -> None:
        cur = self._write(
            f"UPDATE workers SET heartbeat_at={DB_NOW} WHERE worker_id=?",
            (worker_id,))
        n = cur.rowcount
        if n == 0:
            self.conn.rollback()
            raise LeaseNotOwned("worker not registered", lane_id=worker_id)
        self._commit()

    def set_worker_drain(self, worker_id: str, draining: bool,
                         deadline: float | None = None) -> sqlite3.Row:
        """置/清 drain 标志：drain 中不再 acquire 新 lane；deadline 到点后，
        手头剩余 lane 由其他活跃 worker 强制接手（更高 epoch）。"""
        with self._lock:
            w = self.conn.execute("SELECT * FROM workers WHERE worker_id=?",
                                  (worker_id,)).fetchone()
            if w is None:
                raise KeyError("worker not found")
            now = self.db_now()
            flag = 1 if draining else 0
            dl_abs = (now + deadline) if draining and deadline is not None else 0.0
            self._write(
                "UPDATE workers SET draining=?, drain_deadline=? WHERE worker_id=?",
                (flag, dl_abs, worker_id))
            self._write(
                "UPDATE lane_leases SET draining=?, drain_deadline=? "
                "WHERE owner_id=?", (flag, dl_abs, worker_id))
            self._commit()
            return self.conn.execute("SELECT * FROM workers WHERE worker_id=?",
                                     (worker_id,)).fetchone()

    def set_worker_stopping(self, worker_id: str) -> None:
        self._write("UPDATE workers SET stopping=1 WHERE worker_id=?",
                    (worker_id,))
        self._commit()

    def active_workers(self, stale_after: float) -> list[sqlite3.Row]:
        """未置 stopping 且心跳未失活的成员。失活阈值由配置注入（仲裁参考）。"""
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM workers WHERE stopping=0 AND heartbeat_at >= "
                f"({DB_NOW} - ?)", (stale_after,)))

    def list_workers(self, stale_after: float) -> list[sqlite3.Row]:
        with self._lock:
            rows = list(self.conn.execute("SELECT * FROM workers"))
            now = self.db_now()
        out = []
        for r in rows:
            d = dict(r)
            d["alive"] = (not r["stopping"]
                          and now - r["heartbeat_at"] <= stale_after)
            d["age_heartbeat"] = now - r["heartbeat_at"]
            cnt = self.owned_counts(r["worker_id"])
            d["owned"] = cnt["owned"]
            d["active"] = cnt["active"]
            out.append(d)
        return out

    def owned_counts(self, worker_id: str) -> dict:
        with self._lock:
            owned = self.conn.execute(
                "SELECT COUNT(*) c FROM lane_leases WHERE owner_id=?",
                (worker_id,)).fetchone()["c"]
            active = self.conn.execute(
                "SELECT COUNT(*) c FROM deliveries d JOIN lane_leases l "
                "ON l.lane_id=d.endpoint_id WHERE l.owner_id=? "
                "AND d.status='inflight'", (worker_id,)).fetchone()["c"]
        return {"owned": owned, "active": active}

    # ---- lane lease 原语（全部即时事务 + 条件 UPDATE）----------------

    def _audit(self, lane_id: str, old_owner: str | None, new_owner: str | None,
               old_epoch: int, new_epoch: int, reason: str, by: str,
               conn_held: bool = False) -> None:
        sql = (f"INSERT INTO lease_audit(at,lane_id,old_owner,new_owner,"
               "old_epoch,new_epoch,reason,by_worker) "
               f"VALUES({DB_NOW},?,?,?,?,?,?,?)")
        args = (lane_id, old_owner, new_owner, old_epoch, new_epoch, reason, by)
        if conn_held:
            self._write(sql, args)
        else:
            self._write(sql, args)
            self._commit()

    def _bump_lane_to_pool(self, lane_id: str, expected_epoch: int, *,
                           reason: str, by: str, conn_held: bool = False,
                           real_old_owner: str | None = None) -> bool:
        """把 lane 交还公共池并 epoch+1（fence 谓词防双重释放）。

        real_old_owner 用于审计的 old_owner；缺省时从当前行读取，绝不把
        执行者 ``by`` 误记为旧 owner。"""
        if real_old_owner is None:
            row = self.conn.execute(
                "SELECT owner_id FROM lane_leases WHERE lane_id=?",
                (lane_id,)).fetchone()
            real_old_owner = row["owner_id"] if row else None
        cur = self._write(
            "UPDATE lane_leases SET owner_id=NULL, fence_id='', "
            f"expires_at=0, draining=0, drain_deadline=0, "
            "lease_epoch=lease_epoch+1, "
            f"last_handoff_reason=?, updated_at={DB_NOW} "
            "WHERE lane_id=? AND lease_epoch=?",
            (reason, lane_id, expected_epoch))
        if cur.rowcount == 0:
            return False
        self._requeue_inflight(lane_id, conn_held=True)
        self._audit(lane_id, real_old_owner, None, expected_epoch,
                    expected_epoch + 1, reason, by, conn_held=True)
        if not conn_held:
            self._commit()
        return True

    def lease_view(self, lane_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM lane_leases WHERE lane_id=?",
                (lane_id,)).fetchone()

    def list_leases(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM lane_leases ORDER BY lane_id"))

    def acquire_from_pool(self, lane_id: str, worker_id: str, *,
                          ttl: float, reason: str = "acquire",
                          expected_owner: str | None = None,
                          expected_epoch: int | None = None) -> sqlite3.Row:
        """从公共池（或指定原 owner 谓词）获取 lane；epoch+1，发放全新 fence。

        首次 acquire（pool 中 epoch=0）与 expiry steal 都必须递增 epoch。"""
        fence = new_fence()
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    raise LeaseNotOwned("lane not found", lane_id=lane_id)
                if row["owner_id"] == worker_id:
                    self.conn.rollback()
                    raise LeaseNotOwned("already owned by self",
                                        lane_id=lane_id,
                                        old_epoch=row["lease_epoch"])
                if expected_owner is not None:
                    # rebalance 直传：old owner + epoch 必须精确匹配
                    if row["owner_id"] != expected_owner or (
                            expected_epoch is not None
                            and row["lease_epoch"] != expected_epoch):
                        self.conn.rollback()
                        raise StaleEpoch(
                            "target lease moved before handoff",
                            lane_id=lane_id, old_epoch=row["lease_epoch"])
                else:
                    # 只能从公共池取（owner IS NULL）
                    if row["owner_id"] is not None:
                        self.conn.rollback()
                        raise LeaseNotOwned(
                            "lane is owned by another worker",
                            lane_id=lane_id,
                            old_epoch=row["lease_epoch"])
                old_owner, old_epoch = row["owner_id"], row["lease_epoch"]
                new_epoch = old_epoch + 1
                self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=?, "
                    f"fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    f"drain_deadline=0, last_handoff_reason=?, updated_at={DB_NOW} "
                    "WHERE lane_id=? AND lease_epoch=? AND owner_id IS ?",
                    (worker_id, new_epoch, fence, ttl, reason, lane_id,
                     old_epoch, old_owner))
                self._audit(lane_id, old_owner, worker_id, old_epoch,
                            new_epoch, reason, worker_id, conn_held=True)
                self._commit()
                return self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
            except Exception:
                self.conn.rollback()
                raise

    def renew_lane(self, lane_id: str, worker_id: str, epoch: int,
                   fence: str, ttl: float) -> sqlite3.Row:
        """续期：owner+epoch+fence 三元组全中 **且租约尚未过期** 才延长。

        暂停超过 TTL 的 worker 恢复后续期：expires_at < db_now 使谓词失配，
        数据库直接拒绝（stale_epoch），旧 fence 无法续命。"""
        cur = self._write(
            "UPDATE lane_leases SET expires_at=" + DB_NOW + "+?, "
            f"updated_at={DB_NOW} "
            "WHERE lane_id=? AND owner_id=? AND lease_epoch=? AND fence_id=? "
            f"AND expires_at >= {DB_NOW}",
            (ttl, lane_id, worker_id, epoch, fence))
        if cur.rowcount == 0:
            self.conn.rollback()
            row = self.lease_view(lane_id)
            raise StaleEpoch(
                "renew rejected: epoch/fence mismatch or lease expired",
                lane_id=lane_id,
                old_epoch=epoch,
                new_epoch=row["lease_epoch"] if row else None)
        self._commit()
        return self.lease_view(lane_id)

    def steal_expired_lane(self, lane_id: str, worker_id: str, *,
                           ttl: float) -> sqlite3.Row | None:
        """TTL 到期后的 expiry steal（epoch+1）。

        drain deadline 的强制接手见 :meth:`steal_due_drain`。
        同时把该 lane 所有 inflight 投递退回 pending（孤儿回收）。"""
        fence = new_fence()
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    return None
                if row["owner_id"] == worker_id:
                    self.conn.rollback()
                    return None
                if row["expires_at"] >= self.db_now():
                    self.conn.rollback()
                    return None
                reason = "expiry_steal"
                old_owner, old_epoch = row["owner_id"], row["lease_epoch"]
                new_epoch = old_epoch + 1
                cur = self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=?, "
                    f"fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    f"drain_deadline=0, last_handoff_reason=?, updated_at={DB_NOW} "
                    "WHERE lane_id=? AND lease_epoch=?",
                    (worker_id, new_epoch, fence, ttl, reason, lane_id,
                     old_epoch))
                if cur.rowcount == 0:
                    self.conn.rollback()
                    raise StaleEpoch("steal raced", lane_id=lane_id,
                                     old_epoch=old_epoch)
                n = self._requeue_inflight(lane_id, conn_held=True)
                self._audit(lane_id, old_owner, worker_id, old_epoch,
                            new_epoch, reason, worker_id, conn_held=True)
                self._commit()
                result = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                # 孤儿回收计数挂在结果行旁边（调用方读取）
                result = dict(result)
                result["requeued"] = n
                return result
            except Exception:
                self.conn.rollback()
                raise

    def steal_due_drain(self, lane_id: str, worker_id: str, *,
                        ttl: float) -> dict | None:
        """drain deadline 到点的强制接手（不要求 lease 过期）。

        单事务直接 old_owner -> new_owner，epoch+1、发新 fence、回退 inflight，
        reason 固定为 drain_handoff。调用方仅对“他人、draining=1、
        drain_deadline 已过”的 lane 使用。"""
        fence = new_fence()
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    return None
                now = self.db_now()
                due = (row["owner_id"] != worker_id and row["draining"]
                       and 0 < row["drain_deadline"] <= now)
                if not due:
                    self.conn.rollback()
                    return None
                old_owner, old_epoch = row["owner_id"], row["lease_epoch"]
                new_epoch = old_epoch + 1
                cur = self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=?, "
                    f"fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    "drain_deadline=0, last_handoff_reason='drain_handoff', "
                    f"updated_at={DB_NOW} WHERE lane_id=? AND lease_epoch=?",
                    (worker_id, new_epoch, fence, ttl, lane_id, old_epoch))
                if cur.rowcount == 0:
                    self.conn.rollback()
                    raise StaleEpoch("drain handoff raced", lane_id=lane_id,
                                     old_epoch=old_epoch)
                n = self._requeue_inflight(lane_id, conn_held=True)
                self._audit(lane_id, old_owner, worker_id, old_epoch,
                            new_epoch, "drain_handoff", worker_id,
                            conn_held=True)
                self._commit()
                result = dict(self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone())
                result["requeued"] = n
                return result
            except Exception:
                self.conn.rollback()
                raise

    def release_lane(self, lane_id: str, worker_id: str, epoch: int,
                     fence: str, *, reason: str = "release") -> bool:
        """主动交还公共池（fence 三元组 + epoch 必须匹配，epoch+1）。"""
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    raise LeaseNotOwned("lane not found", lane_id=lane_id)
                if (row["owner_id"] != worker_id
                        or row["lease_epoch"] != epoch
                        or row["fence_id"] != fence):
                    self.conn.rollback()
                    raise StaleEpoch(
                        "release rejected: lease moved on",
                        lane_id=lane_id, old_epoch=epoch,
                        new_epoch=row["lease_epoch"])
                ok = self._bump_lane_to_pool(
                    lane_id, epoch, reason=reason, by=worker_id,
                    conn_held=True)
                self._commit()
                return ok
            except Exception:
                self.conn.rollback()
                raise

    def handoff_lane(self, lane_id: str, from_worker: str, epoch: int,
                     fence: str, to_worker: str, *, ttl: float,
                     reason: str = "rebalance_handoff") -> sqlite3.Row:
        """合作式交接：fence 三元组匹配后直接换 owner（不经过公共池），epoch+1。

        旧 owner 本地缓存的 fence 立即失效；新 owner 得到全新 fence 后才能写。"""
        fence_new = new_fence()
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    raise LeaseNotOwned("lane not found", lane_id=lane_id)
                if (row["owner_id"] != from_worker
                        or row["lease_epoch"] != epoch
                        or row["fence_id"] != fence):
                    self.conn.rollback()
                    raise StaleEpoch(
                        "handoff rejected: lease moved on",
                        lane_id=lane_id, old_epoch=epoch,
                        new_epoch=row["lease_epoch"])
                tw = self.conn.execute(
                    "SELECT 1 FROM workers WHERE worker_id=? AND stopping=0",
                    (to_worker,)).fetchone()
                if tw is None:
                    self.conn.rollback()
                    raise LeaseNotOwned("target worker unknown or stopping",
                                        lane_id=lane_id)
                new_epoch = epoch + 1
                self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=?, "
                    f"fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    f"drain_deadline=0, last_handoff_reason=?, updated_at={DB_NOW} "
                    "WHERE lane_id=? AND lease_epoch=? AND owner_id=? "
                    "AND fence_id=?",
                    (to_worker, new_epoch, fence_new, ttl, reason, lane_id,
                     epoch, from_worker, fence))
                n = self._requeue_inflight(lane_id, conn_held=True)
                self._audit(lane_id, from_worker, to_worker, epoch,
                            new_epoch, reason, from_worker, conn_held=True)
                self._commit()
                result = dict(self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone())
                result["requeued"] = n
                return result
            except Exception:
                self.conn.rollback()
                raise

    def force_assign_lane(self, lane_id: str, to_worker: str, *, ttl: float,
                          reason: str) -> sqlite3.Row:
        """控制面强制指派（测试/运维用）：无论当前 owner 是谁，epoch+1。"""
        with self._lock:
            self._begin()
            try:
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    raise LeaseNotOwned("lane not found", lane_id=lane_id)
                old_owner, old_epoch = row["owner_id"], row["lease_epoch"]
                fence = new_fence()
                self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=lease_epoch+1,"
                    f" fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    f"drain_deadline=0, last_handoff_reason=?, updated_at={DB_NOW} "
                    "WHERE lane_id=?",
                    (to_worker, fence, ttl, reason, lane_id))
                self._requeue_inflight(lane_id, conn_held=True)
                self._audit(lane_id, old_owner, to_worker, old_epoch,
                            old_epoch + 1, reason, "control-plane",
                            conn_held=True)
                self._commit()
                return self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
            except Exception:
                self.conn.rollback()
                raise

    def _requeue_inflight(self, lane_id: str, *, conn_held: bool = False
                          ) -> int:
        """lane 易主的同一事务内：旧 owner 所有在途投递退回 pending、清空旧 fence。

        seq/not_before/target_url/sig_version 一律不动——继任者沿用原水位与快照。"""
        cur = self._write(
            "UPDATE deliveries SET status='pending', fence_owner=NULL, "
            "fence_epoch=NULL, fence_id='', "
            f"updated_at={DB_NOW} WHERE endpoint_id=? AND status='inflight'",
            (lane_id,))
        return cur.rowcount

    def pool_lanes(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM lane_leases WHERE owner_id IS NULL "
                "ORDER BY lane_id"))

    def owned_lanes(self, worker_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM lane_leases WHERE owner_id=? ORDER BY lane_id",
                (worker_id,)))

    def stealable_lanes(self, *, skip_draining: bool = True,
                        inflight_grace: float = 0.0,
                        idle_grace: float | None = None
                        ) -> list[sqlite3.Row]:
        """可被 expiry steal 的过期 lane（普通成员视角）。

        - 存在在途 inflight（SIGKILL 留下的孤儿）：过期超过 inflight_grace；
        - 纯空闲 lane（owner 崩溃时恰好没在途工作）：过期超过 idle_grace，
          避开健康 owner 在重负载下的续期调度抖动。"""
        idle_grace = inflight_grace if idle_grace is None else idle_grace
        q = ("SELECT l.* FROM lane_leases l WHERE l.owner_id IS NOT NULL "
             "AND ("
             "(EXISTS (SELECT 1 FROM deliveries d WHERE d.endpoint_id=l.lane_id "
             f"AND d.status='inflight') AND l.expires_at + ? < {DB_NOW}) "
             "OR "
             "(NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.endpoint_id=l.lane_id "
             f"AND d.status='inflight') AND l.expires_at + ? < {DB_NOW}))")
        if skip_draining:
            q += " AND l.draining=0"
        q += " ORDER BY l.expires_at LIMIT 50"
        with self._lock:
            return list(self.conn.execute(
                q, (inflight_grace, idle_grace)))

    def orphan_lanes(self, *, include_idle: bool = False,
                     grace: float = 0.0
                     ) -> list[sqlite3.Row]:
        """过期 lane；include_idle 时包含无在途工作的纯空闲 lane。

        grace>0 时要求过期超过 grace 秒（宽限），避免与健康 owner 的
        续期抖动错拍。仅协调器（单 leader）以低频使用：集中回收纯空闲
        过期 lane 到公共池（epoch+1），与普通成员的 steal 互补。"""
        q = ("SELECT * FROM lane_leases WHERE owner_id IS NOT NULL "
             f"AND expires_at + ? < {DB_NOW}")
        if not include_idle:
            q += (" AND EXISTS (SELECT 1 FROM deliveries d "
                  "WHERE d.endpoint_id=lane_leases.lane_id "
                  "AND d.status='inflight')")
        q += " ORDER BY expires_at LIMIT 50"
        with self._lock:
            return list(self.conn.execute(q, (grace,)))

    def drain_due_lanes(self, worker_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM lane_leases WHERE owner_id=? AND draining=1 "
                f"AND drain_deadline>0 AND drain_deadline <= {DB_NOW}",
                (worker_id,)))

    def other_draining_due(self, worker_id: str) -> list[sqlite3.Row]:
        """其他 worker drain 期限已到、仍持有的 lane（允许强制接手）。"""
        with self._lock:
            return list(self.conn.execute(
                "SELECT * FROM lane_leases WHERE owner_id<>? AND draining=1 "
                f"AND drain_deadline>0 AND drain_deadline <= {DB_NOW} "
                "ORDER BY lane_id LIMIT 20", (worker_id,)))

    # ---- 调度器领取 / 结果回写（全部 fence 条件化）-------------------

    def claim_due(self, eid: str, limit: int, worker_id: str,
                  epoch: int, fence: str, now: float | None = None
                  ) -> list[sqlite3.Row]:
        """领取应当现在发送的投递。

        规则保持不变：端点级 not_before 水位 + 每个 object_key 仅队头可领，
        不同 key 并发；新增硬条件——lane lease 的 (owner,epoch,fence) 必须匹配。
        owner 已被 steal 后，旧 owner 的 claim 谓词整体失配，一条都领不走。
        """
        with self._lock:
            lease = self.conn.execute(
                "SELECT * FROM lane_leases WHERE lane_id=? AND owner_id=? "
                "AND lease_epoch=? AND fence_id=?",
                (eid, worker_id, epoch, fence)).fetchone()
            if lease is None:
                raise StaleEpoch(
                    "claim rejected: not the current lease owner/fence",
                    lane_id=eid, old_epoch=epoch)
            if lease["expires_at"] < self.db_now():
                raise StaleEpoch("claim rejected: lease expired",
                                 lane_id=eid, old_epoch=epoch)
            paused = self.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE endpoint_id=? "
                f"AND status IN ('pending','inflight') AND not_before>{DB_NOW}",
                (eid,)).fetchone()["c"]
            if paused:
                return []
            heads = list(self.conn.execute(
                "SELECT object_key, MIN(seq) h FROM deliveries "
                "WHERE endpoint_id=? AND status NOT IN "
                f"('succeeded','canceled') AND not_before <= {DB_NOW} "
                "GROUP BY object_key", (eid,)))
            out: list[sqlite3.Row] = []
            for h in heads:
                if len(out) >= limit:
                    break
                row = self.conn.execute(
                    "SELECT * FROM deliveries WHERE endpoint_id=? AND seq=? "
                    f"AND status='pending' AND not_before<={DB_NOW}",
                    (eid, h["h"])).fetchone()
                if row is None:
                    continue
                cur = self._write(
                    "UPDATE deliveries SET status='inflight', "
                    "fence_owner=?, fence_epoch=?, fence_id=?, "
                    f"leased_until=(SELECT expires_at FROM lane_leases "
                    "WHERE lane_id=?), "
                    f"updated_at={DB_NOW} "
                    "WHERE id=? AND status='pending' AND EXISTS ("
                    "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
                    "AND l.owner_id=? AND l.lease_epoch=? AND l.fence_id=? "
                    f"AND l.expires_at >= {DB_NOW})",
                    (worker_id, epoch, fence, eid, row["id"],
                     worker_id, epoch, fence))
                if cur.rowcount:
                    out.append(self.conn.execute(
                        "SELECT * FROM deliveries WHERE id=?",
                        (row["id"],)).fetchone())
            if out:
                self._commit()
            return out

    def redrive_inflight(self, eid: str, worker_id: str, epoch: int,
                         fence: str) -> list[sqlite3.Row]:
        """lane 刚 acquire/steal 到手：把归属本 lane 的 inflight 孤儿重驱。

        steal/handoff 事务里它们已被退回 pending；这里兜底处理“同 owner 重启”
        等边角，返回当前可立即发送的 pending 行（队头/水位规则由 claim 负责）。"""
        with self._lock:
            lease = self.conn.execute(
                "SELECT 1 FROM lane_leases WHERE lane_id=? AND owner_id=? "
                "AND lease_epoch=? AND fence_id=?",
                (eid, worker_id, epoch, fence)).fetchone()
            if lease is None:
                raise StaleEpoch("redrive rejected: stale fence", lane_id=eid)
            n = self._write(
                "UPDATE deliveries SET status='pending', fence_owner=NULL, "
                "fence_epoch=NULL, fence_id='', "
                f"updated_at={DB_NOW} WHERE endpoint_id=? AND status='inflight'",
                (eid,)).rowcount
            if n:
                self._commit()
            return list(self.conn.execute(
                "SELECT * FROM deliveries WHERE endpoint_id=? AND status='pending'",
                (eid,)))

    def mark_succeeded(self, delivery_id: str, code: int, *,
                       worker_id: str, epoch: int, fence: str) -> None:
        cur = self._write(
            "UPDATE deliveries SET status='succeeded', attempts=attempts+1, "
            "last_status=?, last_error=NULL, not_before=0, leased_until=NULL, "
            f"updated_at={DB_NOW} WHERE id=? AND status='inflight' AND EXISTS ("
            "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
            "AND l.owner_id=? AND l.lease_epoch=? AND l.fence_id=?)",
            (code, delivery_id, worker_id, epoch, fence))
        if cur.rowcount == 0:
            self._raise_fence_stale(delivery_id, epoch, "complete")
        self._commit()

    def mark_retry(self, delivery_id: str, code: Optional[int], error: str,
                   delay_seconds: float, *, worker_id: str, epoch: int,
                   fence: str) -> float:
        """临时失败：推进端点级 not_before 水位（DB 时钟 + delay），保持 inflight。

        延迟参数以秒表示，落库绝对值由数据库计算，不信任调用方时钟。"""
        with self._lock:
            row = self.conn.execute(
                "SELECT endpoint_id FROM deliveries WHERE id=?",
                (delivery_id,)).fetchone()
            cur = self._write(
                "UPDATE deliveries SET attempts=attempts+1, "
                "fail_count=fail_count+1, last_status=?, last_error=?, "
                f"not_before={DB_NOW}+?, "
                f"leased_until={DB_NOW}+?+60, updated_at={DB_NOW} "
                "WHERE id=? AND status='inflight' AND EXISTS ("
                "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
                "AND l.owner_id=? AND l.lease_epoch=? AND l.fence_id=?)",
                (code, error, delay_seconds, delay_seconds, delivery_id,
                 worker_id, epoch, fence))
            if cur.rowcount == 0:
                self._raise_fence_stale(delivery_id, epoch, "retry")
            eid = row["endpoint_id"]
            nb_row = self.conn.execute(
                f"SELECT MAX(not_before) nb FROM deliveries "
                "WHERE id=?", (delivery_id,)).fetchone()
            nb = nb_row["nb"]
            self._write(
                "UPDATE deliveries SET not_before=? WHERE endpoint_id="
                "(SELECT endpoint_id FROM deliveries WHERE id=?) "
                f"AND status IN ('pending','inflight') AND not_before < ?",
                (nb, delivery_id, nb))
            self._commit()
            return nb

    def mark_dead(self, delivery_id: str, code: int, error: str, *,
                  worker_id: str, epoch: int, fence: str) -> None:
        cur = self._write(
            "UPDATE deliveries SET status='dead', attempts=attempts+1, "
            f"last_status=?, last_error=?, leased_until=NULL, updated_at={DB_NOW} "
            "WHERE id=? AND status='inflight' AND EXISTS ("
            "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
            "AND l.owner_id=? AND l.lease_epoch=? AND l.fence_id=?)",
            (code, error, delivery_id, worker_id, epoch, fence))
        if cur.rowcount == 0:
            self._raise_fence_stale(delivery_id, epoch, "dead")
        self._commit()

    def release_delivery(self, delivery_id: str, *, worker_id: str,
                         epoch: int, fence: str) -> None:
        """投递让出本轮（fence 三元组条件化）。"""
        cur = self._write(
            "UPDATE deliveries SET status='pending', leased_until=NULL, "
            "fence_owner=NULL, fence_epoch=NULL, fence_id='', "
            f"updated_at={DB_NOW} WHERE id=? AND status='inflight' AND EXISTS ("
            "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
            "AND l.owner_id=? AND l.lease_epoch=? AND l.fence_id=?)",
            (delivery_id, worker_id, epoch, fence))
        if cur.rowcount == 0:
            self._raise_fence_stale(delivery_id, epoch, "release")
        self._commit()

    def _raise_fence_stale(self, delivery_id: str, epoch: int,
                           op: str) -> None:
        # 条件 UPDATE 失配：先回滚隐式事务，避免污染后续 BEGIN IMMEDIATE
        try:
            self.conn.rollback()
        except sqlite3.Error:
            pass
        with self._lock:
            d = self.conn.execute(
                "SELECT endpoint_id FROM deliveries WHERE id=?",
                (delivery_id,)).fetchone()
            new_epoch = None
            if d:
                l = self.conn.execute(
                    "SELECT lease_epoch FROM lane_leases WHERE lane_id=?",
                    (d["endpoint_id"],)).fetchone()
                if l:
                    new_epoch = l["lease_epoch"]
        raise StaleEpoch(
            f"{op} rejected: current lease epoch/fence does not match",
            lane_id=d["endpoint_id"] if d else "",
            old_epoch=epoch, new_epoch=new_epoch)

    def lane_outstanding(self, lane_id: str) -> int:
        """该 lane 尚未终态（pending 或 inflight）的投递数。

        drain worker 用它判断“手头工作是否已空”——必须直接数行，
        counts() 对缺失状态给默认 0，不能用于这个判断。"""
        with self._lock:
            return self.conn.execute(
                "SELECT COUNT(*) c FROM deliveries WHERE endpoint_id=? "
                "AND status IN ('pending','inflight')",
                (lane_id,)).fetchone()["c"]

    # ---- 控制面 replay / skip（人工操作，不经 worker fence）----------

    def replay(self, delivery_id: str) -> dict:
        now = self.db_now()
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
            self._write(
                "UPDATE deliveries SET status='pending', attempts=0, "
                "fail_count=0, last_error=NULL, not_before=0, "
                "leased_until=NULL, fence_owner=NULL, fence_epoch=NULL, "
                f"fence_id='', updated_at={DB_NOW} "
                "WHERE id=? AND status IN ('dead','canceled')",
                (delivery_id,))
            changed = self.conn.execute("SELECT changes() c").fetchone()["c"]
            self._commit()
            return {"status": "pending", "replayed": bool(changed)}

    def skip(self, delivery_id: str) -> bool:
        self._write(
            "UPDATE deliveries SET status='canceled', leased_until=NULL, "
            f"updated_at={DB_NOW} WHERE id=? AND status IN "
            "('dead','pending','inflight')", (delivery_id,))
        self._commit()
        with self._lock:
            return self.conn.execute(
                "SELECT changes() c").fetchone()["c"] > 0

    # ---- 查询 --------------------------------------------------------

    def delivery(self, delivery_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE id=?",
                (delivery_id,)).fetchone()

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
                f"SELECT MIN(not_before) nb FROM deliveries WHERE endpoint_id=? "
                "AND status IN ('pending','inflight') AND not_before>0",
                (eid,)).fetchone()
            out["next_eligible_at"] = nxt["nb"] if nxt and nxt["nb"] else 0
            return out

    def pending_version_counts(self, eid: str) -> dict:
        with self._lock:
            rows = self.conn.execute(
                "SELECT sig_version v, COUNT(*) c FROM deliveries "
                "WHERE endpoint_id=? AND status IN ('pending','inflight') "
                "GROUP BY sig_version", (eid,)).fetchall()
            return {str(r["v"]): r["c"] for r in rows}

    # ---- rebalance 协调器（DB 选举）----------------------------------

    def coord_acquire(self, worker_id: str, ttl: float) -> bool:
        """尝试成为协调器 leader：空窗期内单条条件 UPDATE 竞选。"""
        cur = self._write(
            "UPDATE coord_state SET leader=?, "
            f"expires_at={DB_NOW}+?, epoch=epoch+1 WHERE id=1 AND "
            f"(leader='' OR leader=? OR expires_at < {DB_NOW})",
            (worker_id, ttl, worker_id))
        won = cur.rowcount > 0
        if won:
            self._commit()
        else:
            self.conn.rollback()
        return won

    def coord_renew(self, worker_id: str, ttl: float) -> bool:
        cur = self._write(
            f"UPDATE coord_state SET expires_at={DB_NOW}+? WHERE id=1 "
            "AND leader=?", (ttl, worker_id))
        ok = cur.rowcount > 0
        if ok:
            self._commit()
        else:
            self.conn.rollback()
        return ok

    def coord_info(self) -> dict:
        with self._lock:
            r = self.conn.execute(
                "SELECT *, CASE WHEN expires_at < " + DB_NOW +
                " THEN 1 ELSE 0 END AS expired FROM coord_state WHERE id=1"
            ).fetchone()
            return dict(r)

    def coord_move_lane(self, lane_id: str, to_worker: str, *, ttl: float,
                        by_worker: str, heartbeat_max_age: float = 1.5
                        ) -> dict:
        """leader 专用的再均衡搬运：带 leader 身份谓词的条件 UPDATE。

        旧 owner 的 fence 三元组立即失效；同事务回退其 inflight 孤儿。
        非 leader 或租约已漂移则 StaleEpoch/LeaseNotOwned，单轮搬运量由
        调用方按 cap 截断。

        heartbeat_max_age：目标 worker 的心跳必须在该秒数内（被 SIGSTOP/
        挂死者无法刷新心跳，不能成为再均衡目标）。"""
        fence = new_fence()
        with self._lock:
            self._begin()
            try:
                info = self.conn.execute(
                    "SELECT * FROM coord_state WHERE id=1").fetchone()
                if info["leader"] != by_worker or info["expires_at"] < self.db_now():
                    self.conn.rollback()
                    raise LeaseNotOwned("not the active coordinator",
                                        lane_id=lane_id)
                row = self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone()
                if row is None:
                    self.conn.rollback()
                    raise LeaseNotOwned("lane not found", lane_id=lane_id)
                if row["owner_id"] == to_worker:
                    self.conn.rollback()
                    raise LeaseNotOwned("already at target", lane_id=lane_id)
                # 目标必须未退出/未 drain，且健康：0 条 lane 的新成员可以；
                # 一旦持有 lane，必须至少一条未过期（证明真的在续期，而不是
                # 被 SIGSTOP 挂死只剩心跳）。
                tw = self.conn.execute(
                    "SELECT 1 FROM workers w WHERE w.worker_id=? "
                    "AND w.stopping=0 AND w.draining=0 "
                    f"AND w.heartbeat_at >= {DB_NOW} - ? AND ("
                    "NOT EXISTS (SELECT 1 FROM lane_leases hl "
                    "WHERE hl.owner_id=w.worker_id) "
                    f"OR EXISTS (SELECT 1 FROM lane_leases hl "
                    f"WHERE hl.owner_id=w.worker_id AND hl.expires_at>={DB_NOW}))",
                    (to_worker, heartbeat_max_age)).fetchone()
                if tw is None:
                    self.conn.rollback()
                    raise LeaseNotOwned(
                        "target worker unknown/draining/stopping/frozen",
                        lane_id=lane_id)
                old_owner, old_epoch = row["owner_id"], row["lease_epoch"]
                self._write(
                    "UPDATE lane_leases SET owner_id=?, lease_epoch=?, "
                    f"fence_id=?, expires_at={DB_NOW}+?, draining=0, "
                    "drain_deadline=0, last_handoff_reason='rebalance_handoff', "
                    f"updated_at={DB_NOW} WHERE lane_id=? AND lease_epoch=?",
                    (to_worker, old_epoch + 1, fence, ttl, lane_id, old_epoch))
                n = self._requeue_inflight(lane_id, conn_held=True)
                self._audit(lane_id, old_owner, to_worker, old_epoch,
                            old_epoch + 1, "rebalance_handoff", by_worker,
                            conn_held=True)
                self._commit()
                result = dict(self.conn.execute(
                    "SELECT * FROM lane_leases WHERE lane_id=?",
                    (lane_id,)).fetchone())
                result["requeued"] = n
                return result
            except Exception:
                self.conn.rollback()
                raise

    def coord_reclaim_idle(self, by_worker: str, *, grace: float,
                           max_n: int = 5) -> list[str]:
        """leader 专用：把过期超过 grace 且**彻底空闲**的 lane 还回公共池。

        “彻底空闲”= 没有任何 pending/inflight 投递（手头工作为零）。一个被
        SIGSTOP/崩溃、但仍有积压的 owner 的 lane 不在这里回收——那是
        expiry_steal 的职责（直传接手并续发积压）。带 leader 谓词。"""
        with self._lock:
            info = self.conn.execute(
                "SELECT * FROM coord_state WHERE id=1").fetchone()
            if info["leader"] != by_worker or info["expires_at"] < self.db_now():
                return []
            lanes = list(self.conn.execute(
                "SELECT lane_id, lease_epoch, owner_id FROM lane_leases "
                "WHERE owner_id IS NOT NULL AND draining=0 "
                f"AND expires_at + ? < {DB_NOW} "
                "AND NOT EXISTS (SELECT 1 FROM deliveries d "
                "WHERE d.endpoint_id=lane_leases.lane_id "
                "AND d.status IN ('pending','inflight')) "
                "ORDER BY expires_at LIMIT ?",
                (grace, max_n)))
            reclaimed = []
            for r in lanes:
                if self._bump_lane_to_pool(r["lane_id"], r["lease_epoch"],
                                          reason="orphan_expiry_reclaim",
                                          by=by_worker,
                                          real_old_owner=r["owner_id"],
                                          conn_held=True):
                    reclaimed.append(r["lane_id"])
            if reclaimed:
                self._commit()
            return reclaimed

    def sweep_orphan_inflight(self) -> int:
        """兜底回收：inflight 的 fence 与当前 lane lease 不一致，或 lease 已过期。

        正常路径里 steal/handoff/restart 已在同事务回退；这里只处理
        “fence 三元组与当前 lease 行不一致”的真正孤儿（例如 force_assign
        或 restart 已 bump owner，但旧 inflight 还挂着旧 fence）。

        注意：**仅 lease 过期、owner 尚未易主**的 inflight 不在此回收——
        那是 expiry_steal 的职责，由 steal 事务在 owner 改名的同一刻回退，
        否则会让“被 SIGSTOP 但仍持有着 lane”的 owner 的工作被提前转成
        pending，进而被误判为空闲 lane 走 acquire 两段路径。"""
        cur = self._write(
            "UPDATE deliveries SET status='pending', fence_owner=NULL, "
            "fence_epoch=NULL, fence_id='', "
            f"updated_at={DB_NOW} WHERE status='inflight' AND ("
            "fence_owner IS NULL OR fence_id='' OR NOT EXISTS ("
            "SELECT 1 FROM lane_leases l WHERE l.lane_id=endpoint_id "
            "AND l.owner_id=fence_owner AND l.lease_epoch=fence_epoch "
            "AND l.fence_id=fence_id))")
        n = cur.rowcount
        if n:
            self._commit()
        return n

    def take_rebalance_request(self) -> Optional[sqlite3.Row]:
        with self._lock:
            r = self.conn.execute(
                "SELECT * FROM rebalance_requests ORDER BY id LIMIT 1"
            ).fetchone()
            if r:
                self._write("DELETE FROM rebalance_requests WHERE id=?",
                            (r["id"],))
                self._commit()
            return r

    def request_rebalance(self, max_moves: int) -> None:
        self._write(
            f"INSERT INTO rebalance_requests(requested_at,max_moves) "
            f"VALUES({DB_NOW},?)", (max_moves,))
        self._commit()

    def list_audit(self, limit: int = 100, lane_id: str | None = None
                   ) -> list[sqlite3.Row]:
        q = "SELECT * FROM lease_audit"
        args: list[Any] = []
        if lane_id:
            q += " WHERE lane_id=?"
            args.append(lane_id)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            return list(self.conn.execute(q, args))

    # ---- 指标 --------------------------------------------------------

    def incr_metric(self, worker_id: str, name: str, n: int = 1) -> None:
        try:
            self._write(
                "INSERT INTO metrics_counters(worker_id,name,n) VALUES(?,?,?) "
                "ON CONFLICT(worker_id,name) DO UPDATE SET n=n+excluded.n",
                (worker_id, name, n))
            self._commit()
        except StoreUnavailable:
            # 指标失败不影响数据面；但 outage 期间调用方本应已停止副作用
            pass

    def metrics_snapshot(self) -> dict:
        with self._lock:
            rows = list(self.conn.execute(
                "SELECT worker_id, name, n FROM metrics_counters "
                "ORDER BY worker_id, name"))
        by_worker: dict[str, dict] = {}
        for r in rows:
            by_worker.setdefault(r["worker_id"], {})[r["name"]] = r["n"]
        return by_worker
