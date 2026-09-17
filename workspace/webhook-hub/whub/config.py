"""运行配置（全部可用环境变量覆盖）。

时间边界全部可配置；验收程序会把 TTL/轮询显著调短，但生产默认值保持
分钟级的保守节奏，避免网络抖动触发无谓 failover。
"""
from __future__ import annotations

import os
import random
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass
class HubConfig:
    # ---- 进程 / HTTP ----
    host: str = "0.0.0.0"
    port: int = 8080
    db_path: str = "/tmp/whub/data.db"
    http_timeout: float = 2.5          # 单次出站请求超时
    max_workers: int = 128            # 单 worker 出站 HTTP 线程上限
    seed: bool = True                 # 启动时种入演示租户

    # ---- lease / fence 时间边界（单位：秒）----
    # 生产默认：30s TTL、10s 续期（约 TTL/3，留两次重试余量）。
    lease_ttl: float = 30.0
    renew_interval: float = 10.0
    # 续期抖动预算：多个 worker 不会在同一毫秒竞争同一条过期 lane
    renew_jitter: float = 0.5
    # worker 心跳（仅存活登记，不携带所有权）
    heartbeat_interval: float = 5.0
    # 扫描可 acquire / 可 steal lane 的周期
    sweep_interval: float = 1.0
    # 每轮最多新 acquire 多少条 lane（防止启动时所有权一齐翻转）
    acquire_budget: int = 4
    # 渐进 rebalance：单轮最多搬动多少条 lane（cap 约束）
    rebalance_budget: int = 2
    # drain 给手头工作的收尾期限（相对时间，秒）；0 表示只走自然 TTL
    drain_deadline: float = 20.0

    # ---- 退避 ----
    backoff_base: float = 0.5
    backoff_cap: float = 30.0

    # ---- 进程角色 ----
    store_url: str = ""               # 非空 => 连远端 store；空 => 进程内 store
    worker_id: str = ""               # 空 => 单 worker 模式的固定 id
    admin_token: str = ""             # 控制面共享密钥（空则不校验）
    freeze_support: bool = True       # 是否开放测试用 /test/* 注入接口

    @classmethod
    def from_env(cls) -> "HubConfig":
        c = cls()
        c.host = os.environ.get("WHUB_HOST", c.host)
        c.port = _env_int("WHUB_PORT", c.port)
        c.db_path = os.environ.get("WHUB_DB", c.db_path)
        c.http_timeout = _env_float("WHUB_HTTP_TIMEOUT", c.http_timeout)
        c.max_workers = _env_int("WHUB_MAX_WORKERS", c.max_workers)
        c.seed = os.environ.get("WHUB_SEED", "1") not in ("0", "false", "False")
        c.lease_ttl = _env_float("WHUB_LEASE_TTL", c.lease_ttl)
        c.renew_interval = _env_float("WHUB_RENEW_INTERVAL", c.renew_interval)
        c.renew_jitter = _env_float("WHUB_RENEW_JITTER", c.renew_jitter)
        c.heartbeat_interval = _env_float(
            "WHUB_HEARTBEAT_INTERVAL", c.heartbeat_interval)
        c.sweep_interval = _env_float("WHUB_SWEEP_INTERVAL", c.sweep_interval)
        c.acquire_budget = _env_int("WHUB_ACQUIRE_BUDGET", c.acquire_budget)
        c.rebalance_budget = _env_int(
            "WHUB_REBALANCE_BUDGET", c.rebalance_budget)
        c.drain_deadline = _env_float("WHUB_DRAIN_DEADLINE", c.drain_deadline)
        c.backoff_base = _env_float("WHUB_BACKOFF_BASE", c.backoff_base)
        c.backoff_cap = _env_float("WHUB_BACKOFF_CAP", c.backoff_cap)
        c.store_url = os.environ.get("WHUB_STORE_URL", c.store_url)
        c.worker_id = os.environ.get("WHUB_WORKER_ID", c.worker_id)
        c.admin_token = os.environ.get("WHUB_ADMIN_TOKEN", c.admin_token)
        c.freeze_support = os.environ.get(
            "WHUB_FREEZE_SUPPORT", "1") not in ("0", "false", "False")
        return c

    def jitter(self) -> float:
        return random.uniform(0, self.renew_jitter) if self.renew_jitter else 0.0
