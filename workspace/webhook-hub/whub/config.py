"""运行配置（全部可用环境变量覆盖）。

时间参数分两层：
- 生产默认：lease_ttl=30s，约每 ttl/3 续期一次，jitter ±20%；
- 验收可通过环境变量把 TTL 缩到秒级，但时间语义不变。
所有租约期限只认数据库时钟（julianday('now')），从不信任 worker 本地墙上时钟。
"""
from __future__ import annotations

import os
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
    host: str = "0.0.0.0"
    port: int = 8080
    db_path: str = "/tmp/whub/data.db"
    http_timeout: float = 2.5          # 单次出站请求超时
    poll_interval: float = 0.25        # Runner 空闲轮询
    discover_interval: float = 1.0     # （兼容保留）
    backoff_base: float = 0.5          # 指数退避基数（秒）
    backoff_cap: float = 30.0          # 指数退避上限（秒）
    max_workers: int = 256             # 全局出站 HTTP 工作线程上限
    seed: bool = True                  # 启动时种入演示租户

    # ---- HA / lease 协议参数（均可配置）----
    role: str = "hub"                  # hub(单进程默认) | store | worker
    worker_id: str = ""                # worker 身份；空则 worker-<pid>
    api_port: int = 0                  # worker 进程自带 API 的端口（0=随机）
    port_file: str = ""                # 启动后把实际监听端口写入该文件
    lease_ttl: float = 30.0            # lane lease 寿命（秒）
    renew_interval: float = 0.0        # 续期周期；0 => 自动取 ttl/3
    renew_jitter: float = 0.2          # 续期抖动比例（±20%），避免同拍
    lease_tick_interval: float = 0.5   # lease 管理循环节拍（发现/steal/reconcile）
    coord_interval: float = 1.0        # rebalance 协调器轮询/租期
    rebalance_max_moves: int = 1       # 单轮最多搬运 lane 数（渐进再均衡）
    drain_deadline: float = 30.0       # drain 默认收尾期限（秒）
    worker_stale_after: float = 0.0    # worker 心跳失活阈值；0 => 2*ttl
    outage_marker: str = ""            # 存在该文件即模拟 durable store 拒绝写入
    debug_admin: bool = False          # 开放 /admin/debug/* 陈旧写注入接口

    @classmethod
    def from_env(cls) -> "HubConfig":
        c = cls()
        c.host = os.environ.get("WHUB_HOST", c.host)
        c.port = _env_int("WHUB_PORT", c.port)
        c.db_path = os.environ.get("WHUB_DB", c.db_path)
        c.http_timeout = _env_float("WHUB_HTTP_TIMEOUT", c.http_timeout)
        c.poll_interval = _env_float("WHUB_POLL_INTERVAL", c.poll_interval)
        c.discover_interval = _env_float("WHUB_DISCOVER_INTERVAL",
                                         c.discover_interval)
        c.backoff_base = _env_float("WHUB_BACKOFF_BASE", c.backoff_base)
        c.backoff_cap = _env_float("WHUB_BACKOFF_CAP", c.backoff_cap)
        c.max_workers = _env_int("WHUB_MAX_WORKERS", c.max_workers)
        c.seed = os.environ.get("WHUB_SEED", "1") not in ("0", "false", "False")

        c.role = os.environ.get("WHUB_ROLE", c.role)
        c.worker_id = os.environ.get("WHUB_WORKER_ID", c.worker_id)
        c.api_port = _env_int("WHUB_API_PORT", c.port)
        c.port_file = os.environ.get("WHUB_PORT_FILE", c.port_file)
        c.lease_ttl = _env_float("WHUB_LEASE_TTL", c.lease_ttl)
        c.renew_interval = _env_float("WHUB_RENEW_INTERVAL",
                                      c.renew_interval)
        c.lease_tick_interval = _env_float("WHUB_LEASE_TICK_INTERVAL",
                                           c.lease_tick_interval)
        c.renew_jitter = _env_float("WHUB_RENEW_JITTER", c.renew_jitter)
        c.coord_interval = _env_float("WHUB_COORD_INTERVAL",
                                      c.coord_interval)
        c.rebalance_max_moves = _env_int("WHUB_REBALANCE_MAX_MOVES",
                                         c.rebalance_max_moves)
        c.drain_deadline = _env_float("WHUB_DRAIN_DEADLINE", c.drain_deadline)
        c.worker_stale_after = _env_float("WHUB_WORKER_STALE_AFTER",
                                          c.worker_stale_after)
        c.outage_marker = os.environ.get("WHUB_OUTAGE_MARKER", c.outage_marker)
        c.debug_admin = os.environ.get("WHUB_DEBUG_ADMIN", "0") in (
            "1", "true", "True")
        return c

    def effective_renew_interval(self) -> float:
        return self.renew_interval or self.lease_ttl / 3.0

    def effective_worker_stale_after(self) -> float:
        return self.worker_stale_after or self.lease_ttl * 2.0
