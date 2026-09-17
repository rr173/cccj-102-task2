"""运行配置（全部可用环境变量覆盖）。"""
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
    discover_interval: float = 1.0     # Supervisor 发现端点
    reap_interval: float = 15.0        # 过期租约回收
    backoff_base: float = 0.5          # 指数退避基数（秒）
    backoff_cap: float = 30.0          # 指数退避上限（秒）
    max_workers: int = 256             # 全局出站 HTTP 工作线程上限
    seed: bool = True                  # 启动时种入演示租户

    @classmethod
    def from_env(cls) -> "HubConfig":
        c = cls()
        c.host = os.environ.get("WHUB_HOST", c.host)
        c.port = _env_int("WHUB_PORT", c.port)
        c.db_path = os.environ.get("WHUB_DB", c.db_path)
        c.http_timeout = _env_float("WHUB_HTTP_TIMEOUT", c.http_timeout)
        c.poll_interval = _env_float("WHUB_POLL_INTERVAL", c.poll_interval)
        c.discover_interval = _env_float("WHUB_DISCOVER_INTERVAL", c.discover_interval)
        c.reap_interval = _env_float("WHUB_REAP_INTERVAL", c.reap_interval)
        c.backoff_base = _env_float("WHUB_BACKOFF_BASE", c.backoff_base)
        c.backoff_cap = _env_float("WHUB_BACKOFF_CAP", c.backoff_cap)
        c.max_workers = _env_int("WHUB_MAX_WORKERS", c.max_workers)
        c.seed = os.environ.get("WHUB_SEED", "1") not in ("0", "false", "False")
        return c
