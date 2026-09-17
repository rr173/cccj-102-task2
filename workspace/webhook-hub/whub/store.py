"""durable store 进程的薄垫片。

实际 HTTP 面在 :mod:`whub.api` 中以 ``HubServer(enable_rpc=True)`` 提供
（业务/控制面/``POST /rpc`` 共用一套 handler）。本模块保留 Engine 的进程
语义说明与测试辅助。
"""
from __future__ import annotations

from .engine import Engine  # re-export

__all__ = ["Engine"]
