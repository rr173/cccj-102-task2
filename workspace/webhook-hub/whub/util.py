"""通用工具：ID 生成。"""
from __future__ import annotations

import secrets


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(12)}"
