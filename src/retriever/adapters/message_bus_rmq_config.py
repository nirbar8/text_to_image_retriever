from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RmqConfig:
    host: str
    port: int
    user: str
    password: str
    prefetch_count: int = 256
    heartbeat_s: int = 0
    blocked_connection_timeout_s: int = 0
    ack_debug: bool = False
