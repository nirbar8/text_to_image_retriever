from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TileKey:
    source: str
    z: int
    x: int
    y: int
    variant: Optional[str] = None


def canonical_tile_id(key: TileKey) -> str:
    """Return a canonical, stable tile id string from a TileKey."""
    variant = key.variant or ""
    return f"{key.source}:{key.z}/{key.x}/{key.y}:{variant}".rstrip(":")


def tile_id_hash(tile_id: str) -> str:
    """Return a short, deterministic hash for use as a stable key."""
    h = hashlib.sha256(tile_id.encode("utf-8")).hexdigest()
    return h[:16]
