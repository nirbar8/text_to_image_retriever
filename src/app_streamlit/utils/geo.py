from __future__ import annotations

import hashlib
from typing import Tuple


def _stable_hash_int(x: str) -> int:
    h = hashlib.sha256(x.encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def simulate_lat_lon(
    image_id: int,
    center: Tuple[float, float] = (32.0853, 34.7818),
    radius_deg: float = 0.25,
) -> Tuple[float, float]:
    """Deterministic pseudo-geo for infra testing."""
    base = _stable_hash_int(str(image_id))
    u = (base % 10_000) / 10_000.0
    v = ((base // 10_000) % 10_000) / 10_000.0

    lat = center[0] + (u - 0.5) * 2 * radius_deg
    lon = center[1] + (v - 0.5) * 2 * radius_deg
    return lat, lon
