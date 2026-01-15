from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from retriever.components.tyler.models.base_config import BaseTylerConfig


@dataclass(frozen=True)
class SatelliteTylerConfig(BaseTylerConfig):
    bounds: Tuple[float, float, float, float]
    tile_size_deg: float = 0.01
    tile_size_px: int = 512
    image_count: int = 10
    image_size_deg: float = 0.05
    rotation_deg_max: float = 45.0
    seed: int = 1337
    source_name: str = "satellite"
    output_crs: str = "EPSG:4326"
