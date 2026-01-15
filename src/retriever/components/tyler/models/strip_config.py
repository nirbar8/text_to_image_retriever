from __future__ import annotations

from dataclasses import dataclass
from typing import List

from retriever.components.tyler.models.base_config import BaseTylerConfig


@dataclass(frozen=True)
class StripTylerConfig(BaseTylerConfig):
    image_id: int
    resolution_m_per_px: float
    image_width: int
    image_height: int
    tile_width: int
    tile_height: int
    target_meters: List[float]
    source_name: str = "strip"
    output_crs: str = "EPSG:4326"
