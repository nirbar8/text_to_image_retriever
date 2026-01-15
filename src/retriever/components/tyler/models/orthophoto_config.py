from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from retriever.components.tyler.models.base_config import BaseTylerConfig


@dataclass(frozen=True)
class OrthophotoTylerConfig(BaseTylerConfig):
    raster_path: Path
    tile_size_px: int = 512
    stride_px: int = 512
    source_name: str = "orthophoto"
    output_crs: str = "EPSG:4326"
