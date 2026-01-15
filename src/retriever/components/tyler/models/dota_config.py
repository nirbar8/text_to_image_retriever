from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from retriever.components.tyler.models.base_config import BaseTylerConfig


@dataclass(frozen=True)
class DotaTylerConfig(BaseTylerConfig):
    images_root: Path
    max_items: int = 10000
    seed: int = 1337
    lat_range: Tuple[float, float] = (-60.0, 60.0)
    lon_range: Tuple[float, float] = (-180.0, 180.0)
    source_name: str = "dota"
    output_crs: str = "EPSG:4326"
    extensions: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".tif", ".tiff")
