from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import numpy as np
from PIL import Image

from retriever.core.tile_id import TileKey, canonical_tile_id


@dataclass(frozen=True)
class TileSpec:
    image_id: int
    tile_id: str
    image_path: str
    width: int
    height: int
    lat: float
    lon: float
    utm_zone: str


@dataclass(frozen=True)
class DotaTylerConfig:
    images_root: Path
    max_items: int = 10000
    seed: int = 1337
    lat_range: Tuple[float, float] = (-60.0, 60.0)
    lon_range: Tuple[float, float] = (-180.0, 180.0)
    source_name: str = "dota"
    extensions: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".tif", ".tiff")


class DotaTyler:
    def __init__(self, cfg: DotaTylerConfig):
        self._cfg = cfg

    def _random_geo(self, rng: np.random.Generator) -> Tuple[float, float, str]:
        lat = float(rng.uniform(self._cfg.lat_range[0], self._cfg.lat_range[1]))
        lon = float(rng.uniform(self._cfg.lon_range[0], self._cfg.lon_range[1]))
        zone = int((lon + 180.0) // 6.0) + 1
        zone = min(max(zone, 1), 60)
        hemi = "N" if lat >= 0 else "S"
        utm_zone = f"{zone:02d}{hemi}"
        return lat, lon, utm_zone

    def _iter_images(self) -> Iterable[Path]:
        if not self._cfg.images_root.exists():
            return []
        return sorted(
            path
            for path in self._cfg.images_root.rglob("*")
            if path.is_file() and path.suffix.lower() in self._cfg.extensions
        )

    def generate_tiles(self) -> List[TileSpec]:
        images = list(self._iter_images())
        n = min(self._cfg.max_items, len(images))
        rng = np.random.default_rng(self._cfg.seed)

        tiles: List[TileSpec] = []
        for idx, img_path in enumerate(images[:n], start=1):
            with Image.open(img_path) as img:
                width, height = img.size

            lat, lon, utm_zone = self._random_geo(rng)
            image_id = idx
            key = TileKey(source=self._cfg.source_name, z=0, x=image_id, y=0)
            tile_id = canonical_tile_id(key)

            tiles.append(
                TileSpec(
                    image_id=image_id,
                    tile_id=tile_id,
                    image_path=str(img_path.resolve()),
                    width=int(width),
                    height=int(height),
                    lat=lat,
                    lon=lon,
                    utm_zone=utm_zone,
                )
            )
        return tiles
