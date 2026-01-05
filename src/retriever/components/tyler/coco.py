from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import orjson
from shapely.geometry import Polygon

from retriever.core.tile_id import TileKey, canonical_tile_id


@dataclass(frozen=True)
class TileSpec:
    image_id: int
    tile_id: str
    image_path: str
    pixel_polygon: str
    width: int
    height: int
    lat: float
    lon: float
    utm_zone: str


@dataclass(frozen=True)
class CocoTylerConfig:
    instances_json: Path
    images_dir: Path
    max_items: int = 10000
    seed: int = 1337
    lat_range: Tuple[float, float] = (-60.0, 60.0)
    lon_range: Tuple[float, float] = (-180.0, 180.0)
    source_name: str = "coco"


class CocoTyler:
    def __init__(self, cfg: CocoTylerConfig):
        self._cfg = cfg

    def _random_geo(self, rng: np.random.Generator) -> Tuple[float, float, str]:
        lat = float(rng.uniform(self._cfg.lat_range[0], self._cfg.lat_range[1]))
        lon = float(rng.uniform(self._cfg.lon_range[0], self._cfg.lon_range[1]))
        zone = int((lon + 180.0) // 6.0) + 1
        zone = min(max(zone, 1), 60)
        hemi = "N" if lat >= 0 else "S"
        utm_zone = f"{zone:02d}{hemi}"
        return lat, lon, utm_zone

    def generate_tiles(self) -> List[TileSpec]:
        data = orjson.loads(self._cfg.instances_json.read_bytes())
        images = data.get("images", [])
        n = min(self._cfg.max_items, len(images))
        rng = np.random.default_rng(self._cfg.seed)

        tiles: List[TileSpec] = []
        for img in images[:n]:
            image_id = int(img["id"])
            file_name = img["file_name"]
            image_path = str((self._cfg.images_dir / file_name).resolve())
            width = int(img["width"])
            height = int(img["height"])
            lat, lon, utm_zone = self._random_geo(rng)

            key = TileKey(source=self._cfg.source_name, z=0, x=image_id, y=0)
            tile_id = canonical_tile_id(key)
            pixel_poly = Polygon([(0, 0), (width, 0), (width, height), (0, height), (0, 0)])

            tiles.append(
                TileSpec(
                    image_id=image_id,
                    tile_id=tile_id,
                    image_path=image_path,
                    pixel_polygon=pixel_poly.wkt,
                    width=width,
                    height=height,
                    lat=lat,
                    lon=lon,
                    utm_zone=utm_zone,
                )
            )
        return tiles
