from __future__ import annotations

from typing import List

import numpy as np
import orjson
from shapely.geometry import Polygon

from retriever.components.tyler.models.coco_config import CocoTylerConfig
from retriever.components.tyler.settings.coco_settings import CocoSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.schemas import TileSpec
from retriever.core.tile_id import TileKey, canonical_tile_id


class CocoTyler(BaseTyler):
    tyler_mode: str = TylerMode.COCO.value

    def __init__(self, cfg: CocoTylerConfig):
        self._cfg = cfg

    @classmethod
    def from_settings(cls, settings: CocoSettings) -> "CocoTyler":
        """Create a CocoTyler from settings."""
        cfg = CocoTylerConfig(
            instances_json=settings.instances_json,
            images_dir=settings.images_dir,
            max_items=settings.max_items,
            seed=settings.seed,
            lat_range=(settings.lat_min, settings.lat_max),
            lon_range=(settings.lon_min, settings.lon_max),
        )
        return cls(cfg)

    @classmethod
    def get_settings_from(cls, tyler_settings) -> CocoSettings:
        """Get the settings for this tyler from TylerSettings."""
        return tyler_settings.coco

    @property
    def tile_store(self) -> str:
        return "local"

    @property
    def source(self) -> str:
        return "coco"

    @property
    def tyler_mode(self) -> str:
        return TylerMode.COCO.value

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
            lat, lon, utm_zone = self._random_geo(rng, self._cfg.lat_range, self._cfg.lon_range)

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
                    tyler_mode=self.tyler_mode,
                )
            )
        return tiles
