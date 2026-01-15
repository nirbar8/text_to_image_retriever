from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import numpy as np
from PIL import Image

from retriever.components.tyler.models.dota_config import DotaTylerConfig
from retriever.components.tyler.settings.dota_settings import DotaSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.schemas import TileSpec
from retriever.core.tile_id import TileKey, canonical_tile_id


class DotaTyler(BaseTyler):
    tyler_mode: str = TylerMode.DOTA.value

    def __init__(self, cfg: DotaTylerConfig):
        self._cfg = cfg

    @classmethod
    def from_settings(cls, settings: DotaSettings) -> "DotaTyler":
        """Create a DotaTyler from settings."""
        cfg = DotaTylerConfig(
            images_root=settings.images_root,
            max_items=settings.max_items,
            seed=settings.seed,
            lat_range=(settings.lat_min, settings.lat_max),
            lon_range=(settings.lon_min, settings.lon_max),
        )
        return cls(cfg)

    @classmethod
    def get_settings_from(cls, tyler_settings) -> DotaSettings:
        """Get the settings for this tyler from TylerSettings."""
        return tyler_settings.dota

    @property
    def tile_store(self) -> str:
        return "local"

    @property
    def source(self) -> str:
        return "dota"

    @property
    def tyler_mode(self) -> str:
        return TylerMode.DOTA.value

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

            lat, lon, utm_zone = self._random_geo(rng, self._cfg.lat_range, self._cfg.lon_range)
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
                    tyler_mode=self.tyler_mode,
                )
            )
        return tiles
