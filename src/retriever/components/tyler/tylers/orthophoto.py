from __future__ import annotations

from typing import List

import rasterio
from rasterio.windows import Window
from shapely.geometry import Polygon

from retriever.components.tyler.models.orthophoto_config import OrthophotoTylerConfig
from retriever.components.tyler.settings.orthophoto_settings import OrthophotoSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.schemas import TileSpec
from retriever.core.tile_id import TileKey, canonical_tile_id


class OrthophotoTyler(BaseTyler):
    tyler_mode: str = TylerMode.ORTHOPHOTO.value

    def __init__(self, cfg: OrthophotoTylerConfig):
        self._cfg = cfg

    @classmethod
    def from_settings(cls, settings: OrthophotoSettings) -> "OrthophotoTyler":
        """Create an OrthophotoTyler from settings."""
        cfg = OrthophotoTylerConfig(
            raster_path=settings.raster_path,
            tile_size_px=settings.tile_size_px,
            stride_px=settings.stride_px,
        )
        return cls(cfg)

    @classmethod
    def get_settings_from(cls, tyler_settings) -> OrthophotoSettings:
        """Get the settings for this tyler from TylerSettings."""
        return tyler_settings.orthophoto

    @property
    def tile_store(self) -> str:
        return "orthophoto"

    @property
    def source(self) -> str:
        return "orthophoto"

    @property
    def tyler_mode(self) -> str:
        return TylerMode.ORTHOPHOTO.value

    def generate_tiles(self) -> List[TileSpec]:
        tiles: List[TileSpec] = []
        image_id = 0
        with rasterio.open(self._cfg.raster_path) as src:
            if src.crs is None:
                raise ValueError("Raster has no CRS")
            for row in range(0, src.height, self._cfg.stride_px):
                for col in range(0, src.width, self._cfg.stride_px):
                    window = Window(col, row, self._cfg.tile_size_px, self._cfg.tile_size_px)
                    full = Window(0, 0, src.width, src.height)
                    if window.col_off >= src.width or window.row_off >= src.height:
                        continue
                    window = window.intersection(full)
                    if window.width <= 0 or window.height <= 0:
                        continue

                    pixel_poly = Polygon(
                        [
                            (window.col_off, window.row_off),
                            (window.col_off + window.width, window.row_off),
                            (window.col_off + window.width, window.row_off + window.height),
                            (window.col_off, window.row_off + window.height),
                            (window.col_off, window.row_off),
                        ]
                    )

                    key = TileKey(source=self._cfg.source_name, z=0, x=col, y=row)
                    tile_id = canonical_tile_id(key)

                    tiles.append(
                        TileSpec(
                            image_id=image_id,
                            tile_id=tile_id,
                            raster_path=str(self._cfg.raster_path),
                            pixel_polygon=pixel_poly.wkt,
                            width=int(window.width),
                            height=int(window.height),
                            tyler_mode=self.tyler_mode,
                        )
                    )
                    image_id += 1
        return tiles
