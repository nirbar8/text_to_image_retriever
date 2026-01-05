from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import rasterio
from rasterio.windows import Window
from shapely.geometry import Polygon

from retriever.core.tile_id import TileKey, canonical_tile_id


@dataclass(frozen=True)
class TileSpec:
    image_id: int
    tile_id: str
    raster_path: str
    pixel_polygon: str
    width: int
    height: int


@dataclass(frozen=True)
class OrthophotoTylerConfig:
    raster_path: Path
    tile_size_px: int = 512
    stride_px: int = 512
    output_crs: str = "EPSG:4326"
    source_name: str = "orthophoto"


class OrthophotoTyler:
    def __init__(self, cfg: OrthophotoTylerConfig):
        self._cfg = cfg

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
                        )
                    )
                    image_id += 1
        return tiles
