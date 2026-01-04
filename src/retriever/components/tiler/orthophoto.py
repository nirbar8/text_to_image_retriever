from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import rasterio
from rasterio.windows import Window
from rasterio.warp import transform_bounds

from retriever.core.tile_id import TileKey, canonical_tile_id


@dataclass(frozen=True)
class TileSpec:
    tile_id: str
    minx: float
    miny: float
    maxx: float
    maxy: float
    crs: str
    width: int
    height: int


@dataclass(frozen=True)
class OrthophotoTilerConfig:
    raster_path: Path
    tile_size_px: int = 512
    stride_px: int = 512
    output_crs: str = "EPSG:4326"
    source_name: str = "orthophoto"


class OrthophotoTiler:
    def __init__(self, cfg: OrthophotoTilerConfig):
        self._cfg = cfg

    def generate_tiles(self) -> List[TileSpec]:
        tiles: List[TileSpec] = []
        with rasterio.open(self._cfg.raster_path) as src:
            if src.crs is None:
                raise ValueError("Raster has no CRS")
            for row in range(0, src.height, self._cfg.stride_px):
                for col in range(0, src.width, self._cfg.stride_px):
                    window = Window(col, row, self._cfg.tile_size_px, self._cfg.tile_size_px)
                    if window.col_off >= src.width or window.row_off >= src.height:
                        continue

                    bounds = rasterio.windows.bounds(window, transform=src.transform)
                    if self._cfg.output_crs != str(src.crs):
                        minx, miny, maxx, maxy = transform_bounds(
                            src.crs, self._cfg.output_crs, *bounds, densify_pts=21
                        )
                    else:
                        minx, miny, maxx, maxy = bounds

                    z = 0
                    key = TileKey(source=self._cfg.source_name, z=z, x=col, y=row)
                    tile_id = canonical_tile_id(key)

                    tiles.append(
                        TileSpec(
                            tile_id=tile_id,
                            minx=float(minx),
                            miny=float(miny),
                            maxx=float(maxx),
                            maxy=float(maxy),
                            crs=self._cfg.output_crs,
                            width=int(window.width),
                            height=int(window.height),
                        )
                    )
        return tiles
