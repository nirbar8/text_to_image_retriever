from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import geopandas as gpd
import numpy as np
from shapely.affinity import rotate
from shapely.geometry import Polygon, box

from retriever.core.tile_id import TileKey, canonical_tile_id


@dataclass(frozen=True)
class TileSpec:
    image_id: int
    tile_id: str
    gid: int
    minx: float
    miny: float
    maxx: float
    maxy: float
    crs: str
    width: int
    height: int


@dataclass(frozen=True)
class SatelliteTylerConfig:
    bounds: Tuple[float, float, float, float]
    tile_size_deg: float = 0.01
    tile_size_px: int = 512
    image_count: int = 10
    image_size_deg: float = 0.05
    rotation_deg_max: float = 45.0
    seed: int = 1337
    output_crs: str = "EPSG:4326"
    source_name: str = "satellite"


class SatelliteBoundsTyler:
    def __init__(self, cfg: SatelliteTylerConfig):
        self._cfg = cfg

    def _random_image_polygons(self) -> List[Polygon]:
        minx, miny, maxx, maxy = self._cfg.bounds
        rng = np.random.default_rng(self._cfg.seed)
        polys: List[Polygon] = []
        half = self._cfg.image_size_deg / 2.0

        for _ in range(self._cfg.image_count):
            cx = rng.uniform(minx + half, maxx - half)
            cy = rng.uniform(miny + half, maxy - half)
            rect = box(cx - half, cy - half, cx + half, cy + half)
            angle = rng.uniform(-self._cfg.rotation_deg_max, self._cfg.rotation_deg_max)
            polys.append(rotate(rect, angle=angle, origin=(cx, cy)))
        return polys

    def generate_tiles(self) -> List[TileSpec]:
        tiles: List[TileSpec] = []
        image_polys = self._random_image_polygons()
        gdf = gpd.GeoSeries(image_polys, crs=self._cfg.output_crs)

        image_id = 0
        for gid, poly in enumerate(gdf):
            minx, miny, maxx, maxy = poly.bounds
            y = miny
            row = 0
            while y < maxy:
                x = minx
                col = 0
                while x < maxx:
                    tile_minx = x
                    tile_miny = y
                    tile_maxx = min(x + self._cfg.tile_size_deg, maxx)
                    tile_maxy = min(y + self._cfg.tile_size_deg, maxy)

                    tile_poly = box(tile_minx, tile_miny, tile_maxx, tile_maxy)
                    if poly.contains(tile_poly):
                        key = TileKey(source=self._cfg.source_name, z=0, x=col, y=row, variant=str(gid))
                        tile_id = canonical_tile_id(key)
                        tiles.append(
                            TileSpec(
                                image_id=image_id,
                                tile_id=tile_id,
                                gid=int(gid),
                                minx=float(tile_minx),
                                miny=float(tile_miny),
                                maxx=float(tile_maxx),
                                maxy=float(tile_maxy),
                                crs=self._cfg.output_crs,
                                width=int(self._cfg.tile_size_px),
                                height=int(self._cfg.tile_size_px),
                            )
                        )
                        image_id += 1
                    col += 1
                    x += self._cfg.tile_size_deg
                row += 1
                y += self._cfg.tile_size_deg
        return tiles
