from __future__ import annotations

from typing import List

import geopandas as gpd
import numpy as np
from shapely.affinity import rotate
from shapely.geometry import Polygon, box

from retriever.components.tyler.models.satellite_config import SatelliteTylerConfig
from retriever.components.tyler.settings.satellite_settings import SatelliteSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.schemas import TileSpec
from retriever.core.tile_id import TileKey, canonical_tile_id


class SatelliteBoundsTyler(BaseTyler):
    tyler_mode: str = TylerMode.SATELLITE.value

    def __init__(self, cfg: SatelliteTylerConfig):
        self._cfg = cfg

    @classmethod
    def from_settings(cls, settings: SatelliteSettings) -> "SatelliteBoundsTyler":
        """Create a SatelliteBoundsTyler from settings."""
        cfg = SatelliteTylerConfig(
            bounds=(settings.bounds_minx, settings.bounds_miny, settings.bounds_maxx, settings.bounds_maxy),
            tile_size_deg=settings.tile_size_deg,
            tile_size_px=settings.tile_size_px,
            image_count=settings.image_count,
            image_size_deg=settings.image_size_deg,
            rotation_deg_max=settings.rotation_deg_max,
            seed=settings.seed,
        )
        return cls(cfg)

    @classmethod
    def get_settings_from(cls, tyler_settings) -> SatelliteSettings:
        """Get the settings for this tyler from TylerSettings."""
        return tyler_settings.satellite

    @property
    def tile_store(self) -> str:
        return "synthetic"

    @property
    def source(self) -> str:
        return "satellite"

    @property
    def tyler_mode(self) -> str:
        return TylerMode.SATELLITE.value

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
                        pixel_poly = box(
                            col * self._cfg.tile_size_px,
                            row * self._cfg.tile_size_px,
                            (col + 1) * self._cfg.tile_size_px,
                            (row + 1) * self._cfg.tile_size_px,
                        )
                        key = TileKey(source=self._cfg.source_name, z=0, x=col, y=row, variant=str(gid))
                        tile_id = canonical_tile_id(key)
                        tiles.append(
                            TileSpec(
                                image_id=image_id,
                                tile_id=tile_id,
                                gid=int(gid),
                                pixel_polygon=pixel_poly.wkt,
                                width=int(self._cfg.tile_size_px),
                                height=int(self._cfg.tile_size_px),
                                tyler_mode=self.tyler_mode,
                            )
                        )
                        image_id += 1
                    col += 1
                    x += self._cfg.tile_size_deg
                row += 1
                y += self._cfg.tile_size_deg
        return tiles
