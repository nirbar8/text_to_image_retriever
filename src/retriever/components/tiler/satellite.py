from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

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
class SatelliteTilerConfig:
    bounds: Tuple[float, float, float, float]
    tile_size_deg: float = 0.01
    output_crs: str = "EPSG:4326"
    source_name: str = "satellite"


class SatelliteBoundsTiler:
    def __init__(self, cfg: SatelliteTilerConfig):
        self._cfg = cfg

    def generate_tiles(self) -> List[TileSpec]:
        minx, miny, maxx, maxy = self._cfg.bounds
        tiles: List[TileSpec] = []
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
                key = TileKey(source=self._cfg.source_name, z=0, x=col, y=row)
                tile_id = canonical_tile_id(key)
                tiles.append(
                    TileSpec(
                        tile_id=tile_id,
                        minx=float(tile_minx),
                        miny=float(tile_miny),
                        maxx=float(tile_maxx),
                        maxy=float(tile_maxy),
                        crs=self._cfg.output_crs,
                        width=0,
                        height=0,
                    )
                )
                col += 1
                x += self._cfg.tile_size_deg
            row += 1
            y += self._cfg.tile_size_deg
        return tiles
