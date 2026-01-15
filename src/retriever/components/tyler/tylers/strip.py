from __future__ import annotations

from typing import List, Set

from shapely.geometry import Polygon

from retriever.components.tyler.models.strip_config import StripTylerConfig
from retriever.components.tyler.settings.strip_settings import StripSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode
from retriever.components.tyler.tylers.abstracts import BaseTyler
from retriever.core.schemas import TileSpec
from retriever.core.tile_id import TileKey, canonical_tile_id


class StripTyler(BaseTyler):
    tyler_mode: str = TylerMode.STRIP.value

    def __init__(self, cfg: StripTylerConfig):
        self._cfg = cfg

    @classmethod
    def from_settings(cls, settings: StripSettings) -> "StripTyler":
        """Create a StripTyler from settings."""
        cfg = StripTylerConfig(
            image_id=settings.image_id,
            resolution_m_per_px=settings.resolution_m_per_px,
            image_width=settings.image_width,
            image_height=settings.image_height,
            tile_width=settings.tile_width,
            tile_height=settings.tile_height,
            target_meters=settings.target_meters,
        )
        return cls(cfg)

    @classmethod
    def get_settings_from(cls, tyler_settings) -> StripSettings:
        """Get the settings for this tyler from TylerSettings."""
        return tyler_settings.strip

    @property
    def tile_store(self) -> str:
        return "strip"

    @property
    def source(self) -> str:
        return "strip"

    @property
    def tyler_mode(self) -> str:
        return TylerMode.STRIP.value

    def _calculate_max_levels(self) -> int:
        """Calculate the maximum number of pyramid levels."""
        max_level = 0
        while (
            self._cfg.image_width / (2 ** max_level) >= self._cfg.tile_width
            and self._cfg.image_height / (2 ** max_level) >= self._cfg.tile_height
        ):
            max_level += 1
        return max(max_level - 1, 0)  # Return the last valid level

    def _calculate_tile_size_meters(self, level: int) -> float:
        """Calculate tile size in meters for a given pyramid level."""
        return self._cfg.tile_width * self._cfg.resolution_m_per_px / (2 ** level)

    def _find_closest_levels(self, target_meter: float, max_level: int) -> Set[int]:
        """Find the closest level(s) to the target meter size."""
        min_diff = float("inf")
        closest_levels: Set[int] = set()

        for level in range(max_level + 1):
            tile_size_meters = self._calculate_tile_size_meters(level)
            diff = abs(tile_size_meters - target_meter)

            if diff < min_diff:
                min_diff = diff
                closest_levels = {level}
            elif diff == min_diff:
                closest_levels.add(level)

        return closest_levels

    def _get_selected_levels(self) -> Set[int]:
        """Get all levels that are closest to any target meter."""
        max_level = self._calculate_max_levels()
        selected_levels: Set[int] = set()

        for target_meter in self._cfg.target_meters:
            closest_levels = self._find_closest_levels(target_meter, max_level)
            selected_levels.update(closest_levels)

        return selected_levels

    def generate_tiles(self) -> List[TileSpec]:
        """Generate tiles for selected pyramid levels."""
        tiles: List[TileSpec] = []
        selected_levels = self._get_selected_levels()

        for level in selected_levels:
            # Calculate level dimensions
            level_width = int(self._cfg.image_width / (2 ** level))
            level_height = int(self._cfg.image_height / (2 ** level))

            # Generate tiles for this level
            for row in range(0, level_height, self._cfg.tile_height):
                for col in range(0, level_width, self._cfg.tile_width):
                    # Calculate tile bounds at level resolution
                    tile_col_end = min(col + self._cfg.tile_width, level_width)
                    tile_row_end = min(row + self._cfg.tile_height, level_height)
                    actual_tile_width = tile_col_end - col
                    actual_tile_height = tile_row_end - row

                    if actual_tile_width <= 0 or actual_tile_height <= 0:
                        continue

                    # Calculate pixel coordinates at original image scale
                    # Level coordinates need to be scaled up to original image
                    scale_factor = 2 ** level
                    orig_col = col * scale_factor
                    orig_row = row * scale_factor
                    orig_col_end = tile_col_end * scale_factor
                    orig_row_end = tile_row_end * scale_factor

                    pixel_poly = Polygon(
                        [
                            (orig_col, orig_row),
                            (orig_col_end, orig_row),
                            (orig_col_end, orig_row_end),
                            (orig_col, orig_row_end),
                            (orig_col, orig_row),
                        ]
                    )

                    key = TileKey(
                        source=self._cfg.source_name,
                        z=level,
                        x=col,
                        y=row,
                    )
                    tile_id = canonical_tile_id(key)

                    # Calculate tile dimensions in meters
                    tile_width_m = actual_tile_width * self._cfg.resolution_m_per_px / (2 ** level)
                    tile_height_m = actual_tile_height * self._cfg.resolution_m_per_px / (2 ** level)

                    tiles.append(
                        TileSpec(
                            image_id=self._cfg.image_id,
                            tile_id=tile_id,
                            pixel_polygon=pixel_poly.wkt,
                            width=actual_tile_width,
                            height=actual_tile_height,
                            width_m=tile_width_m,
                            height_m=tile_height_m,
                            tyler_mode=self.tyler_mode,
                        )
                    )

        return tiles
