from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Tuple

import numpy as np

from retriever.core.interfaces import Tyler
from retriever.core.schemas import TileSpec


class BaseTyler(ABC):
    """Base abstract class for all tyler implementations."""

    @abstractmethod
    def generate_tiles(self) -> List[TileSpec]:
        """Generate tiles and return a list of TileSpec objects."""
        ...

    @property
    @abstractmethod
    def tile_store(self) -> str:
        """Return the tile store identifier for this tyler."""
        ...

    @property
    @abstractmethod
    def source(self) -> str:
        """Return the source identifier for this tyler."""
        ...

    @property
    @abstractmethod
    def tyler_mode(self) -> str:
        """Return the tyler mode identifier for this tyler."""
        ...

    def _random_geo(self, rng: np.random.Generator, lat_range: Tuple[float, float], lon_range: Tuple[float, float]) -> Tuple[float, float, str]:
        """
        Generate random geographic coordinates and UTM zone.
        
        Args:
            rng: NumPy random number generator
            lat_range: Tuple of (min_lat, max_lat)
            lon_range: Tuple of (min_lon, max_lon)
            
        Returns:
            Tuple of (lat, lon, utm_zone)
        """
        lat = float(rng.uniform(lat_range[0], lat_range[1]))
        lon = float(rng.uniform(lon_range[0], lon_range[1]))
        zone = int((lon + 180.0) // 6.0) + 1
        zone = min(max(zone, 1), 60)
        hemi = "N" if lat >= 0 else "S"
        utm_zone = f"{zone:02d}{hemi}"
        return lat, lon, utm_zone
