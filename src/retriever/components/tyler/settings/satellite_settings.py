from __future__ import annotations

from pydantic import BaseModel, Field


class SatelliteSettings(BaseModel):
    bounds_minx: float = Field(default=34.7)
    bounds_miny: float = Field(default=32.0)
    bounds_maxx: float = Field(default=34.9)
    bounds_maxy: float = Field(default=32.2)
    tile_size_deg: float = Field(default=0.01)
    tile_size_px: int = Field(default=512)
    image_count: int = Field(default=10)
    image_size_deg: float = Field(default=0.05)
    rotation_deg_max: float = Field(default=45.0)
    seed: int = Field(default=1337)
