from __future__ import annotations

from pydantic import BaseModel, Field
from typing import List


class StripSettings(BaseModel):
    image_id: int = Field(default=0)
    resolution_m_per_px: float = Field(default=1.0)
    image_width: int = Field(default=2560)
    image_height: int = Field(default=2560)
    tile_width: int = Field(default=256)
    tile_height: int = Field(default=256)
    target_meters: List[float] = Field(default_factory=lambda: [100.0, 200.0])
