from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel, Field


class OrthophotoSettings(BaseModel):
    raster_path: Path = Field(default=Path("data/rasters/orthophoto.tif"))
    tile_size_px: int = Field(default=512)
    stride_px: int = Field(default=512)
