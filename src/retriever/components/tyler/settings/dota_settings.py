from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel, Field


class DotaSettings(BaseModel):
    images_root: Path = Field(default=Path("data/dota"))
    max_items: int = Field(default=10000)
    seed: int = Field(default=1337)
    lat_min: float = Field(default=-60.0)
    lat_max: float = Field(default=60.0)
    lon_min: float = Field(default=-180.0)
    lon_max: float = Field(default=180.0)
