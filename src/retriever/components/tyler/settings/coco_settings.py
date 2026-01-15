from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel, Field


class CocoSettings(BaseModel):
    images_dir: Path = Field(default=Path("data/coco/train2017"))
    instances_json: Path = Field(default=Path("data/coco/annotations/instances_train2017.json"))
    max_items: int = Field(default=10000)
    seed: int = Field(default=1337)
    lat_min: float = Field(default=-60.0)
    lat_max: float = Field(default=60.0)
    lon_min: float = Field(default=-180.0)
    lon_max: float = Field(default=180.0)
