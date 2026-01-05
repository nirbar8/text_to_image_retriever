from __future__ import annotations

from enum import Enum
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TylerMode(str, Enum):
    ORTHOPHOTO = "orthophoto"
    SATELLITE = "satellite"
    COCO = "coco"


class OrthophotoSettings(BaseModel):
    raster_path: Path = Field(default=Path("data/rasters/orthophoto.tif"))
    tile_size_px: int = Field(default=512)
    stride_px: int = Field(default=512)


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


class CocoSettings(BaseModel):
    images_dir: Path = Field(default=Path("data/coco/train2017"))
    instances_json: Path = Field(default=Path("data/coco/annotations/instances_train2017.json"))
    max_items: int = Field(default=10000)
    seed: int = Field(default=1337)
    lat_min: float = Field(default=-60.0)
    lat_max: float = Field(default=60.0)
    lon_min: float = Field(default=-180.0)
    lon_max: float = Field(default=180.0)


class TylerSettings(BaseSettings):
    mode: TylerMode = Field(default=TylerMode.ORTHOPHOTO)
    orthophoto: OrthophotoSettings = Field(default_factory=OrthophotoSettings)
    satellite: SatelliteSettings = Field(default_factory=SatelliteSettings)
    coco: CocoSettings = Field(default_factory=CocoSettings)
    output_jsonl: Path = Field(default=Path("data/tiles.jsonl"))

    model_config = SettingsConfigDict(
        env_prefix="TYLER_",
        env_file="config/examples/.env.tyler",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
    )
