from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TylerSettings(BaseSettings):
    mode: str = Field(default="orthophoto")
    raster_path: Path = Field(default=Path("data/rasters/orthophoto.tif"))
    bounds_minx: float = Field(default=34.7)
    bounds_miny: float = Field(default=32.0)
    bounds_maxx: float = Field(default=34.9)
    bounds_maxy: float = Field(default=32.2)
    tile_size_px: int = Field(default=512)
    stride_px: int = Field(default=512)
    tile_size_deg: float = Field(default=0.01)
    sat_image_count: int = Field(default=10)
    sat_image_size_deg: float = Field(default=0.05)
    sat_rotation_deg_max: float = Field(default=45.0)
    sat_seed: int = Field(default=1337)
    coco_images_dir: Path = Field(default=Path("data/coco/train2017"))
    coco_instances_json: Path = Field(default=Path("data/coco/annotations/instances_train2017.json"))
    coco_max_items: int = Field(default=500)
    coco_seed: int = Field(default=1337)
    coco_lat_min: float = Field(default=-60.0)
    coco_lat_max: float = Field(default=60.0)
    coco_lon_min: float = Field(default=-180.0)
    coco_lon_max: float = Field(default=180.0)
    output_jsonl: Path = Field(default=Path("data/tiles.jsonl"))

    model_config = SettingsConfigDict(
        env_prefix="TYLER_",
        env_file=".env.tyler",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
