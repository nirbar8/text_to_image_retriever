from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VictorSettings(BaseSettings):
    manifest_path: Path = Field(default=Path("data/manifest_100k.jsonl"))
    coco_root: Path = Field(default=Path("data/coco"))
    coco_images_dir: Path = Field(default=Path("data/coco/train2017"))
    coco_instances_json: Path = Field(default=Path("data/coco/annotations/instances_train2017.json"))
    max_items: int = Field(default=100_000)

    rmq_host: str = Field(default="localhost")
    rmq_port: int = Field(default=5672)
    rmq_user: str = Field(default="guest")
    rmq_pass: str = Field(default="guest")
    queue_name: str = Field(default="tiles.to_index")

    tiles_db_path: Path = Field(default=Path("data/tiles.db"))

    model_config = SettingsConfigDict(
        env_prefix="VICTOR_",
        env_file=".env.victor",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
