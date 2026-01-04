from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VictorSettings(BaseSettings):
    tiles_manifest_path: Path = Field(default=Path("data/tiles.jsonl"))

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
