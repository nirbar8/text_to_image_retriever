from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VectorDBSettings(BaseSettings):
    db_dir: Path = Field(default=Path("data/lancedb"))
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8001)

    model_config = SettingsConfigDict(
        env_prefix="VECTORDB_",
        env_file="config/examples/.env.vectordb",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
