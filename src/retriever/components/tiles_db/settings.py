"""Settings and configuration for TilesDB service."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseModel):
    """Database configuration."""

    db_path: Path = Field(
        default=Path("data/tiles.db"),
        description="Path to SQLite database file",
    )


class TilesDBSettings(BaseSettings):
    """Settings for the TilesDB service."""

    host: str = Field(
        default="127.0.0.1",
        description="FastAPI server host",
    )
    port: int = Field(
        default=8001,
        description="FastAPI server port",
    )
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig,
        description="Database configuration",
    )

    model_config = SettingsConfigDict(
        env_prefix="TILESDB_",
        env_nested_delimiter="__",
        env_file="config/examples/.env.tilesdb",
        case_sensitive=False,
    )
