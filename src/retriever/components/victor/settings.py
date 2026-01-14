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
    embedder_queues: str = Field(default="pe_core=tiles.to_index.pe_core")

    tiles_db_path: Path = Field(default=Path("data/tiles.db"))
    vectordb_url: str = Field(default="http://localhost:8001")
    ready_status: str = Field(default="ready")
    ready_batch_limit: int = Field(default=500)
    poll_interval_s: float = Field(default=60.0)
    ttl_s: int = Field(default=0)
    ttl_batch_limit: int = Field(default=5000)
    vectordb_tables: str = Field(default="")

    model_config = SettingsConfigDict(
        env_prefix="VICTOR_",
        env_file="config/examples/.env.victor",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
