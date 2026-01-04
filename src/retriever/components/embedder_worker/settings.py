from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EmbedderSettings(BaseSettings):
    rmq_host: str = Field(default="localhost")
    rmq_port: int = Field(default=5672)
    rmq_user: str = Field(default="guest")
    rmq_pass: str = Field(default="guest")
    queue_name: str = Field(default="tiles.to_index")

    vectordb_url: str = Field(default="http://localhost:8001")
    table_name: str = Field(default="coco_pe_core_b16_224")

    batch_size: int = Field(default=64)
    max_inflight: int = Field(default=512)
    decode_workers: int = Field(default=8)
    flush_rows: int = Field(default=2048)

    model_config = SettingsConfigDict(
        env_prefix="EMBEDDER_",
        env_file=".env.embedder",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
