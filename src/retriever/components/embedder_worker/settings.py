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
    rmq_retry_s: float = Field(default=5.0)

    vectordb_url: str = Field(default="http://localhost:8001")
    vectordb_timeout_s: float = Field(default=30.0)
    vectordb_retry_s: float = Field(default=2.0)
    vectordb_retry_max_s: float = Field(default=30.0)
    table_name: str = Field(default="")
    embedder_backend: str = Field(default="pe_core")
    model_name: str = Field(default="PE-Core-B16-224")
    clip_pretrained: str = Field(default="openai")
    remote_clip_url: str = Field(default="")
    remote_clip_timeout_s: float = Field(default=60.0)
    remote_clip_image_format: str = Field(default="png")

    tile_store: str = Field(default="orthophoto")
    raster_path: Path = Field(default=Path("data/rasters/orthophoto.tif"))
    cache_tiles: bool = Field(default=True)
    tile_cache_dir: Path = Field(default=Path("data/tiles_cache"))
    tile_cache_format: str = Field(default="png")
    update_tile_statuses: bool = Field(default=True)
    tiles_db_path: Path = Field(default=Path("data/tiles.db"))

    batch_size: int = Field(default=64)
    max_inflight: int = Field(default=512)
    decode_workers: int = Field(default=8)
    flush_rows: int = Field(default=2048)
    flush_interval_s: float = Field(default=5.0)
    idle_flush_s: float = Field(default=3.0)
    job_timeout_s: float = Field(default=30.0)
    recv_log_every: int = Field(default=50)
    pending_log_every_s: float = Field(default=10.0)
    idle_log_every_s: float = Field(default=15.0)

    model_config = SettingsConfigDict(
        env_prefix="EMBEDDER_",
        env_file=".env.embedder",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
