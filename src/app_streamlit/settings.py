from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    retriever_url: str = Field(default="http://localhost:8002")
    vectordb_url: str = Field(default="http://localhost:8001")
    table_name: str = Field(default="coco_pe_core_b16_224")
    timeout_s: float = Field(default=60.0)

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_file=".env.app",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
