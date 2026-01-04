from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RetrieverSettings(BaseSettings):
    vectordb_url: str = Field(default="http://localhost:8001")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8002)
    model_name: str = Field(default="PE-Core-B16-224")

    model_config = SettingsConfigDict(
        env_prefix="RETRIEVER_",
        env_file=".env.retriever",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
