"""Victor scheduler settings and configuration."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Default values
_DEFAULT_SCHEDULE_INTERVAL_SECONDS = 60
_DEFAULT_BATCH_SIZE = 1000
_DEFAULT_TILESDB_URL = "http://127.0.0.1:8001"
_DEFAULT_RMQ_HOST = "localhost"
_DEFAULT_RMQ_PORT = 5672
_DEFAULT_RMQ_USER = "guest"
_DEFAULT_RMQ_PASSWORD = "guest"
_DEFAULT_EMBEDDER_QUEUES = "pe_core=tiles.to_index.pe_core"

# Configuration
_ENV_PREFIX = "VICTOR_"
_ENV_FILE = "config/examples/.env.victor"
_ENV_FILE_ENCODING = "utf-8"


class VictorSettings(BaseSettings):
    """Settings for Victor scheduler component.

    Victor periodically checks TilesDB for tiles with status READY_FOR_INDEXING
    and publishes them to RabbitMQ embedding queues based on their embedder_model.

    All settings can be overridden via environment variables with VICTOR_ prefix.
    Example: VICTOR_SCHEDULE_INTERVAL_SECONDS=30
    """

    # Scheduler configuration
    schedule_interval_seconds: int = Field(
        default=_DEFAULT_SCHEDULE_INTERVAL_SECONDS,
        description="Interval in seconds between scheduler runs",
        gt=0,
    )
    batch_size: int = Field(
        default=_DEFAULT_BATCH_SIZE,
        description="Maximum number of tiles to fetch and process per run",
        gt=0,
    )

    # TilesDB service configuration
    tilesdb_url: str = Field(
        default=_DEFAULT_TILESDB_URL,
        description="Base URL of TilesDB HTTP service",
    )

    # RabbitMQ connection configuration
    rmq_host: str = Field(
        default=_DEFAULT_RMQ_HOST,
        description="RabbitMQ server hostname or IP address",
    )
    rmq_port: int = Field(
        default=_DEFAULT_RMQ_PORT,
        description="RabbitMQ server port",
        gt=0,
        lt=65536,
    )
    rmq_user: str = Field(
        default=_DEFAULT_RMQ_USER,
        description="RabbitMQ authentication username",
    )
    rmq_pass: str = Field(
        default=_DEFAULT_RMQ_PASSWORD,
        description="RabbitMQ authentication password",
    )

    # Queue routing configuration
    embedder_queues: str = Field(
        default=_DEFAULT_EMBEDDER_QUEUES,
        description=(
            "Comma-separated queue mappings in format: backend=queue or backend:model=queue. "
            "Example: pe_core=tiles.to_index.pe_core,clip:ViT-B-32=tiles.to_index.clip"
        ),
    )

    model_config = SettingsConfigDict(
        env_prefix=_ENV_PREFIX,
        env_file=_ENV_FILE,
        env_file_encoding=_ENV_FILE_ENCODING,
        case_sensitive=False,
        extra="ignore",
    )
