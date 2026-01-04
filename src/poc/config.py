from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuration for the PE + LanceDB + RabbitMQ POC.

    Values are loaded in this order:
    1. Environment variables
    2. .env file (if present)
    3. Defaults below
    """

    # ------------------
    # Dataset
    # ------------------
    coco_root: Path = Field(default=Path("data/coco"))
    coco_images_dir: Path = Field(default=Path("data/coco/train2017"))
    coco_instances_json: Path = Field(
        default=Path("data/coco/annotations/instances_train2017.json")
    )
    manifest_path: Path = Field(default=Path("data/manifest_100k.jsonl"))

    # ------------------
    # LanceDB
    # ------------------
    lancedb_dir: Path = Field(default=Path("data/lancedb"))
    table_name: str = Field(default="coco_pe_core_b16_224")

    # ------------------
    # RabbitMQ
    # ------------------
    rmq_host: str = Field(default="localhost")
    rmq_port: int = Field(default=5672)
    rmq_user: str = Field(default="guest")
    rmq_pass: str = Field(default="guest")
    queue_name: str = Field(default="images.to_index")

    # ------------------
    # Indexing
    # ------------------
    max_items: int = Field(default=100_000)
    batch_size: int = Field(default=64)

    # ------------------
    # Pydantic settings config
    # ------------------
    model_config = SettingsConfigDict(
        env_prefix="POC_",     # e.g. POC_RMQ_HOST
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
