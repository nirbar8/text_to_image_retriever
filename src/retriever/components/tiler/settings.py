from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TilerSettings(BaseSettings):
    mode: str = Field(default="orthophoto")
    raster_path: Path = Field(default=Path("data/rasters/orthophoto.tif"))
    bounds_minx: float = Field(default=34.7)
    bounds_miny: float = Field(default=32.0)
    bounds_maxx: float = Field(default=34.9)
    bounds_maxy: float = Field(default=32.2)
    tile_size_px: int = Field(default=512)
    stride_px: int = Field(default=512)
    tile_size_deg: float = Field(default=0.01)
    output_jsonl: Path = Field(default=Path("data/tiles.jsonl"))

    model_config = SettingsConfigDict(
        env_prefix="TILER_",
        env_file=".env.tiler",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
