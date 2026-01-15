from __future__ import annotations

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from retriever.components.tyler.settings.coco_settings import CocoSettings
from retriever.components.tyler.settings.dota_settings import DotaSettings
from retriever.components.tyler.settings.orthophoto_settings import OrthophotoSettings
from retriever.components.tyler.settings.satellite_settings import SatelliteSettings
from retriever.components.tyler.settings.strip_settings import StripSettings
from retriever.components.tyler.settings.tyler_mode import TylerMode


class TylerSettings(BaseSettings):
    mode: TylerMode = Field(default=TylerMode.ORTHOPHOTO)
    orthophoto: OrthophotoSettings = Field(default_factory=OrthophotoSettings)
    satellite: SatelliteSettings = Field(default_factory=SatelliteSettings)
    coco: CocoSettings = Field(default_factory=CocoSettings)
    dota: DotaSettings = Field(default_factory=DotaSettings)
    strip: StripSettings = Field(default_factory=StripSettings)
    output_jsonl: Path = Field(default=Path("data/tiles.jsonl"))

    model_config = SettingsConfigDict(
        env_prefix="TYLER_",
        env_file="config/examples/.env.tyler",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
    )
