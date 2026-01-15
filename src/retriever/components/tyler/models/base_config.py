from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BaseTylerConfig:
    """Base configuration class with common fields for all tyler implementations."""
    source_name: str
    output_crs: str = "EPSG:4326"
