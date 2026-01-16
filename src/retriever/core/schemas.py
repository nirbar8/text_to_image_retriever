from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, Field

# Import tile DB schema from the service (single source of truth)
from retriever.components.tiles_db.sqlite_adapter import (
    TILE_DB_COLUMN_TYPES,
    TILE_DB_COLUMNS,
)

# Vector metadata columns - used for vector index metadata
TILE_GEO_COLUMNS = ("lon", "lat")
VECTOR_METADATA_COLUMNS = (
    "image_path",
    "tile_id",
    "gid",
    "sensor",
    "lon",
    "lat",
    "resolution",
    "tiles_size_meters",
    "request_time",
    "imaging_time",
    "embedder_model",
)
VECTOR_SCHEMA_COLUMNS = ("id", *VECTOR_METADATA_COLUMNS)


@dataclass(frozen=True)
class TileSpec:
    """Unified tile specification model used across all tyler implementations."""
    image_id: int
    tile_id: str
    width: int
    height: int
    tyler_mode: str
    image_path: Optional[str] = None
    raster_path: Optional[str] = None
    gid: Optional[int] = None
    pixel_polygon: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    utm_zone: Optional[str] = None
    width_m: Optional[float] = None
    height_m: Optional[float] = None


class IndexRequest(BaseModel):
    image_id: int
    image_path: Optional[str] = None
    width: int
    height: int
    tile_id: Optional[str] = None
    gid: Optional[int] = None
    raster_path: Optional[str] = None
    pixel_polygon: Optional[str] = None
    bands: Optional[Sequence[int]] = None
    out_width: Optional[int] = None
    out_height: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    utm_zone: Optional[str] = None
    run_id: Optional[str] = None
    tile_store: Optional[str] = None
    source: Optional[str] = None
    embedder_backend: Optional[str] = None
    embedder_model: Optional[str] = None

def pixel_polygon_to_columns(req: IndexRequest) -> Dict[str, Optional[str]]:
    return {"pixel_polygon": req.pixel_polygon}


def geo_to_columns(req: IndexRequest) -> Dict[str, Optional[float | str]]:
    return {"lat": req.lat, "lon": req.lon, "utm_zone": req.utm_zone}


class VectorUpsertRequest(BaseModel):
    rows: List[Dict[str, Any]]


class VectorUpsertResponse(BaseModel):
    inserted: int


class VectorQueryRequest(BaseModel):
    query_vector: Sequence[float]
    k: int = 10
    where: Optional[str] = None
    columns: Optional[Sequence[str]] = None


class VectorQueryResponse(BaseModel):
    results: List[Dict[str, Any]]


class TableInfoResponse(BaseModel):
    db_dir: str
    table_name: str
    vector_col: str
    vector_dim: Optional[int]
    version: Optional[int]
    row_count: Optional[int]
    columns: List[str]


class SampleRowsRequest(BaseModel):
    where: Optional[str] = None
    limit: int = 10
    columns: Optional[Sequence[str]] = None


class DeleteRowsRequest(BaseModel):
    where: str


class DeleteRowsResponse(BaseModel):
    rows_before: Optional[int]
    rows_after: Optional[int]


class ExportRowsRequest(BaseModel):
    where: Optional[str] = None
    page_size: int = 5000
    max_rows: Optional[int] = None
    columns: Optional[Sequence[str]] = None
    out_path: str


class ExportRowsResponse(BaseModel):
    written: int
    out_path: str


class HealthResponse(BaseModel):
    status: str = Field(default="ok")


class RetrieverSearchRequest(BaseModel):
    query_text: str
    table_name: str
    k: int = 10
    where: Optional[str] = None
    columns: Optional[Sequence[str]] = None
    apply_geo_nms: bool = False
    geo_nms_radius_m: Optional[float] = None


class RetrieverSearchResponse(BaseModel):
    results: List[Dict[str, Any]]
