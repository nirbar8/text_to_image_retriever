from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, Field


TILE_PIXEL_COLUMNS = ("pixel_polygon",)
TILE_GEO_POLYGON_COLUMNS = ("geo_polygon",)
TILE_GEO_COLUMNS = ("lat", "lon", "utm_zone")
TILE_DB_COLUMNS = (
    "tile_id",
    "image_id",
    "source",
    "image_path",
    "width",
    "height",
    "status",
    "indexed_at",
    "gid",
    "raster_path",
    *TILE_PIXEL_COLUMNS,
    *TILE_GEO_POLYGON_COLUMNS,
    *TILE_GEO_COLUMNS,
    "tile_store",
)
TILE_DB_COLUMN_TYPES = {
    "tile_id": "TEXT PRIMARY KEY",
    "image_id": "INTEGER",
    "source": "TEXT",
    "image_path": "TEXT",
    "width": "INTEGER",
    "height": "INTEGER",
    "status": "TEXT",
    "indexed_at": "INTEGER",
    "gid": "INTEGER",
    "raster_path": "TEXT",
    "pixel_polygon": "TEXT",
    "geo_polygon": "TEXT",
    "lat": "REAL",
    "lon": "REAL",
    "utm_zone": "TEXT",
    "tile_store": "TEXT",
}
VECTOR_METADATA_COLUMNS = (
    *TILE_DB_COLUMNS,
    "run_id",
    "embedder_backend",
    "embedder_model",
)
VECTOR_SCHEMA_COLUMNS = ("id", *VECTOR_METADATA_COLUMNS)


class IndexRequest(BaseModel):
    image_id: int
    image_path: Optional[str] = None
    width: int
    height: int
    tile_id: Optional[str] = None
    gid: Optional[int] = None
    raster_path: Optional[str] = None
    pixel_polygon: Optional[str] = None
    geo_polygon: Optional[str] = None
    bands: Optional[Sequence[int]] = None
    out_width: Optional[int] = None
    out_height: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    utm_zone: Optional[str] = None
    run_id: Optional[str] = None
    tile_store: Optional[str] = None
    source: Optional[str] = None
    indexed_at: Optional[int] = None
    embedder_backend: Optional[str] = None
    embedder_model: Optional[str] = None

def pixel_polygon_to_columns(req: IndexRequest) -> Dict[str, Optional[str]]:
    return {"pixel_polygon": req.pixel_polygon}


def geo_polygon_to_columns(req: IndexRequest) -> Dict[str, Optional[str]]:
    return {"geo_polygon": req.geo_polygon}


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
    offset: int = 0
    from_end: bool = False
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


class OptimizeResponse(BaseModel):
    status: str
    details: Optional[Dict[str, Any]] = None


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
