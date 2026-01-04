from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, Field


class IndexRequest(BaseModel):
    image_id: int
    image_path: str
    width: int
    height: int
    coco_file_name: Optional[str] = None
    tile_id: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    utm_zone: Optional[str] = None
    run_id: Optional[str] = None


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
