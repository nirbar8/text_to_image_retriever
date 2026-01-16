"""Pydantic models and schemas for TilesDB service."""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class TileStatus(str, Enum):
    """Tile processing status enum."""

    READY_FOR_INDEXING = "ready_for_indexing"
    IN_PROCESS = "in_process"
    INDEXED = "indexed"
    FAILED = "failed"


class TileCreateRequest(BaseModel):
    """Request model for creating/registering a tile."""

    tile_id: str = Field(..., description="Unique tile identifier")
    gid: Optional[str] = Field(default=None, description="Geographic ID")
    sensor: Optional[str] = Field(default=None, description="Sensor type (e.g., 'Sentinel-2')")
    lon: Optional[float] = Field(default=None, description="Longitude coordinate")
    lat: Optional[float] = Field(default=None, description="Latitude coordinate")
    resolution: Optional[float] = Field(default=None, description="Resolution in meters")
    tiles_size_meters: Optional[float] = Field(default=None, description="Tile size in meters")
    image_path: Optional[str] = Field(default=None, description="Path to tile image")
    request_time: Optional[str] = Field(default=None, description="Time request was made (ISO 8601)")
    imaging_time: Optional[str] = Field(default=None, description="Time image was captured (ISO 8601)")
    embedder_model: Optional[str] = Field(default=None, description="Embedder model to use")


class TileUpdateRequest(BaseModel):
    """Request model for updating tile metadata."""

    gid: Optional[str] = None
    sensor: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None
    resolution: Optional[float] = None
    tiles_size_meters: Optional[float] = None
    image_path: Optional[str] = None
    request_time: Optional[str] = None
    imaging_time: Optional[str] = None
    embedder_model: Optional[str] = None


class TileStatusUpdateRequest(BaseModel):
    """Request model for updating tile status."""

    status: TileStatus = Field(..., description="New tile status")


class BatchStatusUpdateRequest(BaseModel):
    """Request model for batch status updates."""

    tile_ids: List[str] = Field(..., description="List of tile IDs to update")
    status: TileStatus = Field(..., description="New status for all tiles")


class TileResponse(BaseModel):
    """Response model for a single tile."""

    tile_id: str
    gid: Optional[str] = None
    sensor: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None
    resolution: Optional[float] = None
    tiles_size_meters: Optional[float] = None
    image_path: Optional[str] = None
    request_time: Optional[str] = None
    imaging_time: Optional[str] = None
    status: TileStatus
    embedder_model: Optional[str] = None


class TilesListResponse(BaseModel):
    """Response model for list of tiles."""

    tiles: List[TileResponse]
    total: int = Field(..., description="Total number of tiles matching filter")
    limit: int = Field(..., description="Limit applied to query")
    offset: int = Field(..., description="Offset applied to query")


class StatusCountsResponse(BaseModel):
    """Response model for tile counts by status."""

    counts: dict[str, int] = Field(..., description="Count of tiles per status")
    total: int = Field(..., description="Total number of tiles")


class BatchOperationResponse(BaseModel):
    """Response model for batch operations."""

    affected: int = Field(..., description="Number of tiles affected")
    operation: str = Field(..., description="Type of operation performed")


class TileBatch(BaseModel):
    """Request model for batch tile creation."""

    tiles: List[TileCreateRequest] = Field(..., description="List of tiles to create")
