"""FastAPI service for TilesDB."""

from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from pydantic import ValidationError

from retriever.components.tiles_db.models import (
    BatchOperationResponse,
    BatchStatusUpdateRequest,
    StatusCountsResponse,
    TileBatch,
    TileCreateRequest,
    TileResponse,
    TileStatus,
    TileStatusUpdateRequest,
    TilesListResponse,
    TileUpdateRequest,
)
from retriever.components.tiles_db.repository import TilesRepositoryService
from retriever.components.tiles_db.settings import TilesDBSettings
from retriever.components.tiles_db.sqlite_adapter import (
    SqliteTilesConfig,
    SqliteTilesRepositoryService,
)


def create_app(settings: Optional[TilesDBSettings] = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: TilesDB settings. If None, will be loaded from environment.

    Returns:
        Configured FastAPI application.
    """
    if settings is None:
        settings = TilesDBSettings()

    app = FastAPI(
        title="TilesDB Service",
        description="Central tile metadata and status management service",
        version="1.0.0",
    )

    # Store settings in app state
    app.state.settings = settings

    def get_repository() -> TilesRepositoryService:
        """Dependency to get a repository instance per request.
        
        This creates a new repository (and SQLite connection) for each request,
        avoiding thread-safety issues with SQLite.
        """
        return SqliteTilesRepositoryService(
            SqliteTilesConfig(settings.database.db_path)
        )

    # Create router
    router = APIRouter(prefix="/tiles", tags=["tiles"])

    # ============================================================================
    # GET endpoints
    # ============================================================================

    @router.get(
        "",
        response_model=TilesListResponse,
        summary="List tiles",
        description="Retrieve a paginated list of tiles with optional status filtering",
    )
    def list_tiles(
        status: Optional[TileStatus] = Query(
            None,
            description="Filter by tile status",
        ),
        limit: int = Query(
            100,
            ge=1,
            le=10000,
            description="Maximum number of tiles to return",
        ),
        offset: int = Query(
            0,
            ge=0,
            description="Number of tiles to skip",
        ),
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TilesListResponse:
        """List tiles with pagination and optional status filtering."""
        tiles, total = repo.list_tiles(status=status, limit=limit, offset=offset)
        return TilesListResponse(
            tiles=tiles,
            total=total,
            limit=limit,
            offset=offset,
        )

    @router.get(
        "/{tile_id}",
        response_model=TileResponse,
        summary="Get tile by ID",
        description="Retrieve a single tile by its ID",
    )
    def get_tile(
        tile_id: str,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TileResponse:
        """Get a single tile by ID."""
        tile = repo.get_tile(tile_id)
        if tile is None:
            raise HTTPException(status_code=404, detail=f"Tile not found: {tile_id}")
        return tile

    @router.get(
        "/status/counts",
        response_model=StatusCountsResponse,
        summary="Get status counts",
        description="Get count of tiles for each status",
    )
    def status_counts(
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> StatusCountsResponse:
        """Get count of tiles per status."""
        counts = repo.status_counts()
        total = sum(counts.values())
        return StatusCountsResponse(counts=counts, total=total)

    # ============================================================================
    # POST endpoints
    # ============================================================================

    @router.post(
        "",
        response_model=TileResponse,
        status_code=201,
        summary="Create tile",
        description="Register a new tile",
    )
    def create_tile(
        req: TileCreateRequest,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TileResponse:
        """Create a new tile."""
        try:
            return repo.create_tile(req.model_dump(exclude_none=True))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.post(
        "/batch",
        response_model=TilesListResponse,
        status_code=201,
        summary="Batch create tiles",
        description="Register multiple tiles at once",
    )
    def batch_create_tiles(
        req: TileBatch,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TilesListResponse:
        """Batch create tiles."""
        try:
            created_tiles = []
            for tile_data in req.tiles:
                tile = repo.create_tile(tile_data.model_dump(exclude_none=True))
                created_tiles.append(tile)
            return TilesListResponse(
                tiles=created_tiles,
                total=len(created_tiles),
                limit=len(created_tiles),
                offset=0,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.post(
        "/{tile_id}/status",
        response_model=TileResponse,
        summary="Update tile status",
        description="Change the status of a single tile",
    )
    def update_tile_status(
        tile_id: str,
        req: TileStatusUpdateRequest,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TileResponse:
        """Update tile status."""
        tile = repo.update_status(tile_id, req.status)
        if tile is None:
            raise HTTPException(status_code=404, detail=f"Tile not found: {tile_id}")
        return tile

    @router.post(
        "/batch/status",
        response_model=BatchOperationResponse,
        summary="Batch update status",
        description="Update status for multiple tiles",
    )
    def batch_update_status(
        req: BatchStatusUpdateRequest,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> BatchOperationResponse:
        """Batch update tile statuses."""
        affected = repo.batch_update_status(req.tile_ids, req.status)
        return BatchOperationResponse(
            affected=affected,
            operation="batch_update_status",
        )

    # ============================================================================
    # PATCH endpoint
    # ============================================================================

    @router.patch(
        "/{tile_id}",
        response_model=TileResponse,
        summary="Update tile metadata",
        description="Update tile metadata without changing status",
    )
    def update_tile(
        tile_id: str,
        req: TileUpdateRequest,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> TileResponse:
        """Update tile metadata."""
        update_data = req.model_dump(exclude_none=True)
        if not update_data:
            # If no fields to update, just return existing tile
            tile = repo.get_tile(tile_id)
            if tile is None:
                raise HTTPException(status_code=404, detail=f"Tile not found: {tile_id}")
            return tile

        tile = repo.update_tile(tile_id, update_data)
        if tile is None:
            raise HTTPException(status_code=404, detail=f"Tile not found: {tile_id}")
        return tile

    # ============================================================================
    # DELETE endpoints
    # ============================================================================

    @router.delete(
        "/{tile_id}",
        response_model=BatchOperationResponse,
        summary="Delete tile",
        description="Delete a single tile",
    )
    def delete_tile(
        tile_id: str,
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> BatchOperationResponse:
        """Delete a single tile."""
        deleted = repo.delete_tile(tile_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Tile not found: {tile_id}")
        return BatchOperationResponse(affected=1, operation="delete_tile")

    @router.delete(
        "/batch",
        response_model=BatchOperationResponse,
        summary="Batch delete tiles",
        description="Delete multiple tiles",
    )
    def batch_delete_tiles(
        tile_ids: list[str] = Query(...),
        repo: TilesRepositoryService = Depends(get_repository),
    ) -> BatchOperationResponse:
        """Batch delete tiles."""
        if not tile_ids:
            raise HTTPException(status_code=400, detail="tile_ids required")
        affected = repo.batch_delete(tile_ids)
        return BatchOperationResponse(affected=affected, operation="batch_delete")

    # ============================================================================
    # Health check
    # ============================================================================

    @app.get(
        "/health",
        summary="Health check",
        description="Check if service is running",
        tags=["health"],
    )
    def health() -> dict:
        """Health check endpoint."""
        return {"status": "ok"}

    # Include router
    app.include_router(router)

    return app
