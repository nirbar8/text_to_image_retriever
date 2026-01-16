"""HTTP client for interacting with TilesDB service."""

from __future__ import annotations

from typing import List, Optional

import httpx

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


class TilesDBClient:
    """HTTP client for TilesDB service.
    
    This client abstracts HTTP communication with the TilesDB service,
    allowing other components to interact with tiles without direct database access.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8001"):
        """Initialize TilesDB client.

        Args:
            base_url: Base URL of TilesDB service
        """
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=30.0)

    def __enter__(self) -> TilesDBClient:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self._client.close()

    def create_tile(self, tile_data: TileCreateRequest) -> TileResponse:
        """Create a new tile.

        Args:
            tile_data: Tile creation request

        Returns:
            Created tile response

        Raises:
            httpx.HTTPError: If request fails
        """
        response = self._client.post(
            f"{self.base_url}/tiles",
            json=tile_data.model_dump(exclude_none=True),
        )
        response.raise_for_status()
        return TileResponse(**response.json())

    def get_tile(self, tile_id: str) -> Optional[TileResponse]:
        """Get a tile by ID.

        Args:
            tile_id: Tile ID

        Returns:
            Tile response or None if not found
        """
        try:
            response = self._client.get(f"{self.base_url}/tiles/{tile_id}")
            response.raise_for_status()
            return TileResponse(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def list_tiles(
        self,
        status: Optional[TileStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> TilesListResponse:
        """List tiles with optional filtering.

        Args:
            status: Filter by status
            limit: Maximum number of tiles
            offset: Number of tiles to skip

        Returns:
            List response with tiles and metadata
        """
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status.value

        response = self._client.get(f"{self.base_url}/tiles", params=params)
        response.raise_for_status()
        return TilesListResponse(**response.json())

    def update_tile(
        self,
        tile_id: str,
        update_data: TileUpdateRequest,
    ) -> TileResponse:
        """Update tile metadata.

        Args:
            tile_id: Tile ID
            update_data: Update request

        Returns:
            Updated tile response
        """
        response = self._client.patch(
            f"{self.base_url}/tiles/{tile_id}",
            json=update_data.model_dump(exclude_none=True),
        )
        response.raise_for_status()
        return TileResponse(**response.json())

    def update_status(
        self,
        tile_id: str,
        status: TileStatus,
    ) -> TileResponse:
        """Update tile status.

        Args:
            tile_id: Tile ID
            status: New status

        Returns:
            Updated tile response
        """
        response = self._client.post(
            f"{self.base_url}/tiles/{tile_id}/status",
            json={"status": status.value},
        )
        response.raise_for_status()
        return TileResponse(**response.json())

    def batch_create_tiles(self, tile_requests: List[TileCreateRequest]) -> TilesListResponse:
        """Create multiple tiles.

        Args:
            tile_requests: List of tile creation requests

        Returns:
            List response with created tiles
        """
        batch_data = {
            "tiles": [t.model_dump(exclude_none=True) for t in tile_requests]
        }
        response = self._client.post(
            f"{self.base_url}/tiles/batch",
            json=batch_data,
        )
        response.raise_for_status()
        return TilesListResponse(**response.json())

    def batch_update_status(
        self,
        tile_ids: List[str],
        status: TileStatus,
    ) -> BatchOperationResponse:
        """Update status for multiple tiles.

        Args:
            tile_ids: List of tile IDs
            status: New status for all

        Returns:
            Operation response with count of affected tiles
        """
        response = self._client.post(
            f"{self.base_url}/tiles/batch/status",
            json={"tile_ids": tile_ids, "status": status.value},
        )
        response.raise_for_status()
        return BatchOperationResponse(**response.json())

    def batch_delete(self, tile_ids: List[str]) -> BatchOperationResponse:
        """Delete multiple tiles.

        Args:
            tile_ids: List of tile IDs to delete

        Returns:
            Operation response with count of deleted tiles
        """
        response = self._client.delete(
            f"{self.base_url}/tiles/batch",
            params={"tile_ids": tile_ids},
        )
        response.raise_for_status()
        return BatchOperationResponse(**response.json())

    def delete_tile(self, tile_id: str) -> BatchOperationResponse:
        """Delete a single tile.

        Args:
            tile_id: Tile ID to delete

        Returns:
            Operation response
        """
        response = self._client.delete(f"{self.base_url}/tiles/{tile_id}")
        response.raise_for_status()
        return BatchOperationResponse(**response.json())

    def status_counts(self) -> StatusCountsResponse:
        """Get count of tiles per status.

        Returns:
            Status counts response
        """
        response = self._client.get(f"{self.base_url}/tiles/status/counts")
        response.raise_for_status()
        return StatusCountsResponse(**response.json())

    def health(self) -> bool:
        """Check if service is healthy.

        Returns:
            True if service is running and healthy
        """
        try:
            response = self._client.get(f"{self.base_url}/health")
            return response.status_code == 200
        except (httpx.RequestError, httpx.HTTPError):
            return False
