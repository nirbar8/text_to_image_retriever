"""Repository protocol and base abstraction for tiles storage."""

from __future__ import annotations

from typing import List, Optional, Protocol, Sequence

from retriever.components.tiles_db.models import TileResponse, TileStatus


class TilesRepositoryService(Protocol):
    """Protocol defining the tiles repository interface.
    
    This abstract interface allows different storage backends (SQLite, PostgreSQL, etc.)
    to be plugged in without changing client code.
    """

    def create_tile(self, tile_data: dict) -> TileResponse:
        """Create or register a new tile.
        
        Args:
            tile_data: Dictionary containing tile metadata
            
        Returns:
            TileResponse with created tile details
        """
        ...

    def get_tile(self, tile_id: str) -> Optional[TileResponse]:
        """Retrieve a single tile by ID.
        
        Args:
            tile_id: Unique tile identifier
            
        Returns:
            TileResponse if found, None otherwise
        """
        ...

    def list_tiles(
        self,
        status: Optional[TileStatus] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[List[TileResponse], int]:
        """List tiles with optional filtering by status.
        
        Args:
            status: Filter by tile status
            limit: Maximum number of tiles to return
            offset: Number of tiles to skip
            
        Returns:
            Tuple of (tiles list, total count)
        """
        ...

    def update_tile(self, tile_id: str, update_data: dict) -> Optional[TileResponse]:
        """Update tile metadata.
        
        Args:
            tile_id: Unique tile identifier
            update_data: Dictionary of fields to update
            
        Returns:
            Updated TileResponse if found, None otherwise
        """
        ...

    def update_status(self, tile_id: str, status: TileStatus) -> Optional[TileResponse]:
        """Update a tile's status.
        
        Args:
            tile_id: Unique tile identifier
            status: New status value
            
        Returns:
            Updated TileResponse if found, None otherwise
        """
        ...

    def batch_update_status(
        self,
        tile_ids: Sequence[str],
        status: TileStatus,
    ) -> int:
        """Update status for multiple tiles.
        
        Args:
            tile_ids: List of tile IDs to update
            status: New status for all tiles
            
        Returns:
            Number of tiles actually updated
        """
        ...

    def delete_tile(self, tile_id: str) -> bool:
        """Delete a tile.
        
        Args:
            tile_id: Unique tile identifier
            
        Returns:
            True if tile was deleted, False if not found
        """
        ...

    def batch_delete(self, tile_ids: Sequence[str]) -> int:
        """Delete multiple tiles.
        
        Args:
            tile_ids: List of tile IDs to delete
            
        Returns:
            Number of tiles deleted
        """
        ...

    def status_counts(self) -> dict[str, int]:
        """Get count of tiles per status.
        
        Returns:
            Dictionary mapping status values to counts
        """
        ...

    def upsert_tiles(self, tiles: Sequence[dict]) -> None:
        """Insert or update multiple tiles at once.
        
        This is the legacy bulk operation for backward compatibility.
        
        Args:
            tiles: List of tile dictionaries
        """
        ...
