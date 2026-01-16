"""SQLite implementation of TilesRepositoryService."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, get_args, get_origin

from retriever.components.tiles_db.models import TileResponse, TileStatus
from retriever.components.tiles_db.repository import TilesRepositoryService


def _derive_sql_type(python_type) -> str:
    """Map Python/Pydantic type to SQLite type."""
    # Handle Optional types
    origin = get_origin(python_type)
    if origin is Optional or origin is type(Optional):
        args = get_args(python_type)
        if args:
            python_type = args[0]
    
    if python_type in (int, type(int)):
        return "INTEGER"
    elif python_type in (float, type(float)):
        return "REAL"
    elif python_type in (str, type(str)):
        return "TEXT"
    elif python_type in (bool, type(bool)):
        return "INTEGER"  # SQLite uses INTEGER for booleans
    else:
        return "TEXT"  # Default to TEXT


def _generate_tile_db_schema() -> tuple[tuple[str, ...], dict[str, str]]:
    """Generate TILE_DB_COLUMNS and TILE_DB_COLUMN_TYPES from TileResponse model.
    
    This function derives the database schema from the Pydantic model,
    ensuring they stay in sync automatically.
    
    Returns:
        Tuple of (column_names, column_types_dict)
    """
    # Get fields from TileResponse model
    fields = TileResponse.model_fields
    
    # Define field order explicitly to match the desired schema
    field_order = [
        "tile_id",
        "gid",
        "sensor",
        "lon",
        "lat",
        "resolution",
        "tiles_size_meters",
        "image_path",
        "request_time",
        "imaging_time",
        "status",
        "embedder_model",
    ]
    
    columns = []
    column_types = {}
    
    for field_name in field_order:
        if field_name not in fields:
            continue
        
        field_info = fields[field_name]
        columns.append(field_name)
        
        # Determine SQL type
        if field_name == "tile_id":
            sql_type = "TEXT PRIMARY KEY"
        else:
            sql_type = _derive_sql_type(field_info.annotation)
        
        column_types[field_name] = sql_type
    
    return tuple(columns), column_types


# Generate schema at module load time
TILE_DB_COLUMNS, TILE_DB_COLUMN_TYPES = _generate_tile_db_schema()


@dataclass(frozen=True)
class SqliteTilesConfig:
    """Configuration for SQLite tiles repository."""

    db_path: Path


class SqliteTilesRepositoryService(TilesRepositoryService):
    """SQLite implementation of the tiles repository.
    
    This adapter stores tile metadata in an SQLite database.
    The schema can be evolved independently without affecting service clients.
    """

    def __init__(self, cfg: SqliteTilesConfig):
        self._cfg = cfg
        self._cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._cfg.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize or migrate the database schema."""
        cur = self._conn.cursor()
        columns_sql = ",\n                ".join(
            f"{name} {col_type}" for name, col_type in TILE_DB_COLUMN_TYPES.items()
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS tiles (
                {columns_sql}
            )
            """
        )
        cur.execute("PRAGMA table_info(tiles)")
        existing = {row[1] for row in cur.fetchall()}
        for name, col_type in TILE_DB_COLUMN_TYPES.items():
            if name not in existing:
                cur.execute(f"ALTER TABLE tiles ADD COLUMN {name} {col_type}")
        self._conn.commit()

    def _row_to_response(self, row: dict) -> TileResponse:
        """Convert database row to TileResponse."""
        # Ensure status is converted to enum
        status = row.get("status")
        if status and isinstance(status, str):
            try:
                status = TileStatus(status)
            except ValueError:
                status = TileStatus.READY_FOR_INDEXING
        else:
            status = TileStatus.READY_FOR_INDEXING

        return TileResponse(
            tile_id=row["tile_id"],
            gid=row.get("gid"),
            sensor=row.get("sensor"),
            lon=row.get("lon"),
            lat=row.get("lat"),
            resolution=row.get("resolution"),
            tiles_size_meters=row.get("tiles_size_meters"),
            image_path=row.get("image_path"),
            request_time=row.get("request_time"),
            imaging_time=row.get("imaging_time"),
            status=status,
            embedder_model=row.get("embedder_model"),
        )

    def _fetch_row(self, tile_id: str) -> Optional[dict]:
        """Fetch a single tile row by ID."""
        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)
        cur.execute(
            f"SELECT {columns} FROM tiles WHERE tile_id = ? LIMIT 1",
            (tile_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip(TILE_DB_COLUMNS, row))

    def create_tile(self, tile_data: dict) -> TileResponse:
        """Create or upsert a tile."""
        tile_id = tile_data.get("tile_id")
        if not tile_id:
            raise ValueError("tile_id is required")

        # Set default status if not provided
        if "status" not in tile_data:
            tile_data["status"] = TileStatus.READY_FOR_INDEXING.value

        # Prepare insert data with all columns
        insert_data = {col: tile_data.get(col) for col in TILE_DB_COLUMNS}

        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)
        placeholders = ", ".join(["?"] * len(TILE_DB_COLUMNS))
        update_cols = ", ".join(
            f"{col}=excluded.{col}"
            for col in TILE_DB_COLUMNS
            if col != "tile_id"
        )
        values = tuple(insert_data[col] for col in TILE_DB_COLUMNS)

        cur.execute(
            f"""
            INSERT INTO tiles ({columns})
            VALUES ({placeholders})
            ON CONFLICT(tile_id) DO UPDATE SET
                {update_cols}
            """,
            values,
        )
        self._conn.commit()

        # Fetch and return created tile
        row = self._fetch_row(tile_id)
        if not row:
            raise RuntimeError(f"Failed to create or retrieve tile {tile_id}")
        return self._row_to_response(row)

    def get_tile(self, tile_id: str) -> Optional[TileResponse]:
        """Retrieve a tile by ID."""
        row = self._fetch_row(tile_id)
        if not row:
            return None
        return self._row_to_response(row)

    def list_tiles(
        self,
        status: Optional[TileStatus] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[List[TileResponse], int]:
        """List tiles with optional filtering and pagination."""
        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)

        # Get total count first
        if status:
            cur.execute(
                "SELECT COUNT(*) FROM tiles WHERE status = ?",
                (status.value,),
            )
        else:
            cur.execute("SELECT COUNT(*) FROM tiles")
        total = cur.fetchone()[0]

        # Fetch paginated results
        if status:
            cur.execute(
                f"""
                SELECT {columns}
                FROM tiles WHERE status = ?
                ORDER BY tile_id
                LIMIT ? OFFSET ?
                """,
                (status.value, limit, offset),
            )
        else:
            cur.execute(
                f"""
                SELECT {columns}
                FROM tiles
                ORDER BY tile_id
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )

        rows = cur.fetchall()
        tiles = [
            self._row_to_response(dict(zip(TILE_DB_COLUMNS, r)))
            for r in rows
        ]
        return tiles, total

    def update_tile(self, tile_id: str, update_data: dict) -> Optional[TileResponse]:
        """Update tile metadata."""
        # Validate tile exists
        if self.get_tile(tile_id) is None:
            return None

        cur = self._conn.cursor()
        # Only update fields that are in the provided data
        set_clauses = [f"{k}=?" for k in update_data.keys()]
        if not set_clauses:
            return self.get_tile(tile_id)

        cur.execute(
            f"UPDATE tiles SET {', '.join(set_clauses)} WHERE tile_id = ?",
            list(update_data.values()) + [tile_id],
        )
        self._conn.commit()

        return self.get_tile(tile_id)

    def update_status(self, tile_id: str, status: TileStatus) -> Optional[TileResponse]:
        """Update tile status."""
        return self.update_tile(tile_id, {"status": status.value})

    def batch_update_status(
        self,
        tile_ids: Sequence[str],
        status: TileStatus,
    ) -> int:
        """Batch update status for multiple tiles."""
        if not tile_ids:
            return 0

        cur = self._conn.cursor()
        placeholders = ",".join(["?"] * len(tile_ids))
        cur.execute(
            f"UPDATE tiles SET status = ? WHERE tile_id IN ({placeholders})",
            [status.value] + list(tile_ids),
        )
        self._conn.commit()
        return cur.rowcount

    def delete_tile(self, tile_id: str) -> bool:
        """Delete a single tile."""
        cur = self._conn.cursor()
        cur.execute("DELETE FROM tiles WHERE tile_id = ?", (tile_id,))
        self._conn.commit()
        return cur.rowcount > 0

    def batch_delete(self, tile_ids: Sequence[str]) -> int:
        """Batch delete multiple tiles."""
        if not tile_ids:
            return 0

        cur = self._conn.cursor()
        placeholders = ",".join(["?"] * len(tile_ids))
        cur.execute(f"DELETE FROM tiles WHERE tile_id IN ({placeholders})", tile_ids)
        self._conn.commit()
        return cur.rowcount

    def status_counts(self) -> dict[str, int]:
        """Get count of tiles per status."""
        cur = self._conn.cursor()
        cur.execute("SELECT status, COUNT(*) FROM tiles GROUP BY status")
        rows = cur.fetchall()
        return {row[0] or "": int(row[1]) for row in rows}

    def upsert_tiles(self, tiles: Sequence[dict]) -> None:
        """Bulk insert or update tiles (legacy operation)."""
        for tile in tiles:
            self.create_tile(tile)
