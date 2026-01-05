from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from retriever.core.interfaces import TilesRepository
from retriever.core.schemas import TILE_DB_COLUMN_TYPES, TILE_DB_COLUMNS


@dataclass(frozen=True)
class SqliteTilesConfig:
    db_path: Path


class SqliteTilesRepository(TilesRepository):
    def __init__(self, cfg: SqliteTilesConfig):
        self._cfg = cfg
        self._cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._cfg.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
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

    def upsert_tiles(self, tiles: Sequence[dict]) -> None:
        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)
        placeholders = ", ".join(["?"] * len(TILE_DB_COLUMNS))
        update_cols = ", ".join(
            f"{col}=excluded.{col}" for col in TILE_DB_COLUMNS if col != "tile_id"
        )
        cur.executemany(
            f"""
            INSERT INTO tiles ({columns})
            VALUES ({placeholders})
            ON CONFLICT(tile_id) DO UPDATE SET
                {update_cols}
            """,
            [
                tuple(t.get(col) for col in TILE_DB_COLUMNS)
                for t in tiles
            ],
        )
        self._conn.commit()

    def list_tiles(self, limit: int = 1000, status: Optional[str] = None) -> List[dict]:
        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)
        if status:
            cur.execute(
                f"""
                SELECT {columns}
                FROM tiles WHERE status = ? LIMIT ?
                """,
                (status, limit),
            )
        else:
            cur.execute(
                f"""
                SELECT {columns}
                FROM tiles LIMIT ?
                """,
                (limit,),
            )
        rows = cur.fetchall()
        return [dict(zip(TILE_DB_COLUMNS, r)) for r in rows]

    def get_tile(self, tile_id: str) -> Optional[dict]:
        cur = self._conn.cursor()
        columns = ", ".join(TILE_DB_COLUMNS)
        cur.execute(
            f"""
            SELECT {columns}
            FROM tiles WHERE tile_id = ? LIMIT 1
            """,
            (tile_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(zip(TILE_DB_COLUMNS, row))

    def status_counts(self) -> dict[str, int]:
        cur = self._conn.cursor()
        cur.execute("SELECT status, COUNT(*) FROM tiles GROUP BY status")
        rows = cur.fetchall()
        return {row[0] or "": int(row[1]) for row in rows}

    def update_status(self, tile_ids: Sequence[str], status: str) -> None:
        cur = self._conn.cursor()
        cur.executemany(
            "UPDATE tiles SET status = ? WHERE tile_id = ?",
            [(status, tile_id) for tile_id in tile_ids],
        )
        self._conn.commit()

    def delete_tiles(self, tile_ids: Sequence[str]) -> None:
        cur = self._conn.cursor()
        cur.executemany("DELETE FROM tiles WHERE tile_id = ?", [(tile_id,) for tile_id in tile_ids])
        self._conn.commit()
