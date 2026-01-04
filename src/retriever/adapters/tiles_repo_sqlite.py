from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from retriever.core.interfaces import TilesRepository


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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tiles (
                tile_id TEXT PRIMARY KEY,
                image_path TEXT,
                width INTEGER,
                height INTEGER,
                status TEXT,
                lat REAL,
                lon REAL,
                utm_zone TEXT
            )
            """
        )
        self._conn.commit()

    def upsert_tiles(self, tiles: Sequence[dict]) -> None:
        cur = self._conn.cursor()
        cur.executemany(
            """
            INSERT INTO tiles (tile_id, image_path, width, height, status, lat, lon, utm_zone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tile_id) DO UPDATE SET
                image_path=excluded.image_path,
                width=excluded.width,
                height=excluded.height,
                status=excluded.status,
                lat=excluded.lat,
                lon=excluded.lon,
                utm_zone=excluded.utm_zone
            """,
            [
                (
                    t.get("tile_id"),
                    t.get("image_path"),
                    t.get("width"),
                    t.get("height"),
                    t.get("status"),
                    t.get("lat"),
                    t.get("lon"),
                    t.get("utm_zone"),
                )
                for t in tiles
            ],
        )
        self._conn.commit()

    def list_tiles(self, limit: int = 1000, status: Optional[str] = None) -> List[dict]:
        cur = self._conn.cursor()
        if status:
            cur.execute(
                "SELECT tile_id, image_path, width, height, status, lat, lon, utm_zone FROM tiles WHERE status = ? LIMIT ?",
                (status, limit),
            )
        else:
            cur.execute(
                "SELECT tile_id, image_path, width, height, status, lat, lon, utm_zone FROM tiles LIMIT ?",
                (limit,),
            )
        rows = cur.fetchall()
        return [
            {
                "tile_id": r[0],
                "image_path": r[1],
                "width": r[2],
                "height": r[3],
                "status": r[4],
                "lat": r[5],
                "lon": r[6],
                "utm_zone": r[7],
            }
            for r in rows
        ]

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
