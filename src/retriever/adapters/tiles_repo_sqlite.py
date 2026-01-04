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
                gid INTEGER,
                raster_path TEXT,
                bbox_minx REAL,
                bbox_miny REAL,
                bbox_maxx REAL,
                bbox_maxy REAL,
                bbox_crs TEXT,
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
            INSERT INTO tiles (
                tile_id, image_path, width, height, status, gid, raster_path,
                bbox_minx, bbox_miny, bbox_maxx, bbox_maxy, bbox_crs,
                lat, lon, utm_zone
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tile_id) DO UPDATE SET
                image_path=excluded.image_path,
                width=excluded.width,
                height=excluded.height,
                status=excluded.status,
                gid=excluded.gid,
                raster_path=excluded.raster_path,
                bbox_minx=excluded.bbox_minx,
                bbox_miny=excluded.bbox_miny,
                bbox_maxx=excluded.bbox_maxx,
                bbox_maxy=excluded.bbox_maxy,
                bbox_crs=excluded.bbox_crs,
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
                    t.get("gid"),
                    t.get("raster_path"),
                    t.get("bbox_minx"),
                    t.get("bbox_miny"),
                    t.get("bbox_maxx"),
                    t.get("bbox_maxy"),
                    t.get("bbox_crs"),
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
                """
                SELECT tile_id, image_path, width, height, status, gid, raster_path,
                       bbox_minx, bbox_miny, bbox_maxx, bbox_maxy, bbox_crs,
                       lat, lon, utm_zone
                FROM tiles WHERE status = ? LIMIT ?
                """,
                (status, limit),
            )
        else:
            cur.execute(
                """
                SELECT tile_id, image_path, width, height, status, gid, raster_path,
                       bbox_minx, bbox_miny, bbox_maxx, bbox_maxy, bbox_crs,
                       lat, lon, utm_zone
                FROM tiles LIMIT ?
                """,
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
                "gid": r[5],
                "raster_path": r[6],
                "bbox_minx": r[7],
                "bbox_miny": r[8],
                "bbox_maxx": r[9],
                "bbox_maxy": r[10],
                "bbox_crs": r[11],
                "lat": r[12],
                "lon": r[13],
                "utm_zone": r[14],
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
