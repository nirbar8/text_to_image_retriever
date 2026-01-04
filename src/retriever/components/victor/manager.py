from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from retriever.adapters.message_bus_rmq import RabbitMQMessageBus, RmqConfig
from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.components.victor.settings import VictorSettings
from retriever.core.interfaces import MessageBus, TilesRepository
from retriever.core.schemas import IndexRequest


@dataclass
class VectorManager:
    bus: MessageBus
    tiles_repo: TilesRepository

    def ingest_manifest(self, manifest_path: Path, queue_name: str) -> int:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        run_id = self._new_run_id()
        published = 0
        tiles: List[dict] = []

        for line in manifest_path.read_text().splitlines():
            msg = json.loads(line)
            msg["run_id"] = run_id
            req = IndexRequest(**msg)
            self.bus.publish(queue_name, req.model_dump())
            published += 1

            tiles.append(
                {
                    "tile_id": req.tile_id or f"tile:{req.image_id}",
                    "image_path": req.image_path,
                    "width": req.width,
                    "height": req.height,
                    "status": "queued",
                    "gid": req.gid,
                    "raster_path": req.raster_path,
                    "bbox_minx": req.bbox.minx if req.bbox else None,
                    "bbox_miny": req.bbox.miny if req.bbox else None,
                    "bbox_maxx": req.bbox.maxx if req.bbox else None,
                    "bbox_maxy": req.bbox.maxy if req.bbox else None,
                    "bbox_crs": req.bbox.crs if req.bbox else None,
                    "lat": req.lat,
                    "lon": req.lon,
                    "utm_zone": req.utm_zone,
                }
            )

        if tiles:
            self.tiles_repo.upsert_tiles(tiles)
        return published

    def mark_indexed(self, tile_ids: Iterable[str]) -> None:
        self.tiles_repo.update_status(list(tile_ids), status="indexed")

    def delete_tiles(self, tile_ids: Iterable[str]) -> None:
        self.tiles_repo.delete_tiles(list(tile_ids))

    def scaffold_ttl_cleanup(self) -> None:
        """Placeholder for TTL policies and cleanup scheduling."""
        return None

    @staticmethod
    def _new_run_id() -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{ts}_{uuid.uuid4().hex[:10]}"


def run() -> None:
    s = VictorSettings()
    bus = RabbitMQMessageBus(RmqConfig(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass))
    repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))
    manager = VectorManager(bus=bus, tiles_repo=repo)
    published = manager.ingest_manifest(s.tiles_manifest_path, queue_name=s.queue_name)
    print(f"Published {published} index requests to {s.queue_name}")


if __name__ == "__main__":
    run()
