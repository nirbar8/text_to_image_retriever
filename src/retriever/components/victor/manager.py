from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from retriever.adapters.message_bus_rmq import RmqMessageBusFactory
from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.components.victor.settings import VictorSettings
from retriever.core.interfaces import MessageBus, TilesRepository
from retriever.core.schemas import IndexRequest, geo_to_columns, pixel_polygon_to_columns


@dataclass
class VectorManager:
    bus: MessageBus
    tiles_repo: TilesRepository

    def ingest_manifest(self, manifest_path: Path, queues: "EmbedderQueues") -> int:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        run_id = self._new_run_id()
        published = 0
        tiles: List[dict] = []

        for line in manifest_path.read_text().splitlines():
            msg = json.loads(line)
            msg["run_id"] = run_id
            req = IndexRequest(**msg)
            for queue in queues.for_request(req):
                self.bus.publish(queue, req.model_dump())
                published += 1

            tiles.append(
                {
                    "tile_id": req.tile_id or f"tile:{req.image_id}",
                    "image_path": req.image_path,
                    "width": req.width,
                    "height": req.height,
                    "status": "waiting for embedding",
                    "gid": req.gid,
                    "raster_path": req.raster_path,
                    "tile_store": req.tile_store,
                    "source": req.source,
                    **pixel_polygon_to_columns(req),
                    **geo_to_columns(req),
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


@dataclass(frozen=True)
class EmbedderQueues:
    default_queue: str
    by_backend: Dict[str, str]
    by_backend_model: Dict[Tuple[str, str], str]
    all_queues: List[str]

    def for_request(self, req: IndexRequest) -> List[str]:
        backend = (req.embedder_backend or "").strip()
        model = (req.embedder_model or "").strip()
        if not backend:
            return list(self.all_queues)
        if model:
            queue = self.by_backend_model.get((backend, model))
            if queue:
                return [queue]
        if backend in self.by_backend:
            return [self.by_backend[backend]]
        raise ValueError(
            "No queue mapping found for embedder backend "
            f"'{backend}' (model='{model}'). Configure VICTOR_EMBEDDER_QUEUES."
        )


def _parse_embedder_queues(raw: str) -> EmbedderQueues:
    cleaned = [part.strip() for part in raw.split(",") if part.strip()]
    if not cleaned:
        raise ValueError("No queue names configured. Set VICTOR_EMBEDDER_QUEUES.")
    by_backend: Dict[str, str] = {}
    by_backend_model: Dict[Tuple[str, str], str] = {}
    all_queues: List[str] = []

    for entry in cleaned:
        if "=" not in entry:
            raise ValueError(
                "Invalid VICTOR_EMBEDDER_QUEUES entry "
                f"'{entry}'. Use 'backend=queue' or 'backend:model=queue'."
            )
        key, queue = entry.split("=", 1)
        key = key.strip()
        queue = queue.strip()
        if not key or not queue:
            raise ValueError(
                "Invalid VICTOR_EMBEDDER_QUEUES entry "
                f"'{entry}'. Use 'backend=queue' or 'backend:model=queue'."
            )
        if ":" in key:
            backend, model = (part.strip() for part in key.split(":", 1))
            by_backend_model[(backend, model)] = queue
        else:
            by_backend[key] = queue
        if queue not in all_queues:
            all_queues.append(queue)

    return EmbedderQueues(
        default_queue=all_queues[0],
        by_backend=by_backend,
        by_backend_model=by_backend_model,
        all_queues=all_queues,
    )


def run() -> None:
    s = VictorSettings()
    bus = RmqMessageBusFactory().create(RmqConfig(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass))
    repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))
    manager = VectorManager(bus=bus, tiles_repo=repo)
    queues = _parse_embedder_queues(s.embedder_queues)
    published = manager.ingest_manifest(s.tiles_manifest_path, queues=queues)
    print(f"Published {published} index requests to {', '.join(queues.all_queues)}")


if __name__ == "__main__":
    run()
