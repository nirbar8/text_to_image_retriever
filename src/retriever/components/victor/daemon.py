from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

from retriever.adapters.message_bus_rmq import RmqMessageBusFactory
from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.clients.vectordb import VectorDBClient
from retriever.components.victor.manager import EmbedderQueues, VectorManager, _parse_embedder_queues
from retriever.components.victor.settings import VictorSettings
from retriever.core.interfaces import MessageBus, TilesRepository
from retriever.core.schemas import IndexRequest


def _parse_tables(raw: str) -> Optional[List[str]]:
    cleaned = [part.strip() for part in raw.split(",") if part.strip()]
    if not cleaned:
        return None
    return cleaned


def _build_request(tile: dict, run_id: str) -> Optional[IndexRequest]:
    image_id = tile.get("image_id")
    width = tile.get("width")
    height = tile.get("height")
    if image_id is None or width is None or height is None:
        print(
            "[warn] skipping ready tile with missing image_id/width/height "
            f"(tile_id={tile.get('tile_id')})"
        )
        return None
    return IndexRequest(
        image_id=int(image_id),
        image_path=tile.get("image_path"),
        width=int(width),
        height=int(height),
        tile_id=tile.get("tile_id"),
        gid=tile.get("gid"),
        raster_path=tile.get("raster_path"),
        pixel_polygon=tile.get("pixel_polygon"),
        geo_polygon=tile.get("geo_polygon"),
        lat=tile.get("lat"),
        lon=tile.get("lon"),
        utm_zone=tile.get("utm_zone"),
        tile_store=tile.get("tile_store"),
        source=tile.get("source"),
        run_id=run_id,
    )


@dataclass
class VictorDaemon:
    bus: MessageBus
    tiles_repo: TilesRepository
    vectordb: VectorDBClient
    queues: EmbedderQueues

    def publish_ready_tiles(self, ready_status: str, limit: int) -> int:
        tiles = self.tiles_repo.list_tiles(limit=limit, status=ready_status)
        if not tiles:
            return 0

        run_id = VectorManager._new_run_id()
        published = 0
        tile_ids: List[str] = []

        for tile in tiles:
            req = _build_request(tile, run_id=run_id)
            if not req:
                continue
            for queue in self.queues.for_request(req):
                self.bus.publish(queue, req.model_dump())
                published += 1
            if req.tile_id:
                tile_ids.append(req.tile_id)

        if tile_ids:
            self.tiles_repo.update_status(tile_ids, status="waiting for embedding")
        return published

    def cleanup_expired_tiles(
        self, ttl_s: int, limit: int, table_names: Optional[Iterable[str]] = None
    ) -> int:
        if ttl_s <= 0:
            return 0
        cutoff = int(time.time()) - ttl_s
        expired = self.tiles_repo.list_expired_tiles(cutoff_ts=cutoff, limit=limit)
        if not expired:
            return 0
        if table_names is None:
            table_names = self.vectordb.list_tables()
        for table_name in table_names:
            where = f"indexed_at <= {cutoff}"
            self.vectordb.delete_where(table_name, where=where)
        tile_ids = [tile["tile_id"] for tile in expired if tile.get("tile_id")]
        if tile_ids:
            self.tiles_repo.delete_tiles(tile_ids)
        return len(tile_ids)


def run() -> None:
    s = VictorSettings()
    bus_cfg = RmqConfig(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass)
    bus = RmqMessageBusFactory().create(bus_cfg)
    repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))
    vectordb = VectorDBClient(s.vectordb_url)
    queues = _parse_embedder_queues(s.embedder_queues)
    daemon = VictorDaemon(bus=bus, tiles_repo=repo, vectordb=vectordb, queues=queues)
    tables = _parse_tables(s.vectordb_tables)

    print(
        "Victor daemon started. "
        f"ready_status={s.ready_status}, interval={s.poll_interval_s}s, ttl_s={s.ttl_s}."
    )

    try:
        while True:
            try:
                published = daemon.publish_ready_tiles(
                    ready_status=s.ready_status, limit=s.ready_batch_limit
                )
                if published:
                    print(f"[ready] published {published} requests from status='{s.ready_status}'.")
                deleted = daemon.cleanup_expired_tiles(
                    ttl_s=s.ttl_s, limit=s.ttl_batch_limit, table_names=tables
                )
                if deleted:
                    print(f"[ttl] deleted {deleted} expired tiles.")
            except Exception as exc:
                print(f"[warn] victor daemon loop failed: {exc}")
            time.sleep(s.poll_interval_s)
    except KeyboardInterrupt:
        print("Victor daemon stopped.")


if __name__ == "__main__":
    run()
