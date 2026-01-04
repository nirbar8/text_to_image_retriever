from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tqdm import tqdm

import httpx

from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.adapters.message_bus_rmq import RmqMessageBusFactory
from retriever.adapters.message_bus_rmq_config import RmqConfig
from retriever.adapters.embedder_factory import build_embedder
from retriever.adapters.tile_store import (
    LocalFileTileStore,
    OrthophotoTileStore,
    SyntheticSatelliteTileStore,
)
from retriever.clients.vectordb import VectorDBClient
from retriever.components.embedder_worker.settings import EmbedderSettings
from retriever.core.interfaces import MessageBus, TileStore, TilesRepository
from retriever.core.schemas import IndexRequest


def _tile_id_for_req(req: IndexRequest) -> str:
    return req.tile_id or f"tile:{req.image_id}"


def _safe_update_status(repo: Optional[TilesRepository], tile_ids: Sequence[str], status: str) -> bool:
    if not repo or not tile_ids:
        return True
    try:
        repo.update_status(tile_ids, status=status)
        return True
    except Exception as exc:
        # status updates should never crash the worker
        print(f"[warn] tiles db status update failed ({status}) for {len(tile_ids)} tiles: {exc}")
        return False


def _sanitize_token(value: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in value)


def _cache_tile_path(req: IndexRequest, cache_dir: Path, cache_format: str) -> Path:
    token = req.tile_id or f"image_{req.image_id}"
    name = _sanitize_token(token)
    return cache_dir / f"{name}.{cache_format}"


def _load_tile(
    tile_store: TileStore,
    req: IndexRequest,
    cache_dir: Optional[Path],
    cache_format: str,
) -> Tuple[Any, Optional[str]]:
    """
    Load tile as PIL image (RGB) and optionally cache to disk.

    Returns: (image, resolved_image_path_or_None)
    """
    im = tile_store.get_tile_image(req).convert("RGB")
    resolved = req.image_path

    if resolved:
        return im, resolved

    if cache_dir is None:
        return im, None

    cache_path = _cache_tile_path(req, cache_dir, cache_format)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if not cache_path.exists():
            im.save(cache_path)
        resolved = str(cache_path)
    except Exception:
        resolved = None
    return im, resolved


def _resolve_table_name(settings: EmbedderSettings) -> str:
    if settings.table_name.strip():
        return settings.table_name
    return f"tiles_{settings.model_name.lower().replace('-', '_')}"


def _make_tile_store(settings: EmbedderSettings) -> TileStore:
    store = settings.tile_store.lower()
    if store == "local":
        return LocalFileTileStore()
    if store == "synthetic":
        return SyntheticSatelliteTileStore()
    raster_path = settings.raster_path
    if not raster_path.exists():
        raise FileNotFoundError(
            f"Raster not found: {raster_path}. "
            f"Set EMBEDDER_RASTER_PATH or use EMBEDDER_TILE_STORE=synthetic."
        )
    return OrthophotoTileStore(default_raster_path=str(raster_path))


def _safe_ack(envelope: Any) -> None:
    try:
        envelope.ack()
    except Exception:
        # ack failures are non-fatal; message will be redelivered
        pass


def run() -> None:
    s = EmbedderSettings()

    bus_cfg = RmqConfig(
        s.rmq_host,
        s.rmq_port,
        s.rmq_user,
        s.rmq_pass,
        prefetch_count=s.rmq_prefetch_count,
        heartbeat_s=s.rmq_heartbeat_s,
        blocked_connection_timeout_s=s.rmq_blocked_connection_timeout_s,
        ack_debug=s.rmq_ack_debug,
    )
    bus: MessageBus = RmqMessageBusFactory().create(bus_cfg, style=s.rmq_consume_style)

    tile_store = _make_tile_store(s)
    vectordb = VectorDBClient(s.vectordb_url, timeout_s=s.vectordb_timeout_s)

    tiles_repo: Optional[TilesRepository] = None
    if s.update_tile_statuses:
        tiles_repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))
    if s.require_index_status_before_ack and not s.update_tile_statuses:
        raise ValueError("require_index_status_before_ack requires update_tile_statuses=true")

    model = build_embedder(
        s.embedder_backend,
        s.model_name,
        clip_pretrained=s.clip_pretrained,
        remote_clip_url=s.remote_clip_url,
        remote_clip_timeout_s=s.remote_clip_timeout_s,
        remote_clip_image_format=s.remote_clip_image_format,
    )
    device = model.device
    assert device is not None

    table_name = _resolve_table_name(s)

    # Pool for tile loading (I/O bound)
    executor = ThreadPoolExecutor(max_workers=s.decode_workers)

    pbar = tqdm(total=0, desc="indexed", unit="img")
    indexed_total = 0
    received_total = 0

    print(
        f"Embedder worker started on device={device}, queue={s.queue_name}, "
        f"tile_store={s.tile_store}, backend={s.embedder_backend}, "
        f"table={table_name}. Ctrl+C to stop."
    )

    # batch holds tuples of (IndexRequest, envelope)
    batch: List[Tuple[IndexRequest, Any]] = []
    last_batch_ts = 0.0

    def process_batch() -> None:
        nonlocal batch, indexed_total

        if not batch:
            return

        tile_ids = [_tile_id_for_req(req) for req, _envelope in batch]
        _safe_update_status(tiles_repo, tile_ids, status="waiting for embedding")

        # Submit tile loads in parallel for this batch
        futures: List[Tuple[IndexRequest, Any, Any]] = []
        for req, envelope in batch:
            fut = executor.submit(
                _load_tile,
                tile_store,
                req,
                s.tile_cache_dir if s.cache_tiles else None,
                s.tile_cache_format,
            )
            futures.append((req, envelope, fut))

        items: List[Tuple[IndexRequest, Any, Any, Optional[str]]] = []

        for req, envelope, fut in futures:
            tile_id = _tile_id_for_req(req)
            try:
                img, resolved_path = fut.result(timeout=s.job_timeout_s)
            except Exception as e:
                # Loading failed or timed out: mark failed and ack, we won't retry this one
                print(f"[warn] failed to load tile for image_id={req.image_id}: {e}")
                _safe_update_status(tiles_repo, [tile_id], status="failed")
                _safe_ack(envelope)
                continue

            items.append((req, envelope, img, resolved_path))

        # Clear the batch (we will rebuild based on which items succeeded)
        batch = []

        if not items:
            return

        # Embed all images in one go
        images = [img for (_req, _env, img, _path) in items]
        try:
            embeddings = model.embed_pil_images(images).numpy()
        except Exception as e:
            # Embedding failed: don't ack anything, messages will be redelivered later
            print(f"[warn] embedding batch of {len(images)} images failed: {e}")
            return

        rows: List[Dict[str, Any]] = []
        envelopes_to_ack: List[Any] = []
        tile_ids_to_index: List[str] = []

        for (req, envelope, _img, resolved_path), emb in zip(items, embeddings):
            row = {
                "id": str(req.image_id),
                "embedding": emb.tolist(),
                "image_path": resolved_path or "",
                "image_id": int(req.image_id),
                "width": int(req.width),
                "height": int(req.height),
                "tile_id": req.tile_id,
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
                "run_id": req.run_id,
            }
            rows.append(row)
            envelopes_to_ack.append(envelope)
            tile_ids_to_index.append(_tile_id_for_req(req))

        # Mark as waiting for index
        _safe_update_status(tiles_repo, tile_ids_to_index, status="waiting for index")

        # Single upsert for the whole batch
        try:
            vectordb.upsert(table_name, rows)
        except httpx.HTTPError as e:
            print(f"[warn] vectordb HTTP error on upsert of {len(rows)} rows: {e}")
            # Leave messages unacked -> redelivery later
            return
        except Exception as e:
            print(f"[warn] vectordb upsert failed for {len(rows)} rows: {e}")
            # Leave messages unacked -> redelivery later
            return

        # Upsert succeeded: mark indexed and ack
        status_ok = _safe_update_status(tiles_repo, tile_ids_to_index, status="indexed")
        if status_ok or not s.require_index_status_before_ack:
            for envelope in envelopes_to_ack:
                _safe_ack(envelope)
        else:
            print(
                "[warn] tiles db status update failed after vectordb upsert; "
                "leaving messages unacked for retry."
            )

        indexed_total += len(rows)
        pbar.update(len(rows))

    try:
        while True:
            try:
                for envelope in bus.consume(s.queue_name):
                    if envelope is None:
                        now = time.time()
                        if batch and (now - last_batch_ts) >= s.flush_interval_s:
                            process_batch()
                            last_batch_ts = time.time()
                        continue
                    payload = envelope.payload
                    req = IndexRequest(**payload)
                    received_total += 1

                    if received_total == 1 or received_total % s.recv_log_every == 0:
                        print(f"[recv] received={received_total} image_id={int(req.image_id)}")

                    # Add to batch
                    batch.append((req, envelope))
                    if len(batch) == 1:
                        last_batch_ts = time.time()

                    now = time.time()
                    # Flush by size OR by time
                    if len(batch) >= s.batch_size or (now - last_batch_ts) >= s.flush_interval_s:
                        process_batch()
                        last_batch_ts = time.time()

                now = time.time()
                if batch and (now - last_batch_ts) >= s.flush_interval_s:
                    process_batch()
                    last_batch_ts = time.time()

            except Exception as exc:
                print(f"[warn] message bus consume failed; retrying in {s.rmq_retry_s}s: {exc}")
                # Drop local batch. All unacked messages will be requeued by RabbitMQ.
                batch = []
                time.sleep(s.rmq_retry_s)
                continue
    except KeyboardInterrupt:
        print("Stopping. Processing last batch before exit...")
        try:
            process_batch()
        except Exception as e:
            print(f"[warn] final batch processing failed: {e}")
    finally:
        executor.shutdown(wait=True)
        pbar.close()
        print(f"Done. Total indexed this run: {indexed_total}")


if __name__ == "__main__":
    run()
