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
from retriever.core.schemas import IndexRequest, bbox_to_columns, geo_to_columns


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


def _resolve_table_name(settings: EmbedderSettings, model_name: str) -> str:
    if settings.table_name.strip():
        return settings.table_name
    safe_model = _sanitize_token(model_name.lower().replace("-", "_"))
    return f"tiles_{safe_model}"


def _normalize_tile_store(value: str) -> str:
    store = value.strip().lower()
    aliases = {
        "file": "local",
        "files": "local",
        "filesystem": "local",
        "satellite": "synthetic",
    }
    return aliases.get(store, store)


def _make_tile_store(settings: EmbedderSettings, tile_store: str) -> TileStore:
    store = _normalize_tile_store(tile_store)
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


def _resolve_embedder_backend(req: IndexRequest, settings: EmbedderSettings) -> str:
    override = (req.embedder_backend or "").strip()
    return override or settings.embedder_backend


def _resolve_embedder_model(req: IndexRequest, settings: EmbedderSettings) -> str:
    override = (req.embedder_model or "").strip()
    return override or settings.model_name


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

    vectordb = VectorDBClient(s.vectordb_url, timeout_s=s.vectordb_timeout_s)

    tiles_repo: Optional[TilesRepository] = None
    if s.update_tile_statuses:
        tiles_repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))
    if s.require_index_status_before_ack and not s.update_tile_statuses:
        raise ValueError("require_index_status_before_ack requires update_tile_statuses=true")

    embedder_cache: Dict[Tuple[str, str], Any] = {}

    def get_embedder(backend: str, model_name: str) -> Any:
        key = (backend.strip().lower(), model_name)
        if key not in embedder_cache:
            embedder_cache[key] = build_embedder(
                backend,
                model_name,
                clip_pretrained=s.clip_pretrained,
                remote_clip_url=s.remote_clip_url,
                remote_clip_timeout_s=s.remote_clip_timeout_s,
                remote_clip_image_format=s.remote_clip_image_format,
            )
        return embedder_cache[key]

    primary_model = get_embedder(s.embedder_backend, s.model_name)
    device = primary_model.device
    assert device is not None

    tile_store_cache: Dict[str, TileStore] = {}

    # Pool for tile loading (I/O bound)
    executor = ThreadPoolExecutor(max_workers=s.decode_workers)

    pbar = tqdm(total=0, desc="indexed", unit="img")
    indexed_total = 0
    received_total = 0

    print(
        f"Embedder worker started on device={device}, queue={s.queue_name}, "
        f"tile_store={s.tile_store}, backend={s.embedder_backend}, "
        f"table={_resolve_table_name(s, s.model_name)}. Ctrl+C to stop."
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
            store_name = _normalize_tile_store(req.tile_store or s.tile_store)
            if store_name not in tile_store_cache:
                try:
                    tile_store_cache[store_name] = _make_tile_store(s, store_name)
                except Exception as exc:
                    tile_id = _tile_id_for_req(req)
                    print(
                        f"[warn] failed to init tile store '{store_name}' "
                        f"for image_id={req.image_id}: {exc}"
                    )
                    _safe_update_status(tiles_repo, [tile_id], status="failed")
                    _safe_ack(envelope)
                    continue
            tile_store = tile_store_cache[store_name]
            fut = executor.submit(
                _load_tile,
                tile_store,
                req,
                s.tile_cache_dir if s.cache_tiles else None,
                s.tile_cache_format,
            )
            futures.append((req, envelope, fut))

        items: List[Dict[str, Any]] = []

        for req, envelope, fut in futures:
            tile_id = _tile_id_for_req(req)
            try:
                img, resolved_path = fut.result(timeout=s.job_timeout_s)
            except Exception as e:
                # Loading failed or timed out: mark failed and ack, we won't retry this one
                store_name = _normalize_tile_store(req.tile_store or s.tile_store)
                print(
                    f"[warn] failed to load tile for image_id={req.image_id} "
                    f"(tile_store={store_name}): {e}"
                )
                _safe_update_status(tiles_repo, [tile_id], status="failed")
                _safe_ack(envelope)
                continue

            backend = _resolve_embedder_backend(req, s)
            model_name = _resolve_embedder_model(req, s)
            if s.table_name.strip() and (backend != s.embedder_backend or model_name != s.model_name):
                print(
                    "[warn] embedder override ignored because EMBEDDER_TABLE_NAME is set; "
                    f"image_id={req.image_id}, backend={backend}, model={model_name}."
                )
                _safe_update_status(tiles_repo, [tile_id], status="failed")
                _safe_ack(envelope)
                continue
            items.append(
                {
                    "req": req,
                    "envelope": envelope,
                    "img": img,
                    "resolved_path": resolved_path,
                    "embedder_backend": backend,
                    "embedder_model": model_name,
                    "table_name": _resolve_table_name(s, model_name),
                }
            )

        # Clear the batch (we will rebuild based on which items succeeded)
        batch = []

        if not items:
            return

        items_by_embedder: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for item in items:
            key = (item["embedder_backend"], item["embedder_model"])
            items_by_embedder.setdefault(key, []).append(item)

        for (backend, model_name), group in items_by_embedder.items():
            images = [item["img"] for item in group]
            try:
                embeddings = get_embedder(backend, model_name).embed_pil_images(images).numpy()
            except Exception as e:
                print(
                    f"[warn] embedding batch of {len(images)} images failed "
                    f"(backend={backend}, model={model_name}): {e}"
                )
                return
            for item, emb in zip(group, embeddings):
                item["embedding"] = emb

        rows_by_table: Dict[str, List[Dict[str, Any]]] = {}
        envelopes_by_table: Dict[str, List[Any]] = {}
        tile_ids_by_table: Dict[str, List[str]] = {}

        for item in items:
            req = item["req"]
            envelope = item["envelope"]
            resolved_path = item["resolved_path"]
            emb = item["embedding"]
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
                "run_id": req.run_id,
                "tile_store": _normalize_tile_store(req.tile_store or s.tile_store),
                "embedder_backend": item["embedder_backend"],
                "embedder_model": item["embedder_model"],
                **bbox_to_columns(req.bbox),
                **geo_to_columns(req),
            }
            table_name = item["table_name"]
            rows_by_table.setdefault(table_name, []).append(row)
            envelopes_by_table.setdefault(table_name, []).append(envelope)
            tile_ids_by_table.setdefault(table_name, []).append(_tile_id_for_req(req))

        # Mark as waiting for index
        for tile_ids in tile_ids_by_table.values():
            _safe_update_status(tiles_repo, tile_ids, status="waiting for index")

        for table_name, rows in rows_by_table.items():
            try:
                vectordb.upsert(table_name, rows)
            except httpx.HTTPError as e:
                print(f"[warn] vectordb HTTP error on upsert of {len(rows)} rows: {e}")
                return
            except Exception as e:
                print(f"[warn] vectordb upsert failed for {len(rows)} rows: {e}")
                return

        # Upsert succeeded: mark indexed and ack
        for table_name, tile_ids in tile_ids_by_table.items():
            status_ok = _safe_update_status(tiles_repo, tile_ids, status="indexed")
            if status_ok or not s.require_index_status_before_ack:
                for envelope in envelopes_by_table.get(table_name, []):
                    _safe_ack(envelope)
            else:
                print(
                    "[warn] tiles db status update failed after vectordb upsert; "
                    "leaving messages unacked for retry."
                )

        indexed_count = sum(len(rows) for rows in rows_by_table.values())
        indexed_total += indexed_count
        pbar.update(indexed_count)

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
