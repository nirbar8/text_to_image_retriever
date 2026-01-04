from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from tqdm import tqdm

import httpx

from retriever.adapters.tiles_repo_sqlite import SqliteTilesConfig, SqliteTilesRepository
from retriever.adapters.message_bus_rmq import RabbitMQMessageBus, RmqConfig
from retriever.adapters.embedder_factory import build_embedder
from retriever.adapters.tile_store import LocalFileTileStore, OrthophotoTileStore, SyntheticSatelliteTileStore
from retriever.clients.vectordb import VectorDBClient
from retriever.components.embedder_worker.settings import EmbedderSettings
from retriever.core.interfaces import MessageBus, TileStore, TilesRepository
from retriever.core.schemas import IndexRequest


@dataclass
class _Job:
    req: IndexRequest
    image: Any
    resolved_image_path: Optional[str]
    ack: Any


def _build_or_predicate_int(field: str, values: Sequence[int]) -> Optional[str]:
    vals = [int(v) for v in values]
    if not vals:
        return None
    return " OR ".join([f"({field} = {v})" for v in vals])


def _tile_id_for_req(req: IndexRequest) -> str:
    return req.tile_id or f"tile:{req.image_id}"


def _safe_update_status(repo: Optional[TilesRepository], tile_ids: Sequence[str], status: str) -> None:
    if not repo or not tile_ids:
        return
    try:
        repo.update_status(tile_ids, status=status)
    except Exception:
        pass


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
        raise FileNotFoundError(f"Raster not found: {raster_path}. Set EMBEDDER_RASTER_PATH or use EMBEDDER_TILE_STORE=synthetic.")
    return OrthophotoTileStore(default_raster_path=str(raster_path))


def run() -> None:
    s = EmbedderSettings()
    bus: MessageBus = RabbitMQMessageBus(RmqConfig(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass))
    tile_store = _make_tile_store(s)
    vectordb = VectorDBClient(s.vectordb_url, timeout_s=s.vectordb_timeout_s)
    tiles_repo: Optional[TilesRepository] = None
    if s.update_tile_statuses:
        tiles_repo = SqliteTilesRepository(SqliteTilesConfig(s.tiles_db_path))

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

    executor = ThreadPoolExecutor(max_workers=s.decode_workers)

    job_futures: List[Tuple[Any, IndexRequest, Any, float]] = []
    write_buffer: List[Dict[str, Any]] = []
    write_acks: List[Any] = []
    write_tile_ids: List[str] = []
    seen_in_run: Set[int] = set()
    next_upsert_retry_ts = 0.0
    retry_backoff_s = float(s.vectordb_retry_s)
    last_upsert_error_log = 0.0

    pbar = tqdm(total=0, desc="indexed", unit="img")
    indexed_total = 0

    def flush_writes(force: bool = False) -> bool:
        nonlocal write_buffer
        nonlocal write_acks
        nonlocal write_tile_ids
        nonlocal next_upsert_retry_ts
        nonlocal retry_backoff_s
        nonlocal last_upsert_error_log

        if not write_buffer:
            return True
        now = time.time()
        if not force and len(write_buffer) < s.flush_rows:
            return True
        if now < next_upsert_retry_ts:
            return False
        try:
            vectordb.upsert(table_name, write_buffer)
        except Exception as exc:
            next_upsert_retry_ts = now + retry_backoff_s
            retry_backoff_s = min(retry_backoff_s * 2.0, float(s.vectordb_retry_max_s))
            if now - last_upsert_error_log >= s.idle_log_every_s:
                print(f"[warn] vectordb upsert failed; retrying in {int(retry_backoff_s)}s: {exc}")
                last_upsert_error_log = now
            return False

        _ack_all(write_acks)
        _safe_update_status(tiles_repo, write_tile_ids, status="indexed")
        write_buffer = []
        write_acks = []
        write_tile_ids = []
        next_upsert_retry_ts = 0.0
        retry_backoff_s = float(s.vectordb_retry_s)
        return True

    def _ack_all(methods: Iterable[Any]) -> None:
        for m in methods:
            try:
                m()
            except Exception:
                pass

    def _safe_ack(method) -> None:
        try:
            method()
        except Exception:
            pass

    def _existing_image_ids(image_ids: Sequence[int]) -> Set[int]:
        pred = _build_or_predicate_int("image_id", image_ids)
        if not pred:
            return set()
        try:
            rows = vectordb.sample_rows(table_name, where=pred, limit=len(image_ids), columns=["image_id"])
        except httpx.HTTPError:
            return set()
        return {int(r["image_id"]) for r in rows}

    def embed_and_stage(jobs: List[_Job]) -> None:
        nonlocal indexed_total

        images = [j.image for j in jobs]
        image_features = model.embed_pil_images(images).numpy()

        staged_tile_ids: List[str] = []
        for j, emb in zip(jobs, image_features):
            req = j.req
            row = {
                "id": str(req.image_id),
                "embedding": emb.tolist(),
                "image_path": j.resolved_image_path or "",
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
            write_buffer.append(row)
            write_acks.append(j.ack)
            tile_id = _tile_id_for_req(req)
            write_tile_ids.append(tile_id)
            staged_tile_ids.append(tile_id)

        indexed_total += len(jobs)
        pbar.update(len(jobs))
        _safe_update_status(tiles_repo, staged_tile_ids, status="embedded")
        _ = flush_writes(force=False)

    def drain_futures(batch_size: int) -> None:
        nonlocal job_futures
        nonlocal last_pending_log

        ready: List[Tuple[IndexRequest, Any, Any, Optional[str]]] = []
        pending: List[Tuple[Any, IndexRequest, Any, float]] = []

        now = time.time()
        for fut, req, ack, start_ts in job_futures:
            if fut.done():
                try:
                    img, resolved_path = fut.result()
                    ready.append((req, ack, img, resolved_path))
                except Exception:
                    _safe_ack(ack)
                    _safe_update_status(tiles_repo, [_tile_id_for_req(req)], status="failed")
            else:
                if now - start_ts > s.job_timeout_s:
                    try:
                        fut.cancel()
                    except Exception:
                        pass
                    _safe_ack(ack)
                    _safe_update_status(tiles_repo, [_tile_id_for_req(req)], status="failed")
                else:
                    pending.append((fut, req, ack, start_ts))

        job_futures = pending
        if not ready:
            if job_futures and now - last_pending_log >= s.pending_log_every_s:
                print(f"[pending] futures={len(job_futures)}")
                last_pending_log = now
            return

        uniq: List[Tuple[IndexRequest, Any, Any, Optional[str]]] = []
        skipped: List[Any] = []
        skipped_tile_ids: List[str] = []
        for req, ack, img, resolved_path in ready:
            image_id = int(req.image_id)
            if image_id in seen_in_run:
                skipped.append(ack)
                skipped_tile_ids.append(_tile_id_for_req(req))
                continue
            seen_in_run.add(image_id)
            uniq.append((req, ack, img, resolved_path))
        if skipped:
            _ack_all(skipped)
            _safe_update_status(tiles_repo, skipped_tile_ids, status="indexed")

        if not uniq:
            return

        uniq_ids = [int(req.image_id) for (req, _, _, _) in uniq]
        exists = _existing_image_ids(uniq_ids)
        if exists:
            kept: List[Tuple[IndexRequest, Any, Any, Optional[str]]] = []
            to_ack: List[Any] = []
            to_mark: List[str] = []
            for req, ack, img, resolved_path in uniq:
                if int(req.image_id) in exists:
                    to_ack.append(ack)
                    to_mark.append(_tile_id_for_req(req))
                else:
                    kept.append((req, ack, img, resolved_path))
            if to_ack:
                _ack_all(to_ack)
                _safe_update_status(tiles_repo, to_mark, status="indexed")
            uniq = kept

        if not uniq:
            return

        for i in range(0, len(uniq), batch_size):
            chunk = uniq[i : i + batch_size]
            jobs = [
                _Job(req=req, ack=ack, image=img, resolved_image_path=resolved_path)
                for (req, ack, img, resolved_path) in chunk
            ]
            embed_and_stage(jobs)

    last_flush = time.time()
    last_message = time.time()
    last_pending_log = 0.0
    last_idle_log = 0.0
    received = 0

    try:
        print(
            f"Embedder worker started on device={device}, queue={s.queue_name}, "
            f"tile_store={s.tile_store}, backend={s.embedder_backend}, table={table_name}. Ctrl+C to stop."
        )
        while True:
            try:
                for envelope in bus.consume(s.queue_name):
                    if envelope.payload.get("_idle"):
                        now = time.time()
                        if job_futures:
                            drain_futures(s.batch_size)
                        if now - last_flush > s.idle_flush_s:
                            flush_writes(force=True)
                            last_flush = now
                        if now - last_message > s.idle_log_every_s and now - last_idle_log >= s.idle_log_every_s:
                            print(
                                f"[idle] no messages for {int(now - last_message)}s; "
                                f"queue={s.queue_name} tile_store={s.tile_store}"
                            )
                            last_idle_log = now
                        continue

                    req = IndexRequest(**envelope.payload)
                    image_id = int(req.image_id)
                    last_message = time.time()
                    received += 1
                    if received == 1 or received % s.recv_log_every == 0:
                        print(f"[recv] received={received} image_id={image_id}")

                    if image_id in seen_in_run:
                        envelope.ack()
                        _safe_update_status(tiles_repo, [_tile_id_for_req(req)], status="indexed")
                        continue
                    _safe_update_status(tiles_repo, [_tile_id_for_req(req)], status="processing")

                    if len(job_futures) >= s.max_inflight:
                        drain_futures(s.batch_size)

                    fut = executor.submit(
                        _load_tile,
                        tile_store,
                        req,
                        s.tile_cache_dir if s.cache_tiles else None,
                        s.tile_cache_format,
                    )
                    job_futures.append((fut, req, envelope.ack, time.time()))

                    now = time.time()
                    if now - last_flush > s.flush_interval_s:
                        flush_writes(force=True)
                        last_flush = now
                    drain_futures(s.batch_size)
            except Exception as exc:
                print(f"[warn] message bus consume failed; retrying in {s.rmq_retry_s}s: {exc}")
                time.sleep(s.rmq_retry_s)
                continue
    except KeyboardInterrupt:
        print("Stopping. Draining remaining jobs...")
        while job_futures:
            drain_futures(s.batch_size)
            time.sleep(0.05)
        flush_writes(force=True)
    finally:
        try:
            flush_writes(force=True)
        except Exception as e:
            print(f"[warn] final flush failed: {e}")
        executor.shutdown(wait=True)
        pbar.close()
        print(f"Done. Total newly indexed this run: {indexed_total}")


if __name__ == "__main__":
    run()
