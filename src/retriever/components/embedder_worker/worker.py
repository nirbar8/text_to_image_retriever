from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from PIL import Image
import torch
from tqdm import tqdm

from retriever.adapters.message_bus_rmq import RabbitMQMessageBus, RmqConfig
from retriever.adapters.pe_core import PECoreEmbedder
from retriever.adapters.tile_store_local import LocalFileTileStore
from retriever.clients.vectordb import VectorDBClient
from retriever.components.embedder_worker.settings import EmbedderSettings
from retriever.core.interfaces import MessageBus, TileStore
from retriever.core.schemas import IndexRequest


@dataclass
class _Job:
    req: IndexRequest
    image_tensor: torch.Tensor
    ack: Any


def _build_or_predicate_int(field: str, values: Sequence[int]) -> Optional[str]:
    vals = [int(v) for v in values]
    if not vals:
        return None
    return " OR ".join([f"({field} = {v})" for v in vals])


def _load_and_preprocess(image_bytes: bytes, preprocess_fn) -> torch.Tensor:
    with Image.open(BytesIO(image_bytes)) as im:
        im = im.convert("RGB")
        t = preprocess_fn(im)
        return t.contiguous()


def run() -> None:
    s = EmbedderSettings()
    bus: MessageBus = RabbitMQMessageBus(RmqConfig(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass))
    tile_store: TileStore = LocalFileTileStore()
    vectordb = VectorDBClient(s.vectordb_url)

    model = PECoreEmbedder("PE-Core-B16-224")
    device = model.device
    assert device is not None

    executor = ThreadPoolExecutor(max_workers=s.decode_workers)

    job_futures: List[Tuple[Any, IndexRequest, Any]] = []
    write_buffer: List[Dict[str, Any]] = []
    seen_in_run: Set[int] = set()

    pbar = tqdm(total=0, desc="indexed", unit="img")
    indexed_total = 0
    use_autocast = device.type == "cuda"

    def flush_writes(force: bool = False) -> None:
        nonlocal write_buffer
        if not write_buffer:
            return
        if not force and len(write_buffer) < s.flush_rows:
            return
        vectordb.upsert(s.table_name, write_buffer)
        write_buffer = []

    def _ack_all(methods: Iterable[Any]) -> None:
        for m in methods:
            try:
                m()
            except Exception:
                pass

    def _existing_image_ids(image_ids: Sequence[int]) -> Set[int]:
        pred = _build_or_predicate_int("image_id", image_ids)
        if not pred:
            return set()
        rows = vectordb.sample_rows(s.table_name, where=pred, limit=len(image_ids), columns=["image_id"])
        return {int(r["image_id"]) for r in rows}

    def embed_and_stage(jobs: List[_Job]) -> None:
        nonlocal indexed_total

        batch_cpu = torch.stack([j.image_tensor for j in jobs], dim=0)
        batch = batch_cpu.to(device, non_blocking=False)

        with torch.inference_mode():
            if use_autocast:
                with torch.autocast(device_type=device.type):
                    image_features = model.model(batch, None)[0]
            else:
                image_features = model.model(batch, None)[0]

        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        image_features = image_features.float().cpu().numpy()

        for j, emb in zip(jobs, image_features):
            req = j.req
            row = {
                "id": str(req.image_id),
                "embedding": emb.tolist(),
                "image_path": req.image_path,
                "image_id": int(req.image_id),
                "width": int(req.width),
                "height": int(req.height),
                "coco_file_name": req.coco_file_name or "",
                "tile_id": req.tile_id,
                "lat": req.lat,
                "lon": req.lon,
                "utm_zone": req.utm_zone,
                "run_id": req.run_id,
            }
            write_buffer.append(row)

        indexed_total += len(jobs)
        pbar.update(len(jobs))
        flush_writes(force=False)

    def drain_futures(batch_size: int) -> None:
        nonlocal job_futures

        ready: List[Tuple[IndexRequest, Any, torch.Tensor]] = []
        pending: List[Tuple[Any, IndexRequest, Any]] = []

        for fut, req, ack in job_futures:
            if fut.done():
                try:
                    img_t = fut.result()
                    ready.append((req, ack, img_t))
                except Exception:
                    ack()
            else:
                pending.append((fut, req, ack))

        job_futures = pending
        if not ready:
            return

        uniq: List[Tuple[IndexRequest, Any, torch.Tensor]] = []
        skipped: List[Any] = []
        for req, ack, img_t in ready:
            image_id = int(req.image_id)
            if image_id in seen_in_run:
                skipped.append(ack)
                continue
            seen_in_run.add(image_id)
            uniq.append((req, ack, img_t))
        if skipped:
            _ack_all(skipped)

        if not uniq:
            return

        uniq_ids = [int(req.image_id) for (req, _, _) in uniq]
        exists = _existing_image_ids(uniq_ids)
        if exists:
            kept: List[Tuple[IndexRequest, Any, torch.Tensor]] = []
            to_ack: List[Any] = []
            for req, ack, img_t in uniq:
                if int(req.image_id) in exists:
                    to_ack.append(ack)
                else:
                    kept.append((req, ack, img_t))
            if to_ack:
                _ack_all(to_ack)
            uniq = kept

        if not uniq:
            return

        for i in range(0, len(uniq), batch_size):
            chunk = uniq[i : i + batch_size]
            jobs = [_Job(req=req, ack=ack, image_tensor=img_t) for (req, ack, img_t) in chunk]
            embed_and_stage(jobs)
            _ack_all([j.ack for j in jobs])

    last_flush = time.time()

    try:
        print(f"Embedder worker started on device={device}. Ctrl+C to stop.")
        for envelope in bus.consume(s.queue_name):
            req = IndexRequest(**envelope.payload)
            image_id = int(req.image_id)

            if image_id in seen_in_run:
                envelope.ack()
                continue

            if len(job_futures) >= s.max_inflight:
                drain_futures(s.batch_size)

            image_bytes = tile_store.get_tile_bytes(req.image_path)
            fut = executor.submit(_load_and_preprocess, image_bytes, model.preprocess)
            job_futures.append((fut, req, envelope.ack))

            now = time.time()
            if now - last_flush > 5.0:
                flush_writes(force=False)
                last_flush = now
            drain_futures(s.batch_size)
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
