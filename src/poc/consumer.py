from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import torch
from PIL import Image
from tqdm import tqdm

from .config import Settings
from .lancedb_store import LanceCfg, add_rows, get_table, table_has_columns
from .pe_model import PECore
from .rmq import RmqConn, consume


DECODE_WORKERS = 8
MAX_INFLIGHT_JOBS = 512
WRITE_FLUSH_ROWS = 2048


@dataclass
class _Job:
    msg: Dict[str, Any]
    image_tensor: torch.Tensor
    method: Any


def _load_and_preprocess(image_path: str, preprocess_fn) -> torch.Tensor:
    with Image.open(image_path) as im:
        im = im.convert("RGB")
        t = preprocess_fn(im)
        return t.contiguous()


def _build_or_predicate_int(field: str, values: Sequence[int]) -> Optional[str]:
    vals = [int(v) for v in values]
    if not vals:
        return None
    return " OR ".join([f"({field} = {v})" for v in vals])


def _ack(channel, methods: Iterable[Any]) -> None:
    for m in methods:
        channel.basic_ack(delivery_tag=m.delivery_tag)


def _existing_image_ids(table, image_ids: Sequence[int]) -> Set[int]:
    pred = _build_or_predicate_int("image_id", image_ids)
    if not pred:
        return set()
    rows = table.search().where(pred).select(["image_id"]).limit(len(image_ids)).to_list()
    return {int(r["image_id"]) for r in rows}


def run() -> None:
    s = Settings()

    model = PECore("PE-Core-B16-224")
    device = model.device
    assert device is not None

    table = get_table(LanceCfg(s.lancedb_dir, s.table_name), embedding_dim=model.embed_dim)

    # Schema feature detection (prevents crashes when you evolve schema)
    supports_run_id = table_has_columns(table, ["run_id"])

    rmq = RmqConn(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass, s.queue_name)
    connection, channel = consume(rmq)
    channel.basic_qos(prefetch_count=1024)

    executor = ThreadPoolExecutor(max_workers=DECODE_WORKERS)

    job_futures: List[Tuple[Any, Dict[str, Any], Any]] = []
    write_buffer: List[Dict[str, Any]] = []
    seen_in_run: Set[int] = set()

    pbar = tqdm(total=0, desc="indexed", unit="img")
    indexed_total = 0
    use_autocast = (device.type == "cuda")

    def flush_writes(force: bool = False) -> None:
        nonlocal write_buffer
        if not write_buffer:
            return
        if not force and len(write_buffer) < WRITE_FLUSH_ROWS:
            return
        add_rows(table, write_buffer)
        write_buffer = []

    def embed_and_stage(jobs: List[_Job]) -> None:
        nonlocal indexed_total

        batch_cpu = torch.stack([j.image_tensor for j in jobs], dim=0)
        batch = batch_cpu.to(device, non_blocking=False)

        with torch.inference_mode():
            if use_autocast:
                with torch.autocast(device_type=device.type):
                    image_features, _, _ = model.model(batch, None)
            else:
                image_features, _, _ = model.model(batch, None)

        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        image_features = image_features.float().cpu().numpy()

        for j, emb in zip(jobs, image_features):
            m = j.msg
            row = {
                "id": str(m["image_id"]),
                "embedding": emb.tolist(),
                "image_path": m["image_path"],
                "image_id": int(m["image_id"]),
                "width": int(m["width"]),
                "height": int(m["height"]),
                "coco_file_name": m.get("coco_file_name", ""),
                "tile_id": m.get("tile_id"),
                "lat": m.get("lat"),
                "lon": m.get("lon"),
                "utm_zone": m.get("utm_zone"),
            }
            if supports_run_id:
                row["run_id"] = m.get("run_id")
            write_buffer.append(row)

        indexed_total += len(jobs)
        pbar.update(len(jobs))
        flush_writes(force=False)

    def drain_futures(batch_size: int) -> None:
        nonlocal job_futures

        ready: List[Tuple[Dict[str, Any], Any, torch.Tensor]] = []
        pending: List[Tuple[Any, Dict[str, Any], Any]] = []

        for fut, msg, method in job_futures:
            if fut.done():
                try:
                    img_t = fut.result()
                    ready.append((msg, method, img_t))
                except Exception as e:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    print(f"[warn] failed to load {msg.get('image_path')}: {e}")
            else:
                pending.append((fut, msg, method))

        job_futures = pending
        if not ready:
            return

        # In-run dedup (skip without embedding)
        uniq: List[Tuple[Dict[str, Any], Any, torch.Tensor]] = []
        skipped: List[Any] = []
        for msg, method, img_t in ready:
            image_id = int(msg["image_id"])
            if image_id in seen_in_run:
                skipped.append(method)
                continue
            seen_in_run.add(image_id)
            uniq.append((msg, method, img_t))
        if skipped:
            _ack(channel, skipped)

        if not uniq:
            return

        # DB-level idempotency (skip without embedding)
        uniq_ids = [int(m["image_id"]) for (m, _, _) in uniq]
        exists = _existing_image_ids(table, uniq_ids)
        if exists:
            kept: List[Tuple[Dict[str, Any], Any, torch.Tensor]] = []
            to_ack: List[Any] = []
            for msg, method, img_t in uniq:
                if int(msg["image_id"]) in exists:
                    to_ack.append(method)
                else:
                    kept.append((msg, method, img_t))
            if to_ack:
                _ack(channel, to_ack)
            uniq = kept

        if not uniq:
            return

        # Embed + stage + ack
        for i in range(0, len(uniq), batch_size):
            chunk = uniq[i : i + batch_size]
            jobs = [_Job(msg=m, method=method, image_tensor=img_t) for (m, method, img_t) in chunk]
            embed_and_stage(jobs)
            _ack(channel, [j.method for j in jobs])

    def on_message(ch, method, properties, body):
        msg = json.loads(body.decode("utf-8"))
        image_id = int(msg["image_id"])

        # Skip duplicates immediately (no decode, no embed)
        if image_id in seen_in_run:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if len(job_futures) >= MAX_INFLIGHT_JOBS:
            drain_futures(s.batch_size)

        fut = executor.submit(_load_and_preprocess, msg["image_path"], model.preprocess)
        job_futures.append((fut, msg, method))

    channel.basic_consume(queue=s.queue_name, on_message_callback=on_message, auto_ack=False)

    last_flush = time.time()

    try:
        print(f"Consumer started on device={device}. Ctrl+C to stop.")
        while True:
            connection.process_data_events(time_limit=0.2)
            drain_futures(s.batch_size)

            now = time.time()
            if now - last_flush > 5.0:
                flush_writes(force=False)
                last_flush = now

    except KeyboardInterrupt:
        print("Stopping. Draining remaining jobs...")
        while job_futures:
            drain_futures(s.batch_size)
            time.sleep(0.05)
        flush_writes(force=True)

    finally:
        # Final cleanup on any exit path (including exceptions)
        try:
            flush_writes(force=True)
        except Exception as e:
            print(f"[warn] final flush failed: {e}")

        try:
            channel.stop_consuming()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
        executor.shutdown(wait=True)
        pbar.close()
        print(f"Done. Total newly indexed this run: {indexed_total}")


if __name__ == "__main__":
    run()
