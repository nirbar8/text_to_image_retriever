from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

from .config import Settings
from .rmq import RmqConn, publish_json


def _new_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid.uuid4().hex[:10]}"


def run() -> None:
    s = Settings()
    rmq = RmqConn(s.rmq_host, s.rmq_port, s.rmq_user, s.rmq_pass, s.queue_name)

    manifest: Path = s.manifest_path
    if not manifest.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest}. Run build-manifest first.")

    run_id = _new_run_id()
    print(f"Producer run_id: {run_id}")

    n = 0
    for line in tqdm(manifest.read_text().splitlines(), desc="publishing"):
        msg = json.loads(line)
        msg["run_id"] = run_id
        publish_json(rmq, msg)
        n += 1

    print(f"Published {n} messages to queue {s.queue_name}")


if __name__ == "__main__":
    run()
