from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import lancedb


@dataclass(frozen=True)
class LanceCfg:
    db_dir: Path
    table_name: str


def get_table(cfg: LanceCfg, embedding_dim: int):
    cfg.db_dir.mkdir(parents=True, exist_ok=True)
    db = lancedb.connect(str(cfg.db_dir))

    if cfg.table_name in db.table_names():
        return db.open_table(cfg.table_name)

    # IMPORTANT:
    # Create table using a sentinel row with typed (non-None) values
    # to prevent Arrow from inferring "null" type columns.
    sentinel = {
        "id": "__schema_sentinel__",
        "embedding": [0.0] * int(embedding_dim),
        "image_path": "",
        "image_id": -1,
        "width": 0,
        "height": 0,
        "coco_file_name": "",
        "run_id": "",     # string typed
        "tile_id": "",    # string typed
        "lat": 0.0,       # float typed
        "lon": 0.0,       # float typed
        "utm_zone": "",   # string typed
    }

    table = db.create_table(cfg.table_name, data=[sentinel], mode="overwrite")
    table.delete("image_id = -1")
    return table


def table_has_columns(table, cols: List[str]) -> bool:
    names = set(table.schema.names)
    return all(c in names for c in cols)


def add_rows(table, rows: List[Dict[str, Any]]) -> None:
    table.add(rows)
