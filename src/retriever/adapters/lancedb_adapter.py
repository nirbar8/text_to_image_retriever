from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

import lancedb
import pyarrow as pa


@dataclass(frozen=True)
class LanceCfg:
    db_dir: Path


@dataclass(frozen=True)
class TableInfo:
    db_dir: Path
    table_name: str
    schema: pa.Schema
    vector_col: str
    vector_dim: Optional[int]
    version: Optional[int]
    row_count: Optional[int]
    versions: Optional[List[Dict[str, Any]]]
    columns: List[str]


def _get_vector_dim_from_schema(schema: pa.Schema, vector_col: str) -> Optional[int]:
    try:
        field = schema.field(vector_col)
    except KeyError:
        return None

    t = field.type
    if pa.types.is_fixed_size_list(t):
        return t.list_size
    if hasattr(t, "list_size"):
        try:
            return int(t.list_size)  # type: ignore[attr-defined]
        except Exception:
            return None
    return None


class LanceDBAdapter:
    def __init__(self, cfg: LanceCfg):
        self._cfg = cfg
        self._cfg.db_dir.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(cfg.db_dir))
        self._schema_cache: Dict[str, pa.Schema] = {}

    def list_tables(self) -> List[str]:
        return list(self._db.table_names())

    def open_table(self, table_name: str):
        return self._db.open_table(table_name)

    def _schema(self, table_name: str) -> pa.Schema:
        if table_name not in self._schema_cache:
            table = self.open_table(table_name)
            self._schema_cache[table_name] = table.schema
        return self._schema_cache[table_name]

    def refresh_schema(self, table_name: str) -> None:
        self._schema_cache.pop(table_name, None)
        _ = self._schema(table_name)

    def table_info(self, table_name: str, vector_col: str = "embedding") -> TableInfo:
        table = self.open_table(table_name)
        schema = self._schema(table_name)
        vector_dim = _get_vector_dim_from_schema(schema, vector_col)

        version: Optional[int]
        try:
            version = int(getattr(table, "version"))
        except Exception:
            version = None

        row_count: Optional[int]
        try:
            row_count = int(table.count_rows())
        except Exception:
            row_count = None

        versions: Optional[List[Dict[str, Any]]]
        try:
            versions = list(table.list_versions())
        except Exception:
            versions = None

        return TableInfo(
            db_dir=self._cfg.db_dir,
            table_name=table_name,
            schema=schema,
            vector_col=vector_col,
            vector_dim=vector_dim,
            version=version,
            row_count=row_count,
            versions=versions,
            columns=list(schema.names),
        )

    def available_columns(self, table_name: str) -> List[str]:
        return list(self._schema(table_name).names)

    def _filter_existing_columns(self, table_name: str, cols: Sequence[str]) -> List[str]:
        available: Set[str] = set(self.available_columns(table_name))
        return [c for c in cols if c in available]

    def get_or_create_table(self, table_name: str, embedding_dim: int, vector_col: str = "embedding"):
        if table_name in self._db.table_names():
            return self._db.open_table(table_name)

        sentinel = {
            "id": "__schema_sentinel__",
            vector_col: [0.0] * int(embedding_dim),
            "image_path": "",
            "image_id": -1,
            "width": 0,
            "height": 0,
            "run_id": "",
            "tile_id": "",
            "gid": -1,
            "raster_path": "",
            "bbox_minx": 0.0,
            "bbox_miny": 0.0,
            "bbox_maxx": 0.0,
            "bbox_maxy": 0.0,
            "bbox_crs": "",
            "lat": 0.0,
            "lon": 0.0,
            "utm_zone": "",
        }

        table = self._db.create_table(table_name, data=[sentinel], mode="overwrite")
        table.delete("image_id = -1")
        self._schema_cache.pop(table_name, None)
        return table

    def add_rows(self, table_name: str, rows: List[Dict[str, Any]], embedding_dim: int) -> int:
        table = self.get_or_create_table(table_name, embedding_dim=embedding_dim)
        table.add(rows)
        return len(rows)

    def sample_rows(
        self,
        table_name: str,
        limit: int = 10,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        table = self.open_table(table_name)
        q = table.search()
        if where:
            q = q.where(where)
        if columns:
            cols = self._filter_existing_columns(table_name, columns)
            if cols:
                q = q.select(cols)
        return q.limit(limit).to_list()

    def delete_where(self, table_name: str, where: str) -> Dict[str, Optional[int]]:
        table = self.open_table(table_name)

        before: Optional[int]
        after: Optional[int]
        try:
            before = int(table.count_rows())
        except Exception:
            before = None

        table.delete(where)

        try:
            after = int(table.count_rows())
        except Exception:
            after = None

        return {"rows_before": before, "rows_after": after}

    def vector_search(
        self,
        table_name: str,
        query_vec: Sequence[float],
        k: int = 10,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        table = self.open_table(table_name)
        q = table.search(list(query_vec))
        if where:
            q = q.where(where)

        if columns:
            cols = self._filter_existing_columns(table_name, columns)
            if cols:
                q = q.select(cols)
        else:
            default_cols = [
                "image_path",
                "image_id",
                "width",
                "height",
                "run_id",
                "tile_id",
                "gid",
                "raster_path",
                "bbox_minx",
                "bbox_miny",
                "bbox_maxx",
                "bbox_maxy",
                "bbox_crs",
                "lat",
                "lon",
                "utm_zone",
            ]
            cols = self._filter_existing_columns(table_name, default_cols)
            if cols:
                q = q.select(cols)

        return q.limit(k).to_list()

    def export_jsonl(
        self,
        table_name: str,
        out_path: Path,
        where: Optional[str] = None,
        page_size: int = 5000,
        max_rows: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> int:
        import json

        out_path.parent.mkdir(parents=True, exist_ok=True)

        table = self.open_table(table_name)
        q = table.search()
        if where:
            q = q.where(where)

        if columns:
            cols = self._filter_existing_columns(table_name, columns)
            if cols:
                q = q.select(cols)

        offset = 0
        written = 0
        with out_path.open("w", encoding="utf-8") as f:
            while True:
                page = q.limit(page_size).offset(offset).to_list()
                if not page:
                    break
                for row in page:
                    f.write(json.dumps(row, ensure_ascii=False, default=str))
                    f.write("\n")
                written += len(page)
                offset += page_size
                if max_rows is not None and written >= max_rows:
                    break
        return written
