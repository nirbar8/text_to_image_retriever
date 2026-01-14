from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Literal, Callable, cast

import lancedb
import pyarrow as pa

from retriever.core.schemas import VECTOR_METADATA_COLUMNS, VECTOR_SCHEMA_COLUMNS


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
        return int(t.list_size)
    return None


def _pa_float(dtype: Literal["float16", "float32"]) -> pa.DataType:
    return pa.float16() if dtype == "float16" else pa.float32()


def _fixed_size_vector_type(value_type: pa.DataType, dim: int) -> pa.DataType:
    """
    Portable fixed-size list type across PyArrow versions + type stubs.
    This avoids Pylance complaining about pa.fixed_size_list.
    """
    return pa.list_(value_type, list_size=int(dim))


class LanceDBAdapter:
    def __init__(self, cfg: LanceCfg):
        self._cfg = cfg
        self._cfg.db_dir.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(cfg.db_dir))
        self._schema_cache: Dict[str, pa.Schema] = {}

    # -----------------------------
    # Basic table ops
    # -----------------------------
    def list_tables(self) -> List[str]:
        return list(self._db.table_names())

    def open_table(self, table_name: str):
        return self._db.open_table(table_name)

    def _schema(self, table_name: str) -> pa.Schema:
        if table_name not in self._schema_cache:
            self._schema_cache[table_name] = self.open_table(table_name).schema
        return self._schema_cache[table_name]

    def refresh_schema(self, table_name: str) -> None:
        self._schema_cache.pop(table_name, None)
        _ = self._schema(table_name)

    def table_info(self, table_name: str, vector_col: str = "embedding") -> TableInfo:
        table = self.open_table(table_name)
        schema = self._schema(table_name)

        try:
            version = int(getattr(table, "version"))
        except Exception:
            version = None

        try:
            row_count = int(table.count_rows())
        except Exception:
            row_count = None

        try:
            versions = list(table.list_versions())
        except Exception:
            versions = None

        return TableInfo(
            db_dir=self._cfg.db_dir,
            table_name=table_name,
            schema=schema,
            vector_col=vector_col,
            vector_dim=_get_vector_dim_from_schema(schema, vector_col),
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

    # -----------------------------
    # Schema creation
    # -----------------------------
    def _build_schema(
        self,
        embedding_dim: int,
        vector_col: str = "embedding",
        vector_dtype: Literal["float16", "float32"] = "float32",
    ) -> pa.Schema:
        vec_type = _fixed_size_vector_type(_pa_float(vector_dtype), int(embedding_dim))

        known: Dict[str, pa.DataType] = {
            "id": pa.string(),
            "image_id": pa.int64(),
            "width": pa.int32(),
            "height": pa.int32(),
            "indexed_at": pa.int64(),
            "gid": pa.int64(),
            "pixel_polygon": pa.string(),
            "geo_polygon": pa.string(),
            "lat": pa.float32(),
            "lon": pa.float32(),
            vector_col: vec_type,
        }

        fields: List[pa.Field] = []
        for col in VECTOR_SCHEMA_COLUMNS:
            dt = known.get(col, pa.string())
            nullable = col != vector_col
            fields.append(pa.field(col, dt, nullable=nullable))

        if vector_col not in {f.name for f in fields}:
            fields.append(pa.field(vector_col, vec_type, nullable=False))

        return pa.schema(fields)

    def _empty_table_from_schema(self, schema: pa.Schema) -> pa.Table:
        arrays: Dict[str, pa.Array] = {}
        for name in schema.names:
            dt = schema.field(name).type
            arrays[name] = pa.array([], type=dt)
        return pa.Table.from_arrays(list(arrays.values()), names=list(arrays.keys()), schema=schema)

    def get_or_create_table(
        self,
        table_name: str,
        embedding_dim: int,
        vector_col: str = "embedding",
        vector_dtype: Literal["float16", "float32"] = "float32",  # NEW optional
    ):
        if table_name in self._db.table_names():
            return self._db.open_table(table_name)

        schema = self._build_schema(
            embedding_dim=int(embedding_dim),
            vector_col=vector_col,
            vector_dtype=vector_dtype,
        )
        empty = self._empty_table_from_schema(schema)
        table = self._db.create_table(table_name, data=empty, mode="create")
        self._schema_cache.pop(table_name, None)
        return table

    # -----------------------------
    # Writes
    # -----------------------------
    @staticmethod
    def _filter_rows_to_schema(schema: pa.Schema, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = set(schema.names)
        return [{k: v for k, v in row.items() if k in allowed} for row in rows]

    def add_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        embedding_dim: int,
        vector_col: str = "embedding",  # NEW optional
        vector_dtype: Literal["float16", "float32"] = "float32",  # NEW optional
    ) -> int:
        table = self.get_or_create_table(
            table_name, embedding_dim=embedding_dim, vector_col=vector_col, vector_dtype=vector_dtype
        )
        filtered = self._filter_rows_to_schema(table.schema, rows)
        if not filtered:
            return 0
        table.add(filtered)
        return len(filtered)

    @staticmethod
    def _build_in_clause(col: str, values: Sequence[Any]) -> str:
        parts: List[str] = []
        for v in values:
            if isinstance(v, str):
                parts.append("'" + v.replace("'", "''") + "'")
            else:
                try:
                    parts.append(str(int(v)))
                except Exception:
                    parts.append("'" + str(v).replace("'", "''") + "'")
        return f"{col} in ({', '.join(parts)})"

    def upsert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        embedding_dim: int,
        id_col: str = "image_id",
        vector_col: str = "embedding",  # NEW optional
        vector_dtype: Literal["float16", "float32"] = "float32",  # NEW optional
        merge_use_index: bool = True,  # NEW optional
    ) -> int:
        table = self.get_or_create_table(
            table_name, embedding_dim=embedding_dim, vector_col=vector_col, vector_dtype=vector_dtype
        )
        filtered = self._filter_rows_to_schema(table.schema, rows)
        if not filtered:
            return 0

        # Prefer merge_insert if available
        mi_fn = getattr(table, "merge_insert", None)
        if callable(mi_fn):
            # Create scalar index on join key if possible (non-fatal on failure)
            try:
                self.ensure_scalar_index(table_name, id_col, index_type="BTREE", wait_timeout=None)
            except Exception:
                pass

            try:
                mi = table.merge_insert(on=[id_col])

                use_index_method = getattr(mi, "use_index", None)
                if callable(use_index_method):
                    try:
                        mi = use_index_method(bool(merge_use_index))
                    except Exception:
                        pass

                mi = mi.when_matched_update_all().when_not_matched_insert_all()
                exec_fn = getattr(mi, "execute", None)
                if callable(exec_fn):
                    try:
                        exec_fn(filtered)
                    except TypeError:
                        exec_fn(data=filtered)
                    return len(filtered)
            except Exception:
                pass  # fallback below

        # Fallback delete+add
        ids = [r.get(id_col) for r in filtered if r.get(id_col) is not None]
        if ids:
            table.delete(self._build_in_clause(id_col, ids))
        table.add(filtered)
        return len(filtered)

    # -----------------------------
    # Indexing
    # -----------------------------
    def ensure_vector_index(
        self,
        table_name: str,
        vector_col: str = "embedding",
        metric: Literal["l2", "cosine", "dot"] = "cosine",
        index_type: Literal["IVF_PQ", "IVF_HNSW_SQ"] = "IVF_PQ",
        num_partitions: Optional[int] = None,
        num_sub_vectors: Optional[int] = None,
        wait_timeout: Optional[float] = None,
        replace: bool = False,
    ) -> Dict[str, Any]:
        table = self.open_table(table_name)

        if not replace:
            list_idx = getattr(table, "list_indices", None)
            if callable(list_idx):
                try:
                    existing = list(list_idx())
                    if existing:
                        return {"status": "ok", "details": {"skipped": True, "existing": existing}}
                except Exception:
                    pass

        kwargs: Dict[str, Any] = {
            "index_type": index_type,
            "metric": metric,
            "vector_column_name": vector_col,  # explicit
        }
        if num_partitions is not None:
            kwargs["num_partitions"] = int(num_partitions)
        if num_sub_vectors is not None:
            kwargs["num_sub_vectors"] = int(num_sub_vectors)
        if wait_timeout is not None:
            kwargs["wait_timeout"] = float(wait_timeout)

        table.create_index(**kwargs)
        return {"status": "ok", "details": {"created": True, "params": kwargs}}

    def ensure_scalar_index(
        self,
        table_name: str,
        column: str,
        index_type: Literal["BTREE", "BITMAP", "LABEL_LIST"] = "BTREE",
        wait_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        table = self.open_table(table_name)

        if column not in set(table.schema.names):
            return {"status": "error", "details": {"error": f"Column not in schema: {column}"}}

        fn = getattr(table, "create_scalar_index", None)
        if not callable(fn):
            return {"status": "error", "details": {"error": "create_scalar_index not available"}}

        kwargs: Dict[str, Any] = {"column": column, "index_type": index_type}
        if wait_timeout is not None:
            kwargs["wait_timeout"] = float(wait_timeout)

        fn(**kwargs)
        return {"status": "ok", "details": {"created": True, "params": kwargs}}

    # -----------------------------
    # Reads
    # -----------------------------
    def vector_search(
        self,
        table_name: str,
        query_vec: Sequence[float],
        k: int = 10,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        vector_col: str = "embedding",  # NEW optional
        nprobes: Optional[int] = None,  # NEW optional
        refine_factor: Optional[int] = None,  # NEW optional
    ) -> List[Dict[str, Any]]:
        table = self.open_table(table_name)

        q = table.search(list(query_vec), vector_column_name=vector_col)

        if where:
            q = q.where(where)

        # Avoid Pylance complaints by using getattr+callable
        if nprobes is not None:
            fn = getattr(q, "nprobes", None)
            if callable(fn):
                q = fn(int(nprobes))
        if refine_factor is not None:
            fn = getattr(q, "refine_factor", None)
            if callable(fn):
                q = fn(int(refine_factor))

        if columns:
            cols = self._filter_existing_columns(table_name, columns)
            if cols:
                q = q.select(cols)
        else:
            default_cols = self._filter_existing_columns(table_name, list(VECTOR_METADATA_COLUMNS))
            if default_cols:
                q = q.select(default_cols)

        return q.limit(k).to_list()

    def sample_rows(
        self,
        table_name: str,
        limit: int = 10,
        where: Optional[str] = None,
        offset: int = 0,
        from_end: bool = False,
        columns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        table = self.open_table(table_name)
        if from_end:
            try:
                row_count = int(table.count_rows())
            except Exception:
                row_count = 0
            offset = max(row_count - limit, 0)

        q = table.search()
        if where:
            q = q.where(where)
        if columns:
            cols = self._filter_existing_columns(table_name, columns)
            if cols:
                q = q.select(cols)
        return q.limit(limit).offset(offset).to_list()

    # -----------------------------
    # Maintenance
    # -----------------------------
    def optimize_table(self, table_name: str) -> Dict[str, Any]:
        table = self.open_table(table_name)
        try:
            res = table.optimize()
        except Exception as exc:
            return {"status": "error", "details": {"error": str(exc)}}

        self.refresh_schema(table_name)
        return {"status": "ok", "details": res if isinstance(res, dict) else None}

    def delete_where(self, table_name: str, where: str) -> Dict[str, Optional[int]]:
        table = self.open_table(table_name)
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
                limit = page_size
                if max_rows is not None:
                    remaining = max_rows - written
                    if remaining <= 0:
                        break
                    limit = min(limit, remaining)

                page = q.limit(limit).offset(offset).to_list()
                if not page:
                    break

                for row in page:
                    f.write(json.dumps(row, ensure_ascii=False, default=str))
                    f.write("\n")

                written += len(page)
                offset += page_size

        return written
