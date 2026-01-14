from __future__ import annotations

from typing import List, Optional, Sequence

import httpx

from retriever.core.interfaces import VectorIndexClient, VectorQueryClient
from retriever.core.schemas import (
    DeleteRowsRequest,
    ExportRowsRequest,
    SampleRowsRequest,
    VectorQueryRequest,
    VectorUpsertRequest,
)


class VectorDBClient(VectorIndexClient, VectorQueryClient):
    def __init__(self, base_url: str, timeout_s: float = 60.0):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_s)

    def upsert(self, table_name: str, rows: List[dict]) -> int:
        payload = VectorUpsertRequest(rows=rows).model_dump()
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/upsert", json=payload)
        resp.raise_for_status()
        return int(resp.json().get("inserted", 0))

    def query(
        self,
        table_name: str,
        query_vector: Sequence[float],
        k: int,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        payload = VectorQueryRequest(
            query_vector=query_vector,
            k=k,
            where=where,
            columns=columns,
        ).model_dump()
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/search", json=payload)
        resp.raise_for_status()
        return list(resp.json().get("results", []))

    def sample_rows(
        self,
        table_name: str,
        where: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        from_end: bool = False,
        columns: Optional[Sequence[str]] = None,
    ) -> List[dict]:
        payload = SampleRowsRequest(
            where=where,
            limit=limit,
            offset=offset,
            from_end=from_end,
            columns=columns,
        ).model_dump()
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/rows", json=payload)
        resp.raise_for_status()
        return list(resp.json().get("results", []))

    def table_info(self, table_name: str) -> dict:
        resp = self._client.get(f"{self._base_url}/tables/{table_name}/info")
        resp.raise_for_status()
        return dict(resp.json())

    def list_tables(self) -> List[str]:
        resp = self._client.get(f"{self._base_url}/tables")
        resp.raise_for_status()
        return list(resp.json().get("tables", []))

    def delete_where(self, table_name: str, where: str) -> dict:
        payload = DeleteRowsRequest(where=where).model_dump()
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/delete", json=payload)
        resp.raise_for_status()
        return dict(resp.json())

    def export_rows(
        self,
        table_name: str,
        out_path: str,
        where: Optional[str] = None,
        page_size: int = 5000,
        max_rows: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> dict:
        payload = ExportRowsRequest(
            out_path=out_path,
            where=where,
            page_size=page_size,
            max_rows=max_rows,
            columns=columns,
        ).model_dump()
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/export", json=payload)
        resp.raise_for_status()
        return dict(resp.json())

    def optimize_table(self, table_name: str) -> dict:
        resp = self._client.post(f"{self._base_url}/tables/{table_name}/optimize", json={})
        resp.raise_for_status()
        return dict(resp.json())

    def close(self) -> None:
        self._client.close()
