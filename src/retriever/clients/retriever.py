from __future__ import annotations

from typing import List, Optional, Sequence

import httpx

from retriever.core.schemas import RetrieverSearchRequest


class RetrieverClient:
    def __init__(self, base_url: str, timeout_s: float = 60.0):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_s)

    def search(
        self,
        query_text: str,
        table_name: str,
        k: int,
        where: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        apply_geo_nms: bool = False,
        geo_nms_radius_m: Optional[float] = None,
    ) -> List[dict]:
        payload = RetrieverSearchRequest(
            query_text=query_text,
            table_name=table_name,
            k=k,
            where=where,
            columns=columns,
            apply_geo_nms=apply_geo_nms,
            geo_nms_radius_m=geo_nms_radius_m,
        ).model_dump()
        resp = self._client.post(f"{self._base_url}/search", json=payload)
        resp.raise_for_status()
        return list(resp.json().get("results", []))

    def close(self) -> None:
        self._client.close()
