from __future__ import annotations

from typing import Dict, List

from fastapi import FastAPI

from retriever.adapters.embedder_factory import build_embedder
from retriever.clients.vectordb import VectorDBClient
from retriever.core.schemas import HealthResponse, RetrieverSearchRequest, RetrieverSearchResponse
from retriever.services.retriever.settings import RetrieverSettings


def _geo_nms_stub(rows: List[dict], radius_m: float | None) -> List[dict]:
    """Placeholder for GeoNMS; currently returns rows unchanged."""
    return rows


def create_app(settings: RetrieverSettings) -> FastAPI:
    app = FastAPI(title="Retriever Service", version="1.0")
    model = build_embedder(
        settings.embedder_backend,
        settings.model_name,
        clip_pretrained=settings.clip_pretrained,
        remote_clip_url=settings.remote_clip_url,
        remote_clip_timeout_s=settings.remote_clip_timeout_s,
        remote_clip_image_format=settings.remote_clip_image_format,
    )
    vectordb = VectorDBClient(settings.vectordb_url)

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse()

    @app.post("/search", response_model=RetrieverSearchResponse)
    def search(req: RetrieverSearchRequest) -> RetrieverSearchResponse:
        qvec = model.embed_texts([req.query_text])[0].tolist()
        results = vectordb.query(
            table_name=req.table_name,
            query_vector=qvec,
            k=req.k,
            where=req.where,
            columns=req.columns,
        )
        if req.apply_geo_nms:
            results = _geo_nms_stub(results, req.geo_nms_radius_m)
        return RetrieverSearchResponse(results=results)

    return app


settings = RetrieverSettings()
app = create_app(settings)
