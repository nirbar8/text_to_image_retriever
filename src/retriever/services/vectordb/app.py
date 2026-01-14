from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, HTTPException

from retriever.adapters.lancedb_adapter import LanceCfg, LanceDBAdapter
from retriever.core.schemas import (
    DeleteRowsRequest,
    DeleteRowsResponse,
    ExportRowsRequest,
    ExportRowsResponse,
    HealthResponse,
    OptimizeResponse,
    SampleRowsRequest,
    TableInfoResponse,
    VectorQueryRequest,
    VectorQueryResponse,
    VectorUpsertRequest,
    VectorUpsertResponse,
)
from retriever.services.vectordb.settings import VectorDBSettings


def create_app(settings: VectorDBSettings) -> FastAPI:
    app = FastAPI(title="VectorDB Service", version="1.0")
    adapter = LanceDBAdapter(LanceCfg(settings.db_dir))

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse()

    @app.get("/tables")
    def list_tables() -> Dict[str, Any]:
        return {"tables": adapter.list_tables()}

    def _table_exists(table_name: str) -> bool:
        return table_name in set(adapter.list_tables())

    @app.get("/tables/{table_name}/info", response_model=TableInfoResponse)
    def table_info(table_name: str) -> TableInfoResponse:
        if not _table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")
        info = adapter.table_info(table_name)
        return TableInfoResponse(
            db_dir=str(info.db_dir),
            table_name=info.table_name,
            vector_col=info.vector_col,
            vector_dim=info.vector_dim,
            version=info.version,
            row_count=info.row_count,
            columns=info.columns,
        )

    @app.post("/tables/{table_name}/search", response_model=VectorQueryResponse)
    def search(table_name: str, req: VectorQueryRequest) -> VectorQueryResponse:
        if not _table_exists(table_name):
            return VectorQueryResponse(results=[])
        results = adapter.vector_search(
            table_name=table_name,
            query_vec=req.query_vector,
            k=req.k,
            where=req.where,
            columns=req.columns,
        )
        return VectorQueryResponse(results=results)

    @app.post("/tables/{table_name}/rows")
    def sample_rows(table_name: str, req: SampleRowsRequest) -> Dict[str, Any]:
        if not _table_exists(table_name):
            return {"results": []}
        results = adapter.sample_rows(
            table_name=table_name,
            limit=req.limit,
            where=req.where,
            offset=req.offset,
            from_end=req.from_end,
            columns=req.columns,
        )
        return {"results": results}

    @app.post("/tables/{table_name}/upsert", response_model=VectorUpsertResponse)
    def upsert(table_name: str, req: VectorUpsertRequest) -> VectorUpsertResponse:
        if not req.rows:
            return VectorUpsertResponse(inserted=0)
        first = req.rows[0]
        embedding = first.get("embedding")
        if not embedding:
            raise HTTPException(status_code=400, detail="Missing embedding in rows")
        embedding_dim = len(embedding)
        inserted = adapter.upsert_rows(table_name, req.rows, embedding_dim=embedding_dim, id_col="image_id")
        return VectorUpsertResponse(inserted=inserted)

    @app.post("/tables/{table_name}/delete", response_model=DeleteRowsResponse)
    def delete_where(table_name: str, req: DeleteRowsRequest) -> DeleteRowsResponse:
        if not _table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")
        res = adapter.delete_where(table_name, req.where)
        return DeleteRowsResponse(**res)

    @app.post("/tables/{table_name}/export", response_model=ExportRowsResponse)
    def export_rows(table_name: str, req: ExportRowsRequest) -> ExportRowsResponse:
        if not _table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")
        written = adapter.export_jsonl(
            table_name=table_name,
            out_path=Path(req.out_path),
            where=req.where,
            page_size=req.page_size,
            max_rows=req.max_rows,
            columns=req.columns,
        )
        return ExportRowsResponse(written=written, out_path=req.out_path)

    @app.post("/tables/{table_name}/optimize", response_model=OptimizeResponse)
    def optimize_table(table_name: str) -> OptimizeResponse:
        if not _table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table not found: {table_name}")
        res = adapter.optimize_table(table_name)
        return OptimizeResponse(**res)

    return app


settings = VectorDBSettings()
app = create_app(settings)
