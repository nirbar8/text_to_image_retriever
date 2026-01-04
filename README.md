# Retrieval + Heatmap System

This repository provides a production-ready, componentized retrieval pipeline with a Streamlit UI.
It separates domain models, adapters, services, and workers to keep boundaries clear and integrations replaceable.

## Architecture Overview

```
COCO Images / Tiles
    │
    │  (manifest.jsonl or tyler output)
    ▼
Vector Manager (victor)
  - tile registry (TilesDB)
  - emits index requests
    │
    ▼
Embedder Worker
  - fetches tiles via TileStore
  - batches PE-Core embeddings
  - upserts into VectorDB service
    │
    ▼
VectorDB Service (FastAPI + LanceDB)
    │
    ▼
Retriever Service (FastAPI)
  - PE-Core text embedding
  - post-processing hooks (GeoNMS scaffold)
    │
    ▼
Streamlit App (HTTP-only clients)
```

## Quickstart (Local)

### 1) Install dependencies

```bash
uv sync --group model --group vectordb --group queue --group app
```

### 2) Install PE-Core (Meta perception models)

```bash
mkdir -p third_party
git clone https://github.com/facebookresearch/perception_models.git third_party/perception_models
uv pip install -e third_party/perception_models --no-deps
```

### 3) Download COCO (optional, for demo)

```bash
./scripts/download_coco.sh
```

### 4) Build a COCO manifest

```bash
uv run victor build-manifest
```

### 5) Start RabbitMQ

```bash
docker compose up -d
```

### 6) Run services and workers

```bash
uv run vectordb-service
uv run retriever-service
uv run embedder-worker
```

### 7) Publish index requests

```bash
uv run victor publish
```

### 8) Run the Streamlit app

```bash
uv run app
```

## Configuration

Each component loads its own `.env.*` file via Pydantic settings. Examples live in `config/examples/`.

- `config/examples/.env.tyler`
- `config/examples/.env.victor`
- `config/examples/.env.embedder`
- `config/examples/.env.vectordb`
- `config/examples/.env.retriever`
- `config/examples/.env.app`

### Required variables per component

- Tyler: `TYLER_MODE`, `TYLER_OUTPUT_JSONL`
- Vector Manager (victor): `VICTOR_MANIFEST_PATH`, `VICTOR_QUEUE_NAME`, `VICTOR_TILES_DB_PATH`
- Embedder: `EMBEDDER_QUEUE_NAME`, `EMBEDDER_VECTORDB_URL`, `EMBEDDER_TABLE_NAME`
- VectorDB service: `VECTORDB_DB_DIR`
- Retriever service: `RETRIEVER_VECTORDB_URL`
- App: `APP_RETRIEVER_URL`, `APP_VECTORDB_URL`, `APP_TABLE_NAME`

## Component Commands

- Tyler: `uv run tyler`
- Vector Manager: `uv run victor build-manifest` and `uv run victor publish`
- Embedder Worker: `uv run embedder-worker`
- VectorDB Service: `uv run vectordb-service`
- Retriever Service: `uv run retriever-service`
- Streamlit App: `uv run app`

## Tests

```bash
uv run pytest
```

## Project Layout

```
src/
  retriever/
    core/
    adapters/
    components/
      tyler/
      victor/
      embedder_worker/
    services/
      vectordb/
      retriever/
    clients/
  app_streamlit/
config/examples/
```
