# Retrieval + Heatmap System (Aerial Tiles)

This repository provides a componentized retrieval pipeline for aerial imagery tiles, with a Streamlit UI and clean service boundaries. It supports orthophoto tiling and simulated satellite imagery tiling.

## Architecture Overview

```
Aerial Imagery
    │
    │  (tiles.jsonl from Tyler)
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
uv sync --group model --group vectordb --group queue --group app --group tyler
```

### 2) Install PE-Core (Meta perception models)

```bash
mkdir -p third_party
git clone https://github.com/facebookresearch/perception_models.git third_party/perception_models
uv pip install -e third_party/perception_models --no-deps
```

### 3) Prepare imagery

- Orthophoto: place your GeoTIFF at `data/rasters/orthophoto.tif`
- Simulated satellite: no data needed (synthetic tiles)

You can download a sample open-source raster:
```bash
uv run download-orthophoto
```

### 4) Generate tiles

Orthophoto tiles:
```bash
uv run tyler --mode orthophoto
```

Simulated satellite tiles:
```bash
uv run tyler --mode satellite
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
- Vector Manager (victor): `VICTOR_TILES_MANIFEST_PATH`, `VICTOR_QUEUE_NAME`, `VICTOR_TILES_DB_PATH`
- Embedder: `EMBEDDER_QUEUE_NAME`, `EMBEDDER_VECTORDB_URL`, `EMBEDDER_TILE_STORE`
- VectorDB service: `VECTORDB_DB_DIR`
- Retriever service: `RETRIEVER_VECTORDB_URL`
- App: `APP_RETRIEVER_URL`, `APP_VECTORDB_URL`, `APP_TABLE_NAME`

### Tile store options

- `EMBEDDER_TILE_STORE=orthophoto` uses `EMBEDDER_RASTER_PATH` + tile bbox
- `EMBEDDER_TILE_STORE=synthetic` uses deterministic synthetic tiles from `gid` + bbox
- `EMBEDDER_TILE_STORE=local` loads `image_path` as a normal image file

### Table naming

If `EMBEDDER_TABLE_NAME` is empty, the worker uses `tiles_<model_name>` (e.g., `tiles_pe_core_b16_224`).

## Component Commands

- Tyler: `uv run tyler`
- Vector Manager: `uv run victor publish`
- Embedder Worker: `uv run embedder-worker`
- VectorDB Service: `uv run vectordb-service`
- Retriever Service: `uv run retriever-service`
- Streamlit App: `uv run app`

## Database cleanup

If you switch tile sources or embedding models, clean storage to avoid mixing schemas:

- LanceDB: delete `data/lancedb` (or drop the table via VectorDB API)
- Tiles DB: delete `data/tiles.db`
- Tile manifest: delete `data/tiles.jsonl`
- RabbitMQ queue: purge `tiles.to_index` from the management UI

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
