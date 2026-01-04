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
  - batches image embeddings
  - optionally caches tiles to disk
  - upserts into VectorDB service
    │
    ▼
VectorDB Service (FastAPI + LanceDB)
    │
    ▼
Retriever Service (FastAPI)
  - text embedding backend
  - post-processing hooks (GeoNMS scaffold)
    │
    ▼
Streamlit App (HTTP-only clients)
```

## Tile Handling + Storage

- Tyler writes `data/tiles.jsonl` with `bbox` + `raster_path` (orthophoto) or `gid` (synthetic); `image_path` is optional.
- Victor publishes tile requests to RabbitMQ and stores the tile registry in `data/tiles.db`.
- The Embedder Worker renders tiles through a `TileStore` and can **cache tiles to disk** (default `data/tiles_cache`).
  - Cached tiles are written into `image_path` in the VectorDB rows.
- The Streamlit UI prefers `image_path` but can fall back to raster-backed tiles when the path is empty.

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

### Tile cache (optional)

- `EMBEDDER_CACHE_TILES=true` writes tiles to disk (default: `data/tiles_cache`)
- `EMBEDDER_TILE_CACHE_DIR` and `EMBEDDER_TILE_CACHE_FORMAT` control cache location/format

### Table naming

If `EMBEDDER_TABLE_NAME` is empty, the worker uses `tiles_<model_name>` (e.g., `tiles_pe_core_b16_224`).

## Embedder Backends

Both the Embedder Worker and Retriever Service use the same backend family.

### PE-Core (default)

- `EMBEDDER_EMBEDDER_BACKEND=pe_core`
- `RETRIEVER_EMBEDDER_BACKEND=pe_core`
- `*_MODEL_NAME=PE-Core-B16-224`

Install:
```bash
git clone https://github.com/facebookresearch/perception_models.git third_party/perception_models
uv pip install -e third_party/perception_models --no-deps
```

### CLIP (OpenCLIP)

- `EMBEDDER_EMBEDDER_BACKEND=clip`
- `RETRIEVER_EMBEDDER_BACKEND=clip`
- `*_MODEL_NAME=ViT-B-32`
- `*_CLIP_PRETRAINED=openai`

Install:
```bash
uv pip install open_clip_torch
```

### SigLip2

- `EMBEDDER_EMBEDDER_BACKEND=siglip2`
- `RETRIEVER_EMBEDDER_BACKEND=siglip2`
- `*_MODEL_NAME=google/siglip2-base-patch16-224`

Install:
```bash
uv pip install transformers
```

### RemoteClip (HTTP)

- `EMBEDDER_EMBEDDER_BACKEND=remoteclip`
- `RETRIEVER_EMBEDDER_BACKEND=remoteclip`
- `*_REMOTE_CLIP_URL=http://localhost:9000`

Expected API contract:

```
POST /embed/images  {"images": ["<base64-png>", ...]}
-> {"embeddings": [[...], ...]}

POST /embed/texts   {"texts": ["a dog", ...]}
-> {"embeddings": [[...], ...]}
```

### Run the embedder worker with a different backend

Pick a backend and set env vars inline:

```bash
EMBEDDER_EMBEDDER_BACKEND=clip EMBEDDER_MODEL_NAME=ViT-B-32 EMBEDDER_CLIP_PRETRAINED=openai uv run embedder-worker
```

```bash
EMBEDDER_EMBEDDER_BACKEND=siglip2 EMBEDDER_MODEL_NAME=google/siglip2-base-patch16-224 uv run embedder-worker
```

```bash
EMBEDDER_EMBEDDER_BACKEND=remoteclip EMBEDDER_REMOTE_CLIP_URL=http://localhost:9000 uv run embedder-worker
```

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
