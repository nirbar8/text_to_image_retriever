
# PE-Core + LanceDB + RabbitMQ Retrieval POC

This repository is a **local POC for a large-scale image retrieval system**, built for hackathon experimentation and later extension to aerial / tiled imagery.

It implements a **CLIP-like dual encoder** (Meta **PE-Core**) for image–text embeddings, a **producer–consumer indexing pipeline** via RabbitMQ, and **vector search** using LanceDB.

The code is written to be **dataset-agnostic** and **metadata-aware**, so geospatial and tiling logic can be added later without changing the core infrastructure.

---

## Architecture Overview

```
COCO Images
    │
    │  (manifest.jsonl)
    ▼
Producer (RabbitMQ)
    │
    ▼
Consumer / Indexer
  - PE-Core image encoder
  - Batch embedding
  - LanceDB writes
    │
    ▼
LanceDB Vector Store
    │
    ▼
Retriever
  - PE-Core text encoder
  - Vector search
  - Metadata returned
```

---

## Features

* PE-Core (Meta Perception Encoder) image + text embeddings
* LanceDB vector storage and search
* RabbitMQ producer–consumer indexing pipeline
* COCO 2017 dataset support (100K images by default)
* Metadata preserved end-to-end (ready for geo / tile fields)
* Pydantic settings with `.env` and environment overrides
* macOS-friendly (CPU / MPS), Linux-ready for scale

---

## Requirements

* macOS or Linux
* Python **3.11**
* Docker Desktop (for RabbitMQ)
* `uv` package manager

---

## Installation

### 1. Clone repository

```bash
git clone <repo-url>
cd pe-lancedb-rmq-poc
```

---

### 2. Install uv (if needed)

```bash
brew install uv
```

---

### 3. Create Python environment and install deps

```bash
uv sync
```

---

### 4. Clone PE-Core (Meta perception models)

```bash
mkdir -p third_party
git clone https://github.com/facebookresearch/perception_models.git third_party/perception_models
uv pip install -e third_party/perception_models --no-deps
```

Install missing lightweight deps:

```bash
uv add ftfy einops timm
```

---

## Dataset Setup (COCO 2017)

### Download images and annotations

```bash
./scripts/download_coco.sh
```

This downloads:

* `train2017/` images
* `instances_train2017.json`

> Data is stored locally in `data/` and is **gitignored**.

---

### Build manifest (100K images)

```bash
uv run build-manifest
```

Creates:

```
data/manifest_100k.jsonl
```

Each line contains:

* image path
* image dimensions
* COCO metadata
* placeholder geo fields (tile_id, lat, lon, etc.)

---

## RabbitMQ Setup

Start RabbitMQ via Docker:

```bash
docker compose up -d
```

Management UI:

* [http://localhost:15672](http://localhost:15672)
* user: `guest`
* pass: `guest`

---

## Indexing Pipeline

### Terminal 1: Start consumer (indexer)

```bash
uv run consumer
```

This:

* Loads PE-Core encoder
* Consumes messages in batches
* Writes embeddings + metadata to LanceDB

---

### Terminal 2: Start producer

```bash
uv run producer
```

This publishes all manifest entries to RabbitMQ.

---

## Querying / Retrieval

Run text-to-image search:

```bash
uv run retriever
```

Default query:

```python
"a dog"
```

Returned results include:

* image path
* image id
* dimensions
* placeholder geo fields

---

## Configuration

All configuration is handled via **Pydantic Settings**.

### Environment variable override

```bash
export POC_BATCH_SIZE=128
export POC_MAX_ITEMS=50000
```

### `.env` file example

```env
POC_RMQ_HOST=localhost
POC_BATCH_SIZE=128
POC_LANCEDB_DIR=data/lancedb
```

---

## Project Structure

```
pe-lancedb-rmq-poc/
├── src/poc/
│   ├── config.py          # Pydantic settings
│   ├── pe_model.py        # PE-Core wrapper
│   ├── producer.py        # RabbitMQ producer
│   ├── consumer.py        # Indexer
│   ├── retriever.py       # Search
│   ├── lancedb_store.py
│   └── rmq.py
├── scripts/
│   └── download_coco.sh
├── third_party/
│   └── perception_models/
├── data/                  # gitignored
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

## Notes & Design Decisions

* `perception_models` is installed without `decord`
  Video dependencies are intentionally excluded for image-only POC.
* Metadata is intentionally flat and explicit
  This makes later geospatial indexing trivial.
* RabbitMQ is optional
  For local debugging, consumer logic can be run synchronously.

---

## Next Steps (Suggested)

* Add image-to-image retrieval
* Add LanceDB filters on metadata
* Add geo / tile indexing fields
* Add Makefile or unified CLI
* Run consumer on GPU (Linux / A100)
* Replace RabbitMQ with Kafka or Redpanda

---

## License

This repository is for experimentation and research.
PE-Core and COCO are subject to their respective licenses.

---

If you want, next I can:

* collapse this into a **single CLI (`poc index / query`)**
* add **local-only indexing without RabbitMQ**
* add **benchmark scripts (latency, throughput)**
<<<<<<< HEAD
=======

>>>>>>> 82cee0e (Initial commit)
