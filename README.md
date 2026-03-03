# GridSight

GridSight is an NFL analytics MVP built from the `Grid Sight` plan.

Current scope:

- NFL data ingestion (`nfl_data_py`)
- next-week player projection model
- play-level embedding index with Qdrant
- FastAPI endpoints for projections and similar-play search
- PostgreSQL schema + loader for app-ready tables
- Docker Compose stack (`api + postgres + qdrant`)

## Quick Start (Local Python)

### 1. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

### 2. Build artifacts (data + model + vector index)

```bash
gridsight bootstrap --start-season 2018 --end-season 2025 --embedding-dim 48 --max-rows 50000
```

### 3. Initialize/load PostgreSQL

Set DB URL first (example):

```bash
export GRIDSIGHT_DATABASE_URL='postgresql+psycopg://gridsight:gridsight@localhost:5432/gridsight'
```

Then:

```bash
gridsight db-bootstrap --drop-existing
```

### 4. Run API

```bash
gridsight api --reload
```

Open `http://localhost:8000/docs`.

## Docker Compose Stack

### 1. Configure env

```bash
cp .env.example .env
```

### 2. Start services

```bash
docker compose --env-file .env up -d --build
```

### 3. Build data and load DB inside container

```bash
docker compose --env-file .env exec api gridsight bootstrap --start-season 2018 --end-season 2025 --max-rows 50000
docker compose --env-file .env exec api gridsight db-bootstrap --drop-existing
```

### 4. Stop services

```bash
docker compose --env-file .env down
```

## CI/CD (GitHub Actions -> Remote Edge VM)

Workflow: [.github/workflows/deploy-dmz.yml](.github/workflows/deploy-dmz.yml)

- Deploys on push to `main`
- Targets a remote edge endpoint directly (no jump host)
- Syncs code and runs [scripts/deploy_dmz.sh](scripts/deploy_dmz.sh)

Setup guide: [docs/CI_CD_DMZ.md](docs/CI_CD_DMZ.md)

## CLI Commands

- `gridsight ingest --start-season 2018 --end-season 2025`
- `gridsight train --validation-season 2025`
- `gridsight index --embedding-dim 48 --max-rows 250000`
- `gridsight bootstrap ...`
- `gridsight db-init [--drop-existing]`
- `gridsight db-load [--truncate]`
- `gridsight db-bootstrap [--drop-existing]`
- `gridsight api --host 0.0.0.0 --port 8000`

## API Endpoints

- `GET /health`
- `GET /v1/projections/{player_id}`
- `POST /v1/similar-plays`

## Project Layout

- `src/gridsight/pipeline/ingest.py` data ingestion
- `src/gridsight/pipeline/projections.py` model training
- `src/gridsight/pipeline/play_vectors.py` embedding and indexing
- `src/gridsight/db/schema.py` PostgreSQL schema
- `src/gridsight/db/sync.py` parquet/model to DB loader
- `src/gridsight/api.py` FastAPI app
- `docker-compose.yml` deployable stack
- `docs/DEPLOY_EDGE_VM.md` deployment runbook
