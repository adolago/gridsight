# GridSight Implementation Mapping

This repository now includes a Year-1 style MVP aligned with the Grid Sight plan:

## Built Now

- Ingestion pipeline for `nfl_data_py` play-by-play and weekly player data.
- Baseline weekly player projection model (`fantasy_points_ppr` next-week forecast).
- Play embedding pipeline with vector search index using local Qdrant.
- PostgreSQL schema for games, plays, players, weekly stats, model runs, and predictions.
- Database sync pipeline that loads parquet/model artifacts into PostgreSQL.
- FastAPI service for player projection and similar-play retrieval.
- CLI workflow to run ingestion, training, indexing, and API server.
- Docker Compose stack for API + Postgres + Qdrant.

## How This Maps to the Plan

- `Data ingestion and storage`: implemented via parquet files in `data/raw` and `data/processed`.
- `Vector database`: implemented with `qdrant-client` local storage and indexed play embeddings.
- `ML pipeline`: implemented with lag-feature engineering + random forest baseline.
- `Consumer API layer`: implemented via `/v1/projections/{player_id}` and `/v1/similar-plays`.
- `Application DB`: implemented via normalized PostgreSQL tables with loader commands.

## Next Build Steps

- Add live game ingestion (streaming) for in-game updates.
- Add league sync and personalized lineup optimization endpoints.
- Expand model stack to role-specific ensembles (QB/RB/WR/TE) and calibration tracking.
- Add frontend client (dashboard + lineup views + alerts).
