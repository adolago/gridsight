from __future__ import annotations

import argparse
import logging

import uvicorn

from gridsight.config import get_settings
from gridsight.db import initialize_database, sync_database
from gridsight.pipeline.ingest import ingest_nfl_data
from gridsight.pipeline.play_vectors import build_play_vector_index
from gridsight.pipeline.projections import train_projection_model



def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )



def run_ingest(start_season: int, end_season: int) -> None:
    settings = get_settings()
    ingest_nfl_data(settings, start_season=start_season, end_season=end_season)



def run_train(validation_season: int | None) -> None:
    settings = get_settings()
    metrics = train_projection_model(settings, validation_season=validation_season)
    logging.getLogger(__name__).info("Projection metrics: %s", metrics)



def run_vector_index(embedding_dim: int, max_rows: int) -> None:
    settings = get_settings()
    summary = build_play_vector_index(settings, embedding_dim=embedding_dim, max_rows=max_rows)
    logging.getLogger(__name__).info("Play vector index summary: %s", summary)



def run_bootstrap(
    start_season: int,
    end_season: int,
    validation_season: int | None,
    embedding_dim: int,
    max_rows: int,
) -> None:
    run_ingest(start_season=start_season, end_season=end_season)
    run_train(validation_season=validation_season)
    run_vector_index(embedding_dim=embedding_dim, max_rows=max_rows)



def run_api(host: str, port: int, reload: bool) -> None:
    uvicorn.run("gridsight.api:app", host=host, port=port, reload=reload)



def run_db_init(drop_existing: bool) -> None:
    settings = get_settings()
    initialize_database(settings=settings, drop_existing=drop_existing)
    logging.getLogger(__name__).info("Database schema initialized at %s", settings.database_url)



def run_db_load(truncate: bool) -> None:
    settings = get_settings()
    counts = sync_database(settings=settings, truncate=truncate, ensure_schema=True)
    logging.getLogger(__name__).info("Database sync complete: %s", counts)



def run_db_bootstrap(drop_existing: bool) -> None:
    settings = get_settings()
    initialize_database(settings=settings, drop_existing=drop_existing)
    counts = sync_database(settings=settings, truncate=False, ensure_schema=False)
    logging.getLogger(__name__).info("Database bootstrap complete: %s", counts)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GridSight CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_cmd = subparsers.add_parser("ingest", help="Ingest NFL raw data")
    ingest_cmd.add_argument("--start-season", type=int, default=2018)
    ingest_cmd.add_argument("--end-season", type=int, default=2025)

    train_cmd = subparsers.add_parser("train", help="Train weekly player projection model")
    train_cmd.add_argument("--validation-season", type=int)

    vector_cmd = subparsers.add_parser("index", help="Build play vector index")
    vector_cmd.add_argument("--embedding-dim", type=int, default=48)
    vector_cmd.add_argument("--max-rows", type=int, default=250_000)

    bootstrap_cmd = subparsers.add_parser("bootstrap", help="Run ingest + train + index")
    bootstrap_cmd.add_argument("--start-season", type=int, default=2018)
    bootstrap_cmd.add_argument("--end-season", type=int, default=2025)
    bootstrap_cmd.add_argument("--validation-season", type=int)
    bootstrap_cmd.add_argument("--embedding-dim", type=int, default=48)
    bootstrap_cmd.add_argument("--max-rows", type=int, default=250_000)

    api_cmd = subparsers.add_parser("api", help="Run API server")
    api_cmd.add_argument("--host", default="0.0.0.0")
    api_cmd.add_argument("--port", type=int, default=8000)
    api_cmd.add_argument("--reload", action="store_true")

    db_init_cmd = subparsers.add_parser("db-init", help="Create PostgreSQL schema")
    db_init_cmd.add_argument("--drop-existing", action="store_true")

    db_load_cmd = subparsers.add_parser(
        "db-load",
        help="Load parquet/model artifacts into PostgreSQL tables",
    )
    db_load_cmd.add_argument("--truncate", action="store_true")

    db_bootstrap_cmd = subparsers.add_parser(
        "db-bootstrap",
        help="Initialize schema and load database tables",
    )
    db_bootstrap_cmd.add_argument("--drop-existing", action="store_true")

    return parser.parse_args()



def main() -> None:
    configure_logging()
    args = parse_args()

    if args.command == "ingest":
        run_ingest(args.start_season, args.end_season)
        return
    if args.command == "train":
        run_train(args.validation_season)
        return
    if args.command == "index":
        run_vector_index(args.embedding_dim, args.max_rows)
        return
    if args.command == "bootstrap":
        run_bootstrap(
            start_season=args.start_season,
            end_season=args.end_season,
            validation_season=args.validation_season,
            embedding_dim=args.embedding_dim,
            max_rows=args.max_rows,
        )
        return
    if args.command == "api":
        run_api(args.host, args.port, args.reload)
        return
    if args.command == "db-init":
        run_db_init(drop_existing=args.drop_existing)
        return
    if args.command == "db-load":
        run_db_load(truncate=args.truncate)
        return
    if args.command == "db-bootstrap":
        run_db_bootstrap(drop_existing=args.drop_existing)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
