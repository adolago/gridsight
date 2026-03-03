from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    models_dir: Path
    vector_db_dir: Path
    raw_pbp_path: Path
    raw_weekly_path: Path
    player_state_path: Path
    play_index_path: Path
    projection_model_path: Path
    play_embedding_model_path: Path
    qdrant_collection: str
    qdrant_url: str | None
    qdrant_api_key: str | None
    qdrant_timeout_seconds: float
    database_url: str



def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default



def get_settings() -> Settings:
    project_root = Path(os.getenv("GRIDSIGHT_PROJECT_ROOT", ".")).resolve()
    data_dir = Path(os.getenv("GRIDSIGHT_DATA_DIR", project_root / "data"))

    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    models_dir = data_dir / "models"
    vector_db_dir = data_dir / "vector_db"

    for directory in (raw_dir, processed_dir, models_dir, vector_db_dir):
        directory.mkdir(parents=True, exist_ok=True)

    qdrant_url = os.getenv("GRIDSIGHT_QDRANT_URL")
    qdrant_api_key = os.getenv("GRIDSIGHT_QDRANT_API_KEY")
    qdrant_timeout = _float_env("GRIDSIGHT_QDRANT_TIMEOUT_SECONDS", 30.0)

    database_url = os.getenv(
        "GRIDSIGHT_DATABASE_URL",
        "postgresql+psycopg://gridsight:gridsight@localhost:5432/gridsight",
    )

    return Settings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        models_dir=models_dir,
        vector_db_dir=vector_db_dir,
        raw_pbp_path=raw_dir / "pbp.parquet",
        raw_weekly_path=raw_dir / "weekly.parquet",
        player_state_path=processed_dir / "latest_player_features.parquet",
        play_index_path=processed_dir / "play_index.parquet",
        projection_model_path=models_dir / "player_projection_model.joblib",
        play_embedding_model_path=models_dir / "play_embedding_model.joblib",
        qdrant_collection=os.getenv("GRIDSIGHT_QDRANT_COLLECTION", "plays"),
        qdrant_url=qdrant_url.strip() if qdrant_url else None,
        qdrant_api_key=qdrant_api_key.strip() if qdrant_api_key else None,
        qdrant_timeout_seconds=qdrant_timeout,
        database_url=database_url,
    )
