from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Normalizer, OneHotEncoder, StandardScaler

from gridsight.config import Settings
from gridsight.vector_store import PlayVectorStore

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = [
    "down",
    "ydstogo",
    "yardline_100",
    "game_seconds_remaining",
    "score_differential",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
]
CATEGORICAL_COLUMNS = ["posteam", "defteam", "play_type"]
PAYLOAD_COLUMNS = [
    "game_id",
    "play_id",
    "season",
    "week",
    "posteam",
    "defteam",
    "play_type",
    "down",
    "ydstogo",
    "yardline_100",
    "yards_gained",
    "epa",
    "desc",
]



def _one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=True)



def _fit_embedding_model(
    frame: pd.DataFrame,
    embedding_dim: int,
) -> tuple[dict[str, Any], np.ndarray]:
    available_numeric = [column for column in NUMERIC_COLUMNS if column in frame.columns]
    available_categorical = [column for column in CATEGORICAL_COLUMNS if column in frame.columns]

    if not available_numeric and not available_categorical:
        raise ValueError("No valid columns found for play embeddings.")

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                available_numeric,
            ),
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("one_hot", _one_hot_encoder()),
                    ]
                ),
                available_categorical,
            ),
        ]
    )

    matrix = preprocessor.fit_transform(frame)
    max_dim = max(2, min(embedding_dim, matrix.shape[1] - 1))
    svd = TruncatedSVD(n_components=max_dim, random_state=42)
    normalizer = Normalizer(copy=False)

    vectors = svd.fit_transform(matrix)
    vectors = normalizer.transform(vectors)

    artifact = {
        "preprocessor": preprocessor,
        "svd": svd,
        "normalizer": normalizer,
        "feature_columns": available_numeric + available_categorical,
        "trained_at": datetime.now(UTC).isoformat(),
    }
    return artifact, vectors



def transform_play_query(artifact: dict[str, Any], query: pd.DataFrame) -> np.ndarray:
    matrix = artifact["preprocessor"].transform(query)
    vectors = artifact["svd"].transform(matrix)
    vectors = artifact["normalizer"].transform(vectors)
    return vectors



def build_play_vector_index(
    settings: Settings,
    embedding_dim: int = 48,
    max_rows: int = 250_000,
) -> dict[str, int | str]:
    pbp = pd.read_parquet(settings.raw_pbp_path)
    pbp = pbp[pbp["play_type"].isin(["pass", "run"])]
    pbp = pbp.dropna(subset=["game_id", "play_id"])

    if len(pbp) > max_rows:
        pbp = pbp.sample(n=max_rows, random_state=42)

    pbp = pbp.reset_index(drop=True)
    artifact, vectors = _fit_embedding_model(pbp, embedding_dim)

    payload_columns = [column for column in PAYLOAD_COLUMNS if column in pbp.columns]
    index_frame = pbp[payload_columns].copy()
    index_frame["vector_id"] = np.arange(len(index_frame), dtype=int)
    index_frame.to_parquet(settings.play_index_path, index=False)

    store = PlayVectorStore.from_settings(settings)
    store.recreate(vector_size=vectors.shape[1])
    store.upsert(index_frame, vectors)

    joblib.dump(artifact, settings.play_embedding_model_path)
    logger.info(
        "Saved play vector index rows=%d vector_size=%d path=%s",
        len(index_frame),
        vectors.shape[1],
        settings.play_embedding_model_path,
    )

    return {
        "rows_indexed": int(len(index_frame)),
        "vector_size": int(vectors.shape[1]),
        "collection": settings.qdrant_collection,
    }
