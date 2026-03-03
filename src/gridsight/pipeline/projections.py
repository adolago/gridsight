from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from gridsight.config import Settings

logger = logging.getLogger(__name__)

TARGET_COLUMN = "fantasy_points_ppr"
BASE_NUMERIC_FEATURES = [
    "fantasy_points_ppr",
    "fantasy_points",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "completions",
    "attempts",
    "air_yards",
]
BASE_CATEGORICAL_FEATURES = ["position", "recent_team", "opponent_team"]



def _one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)



def build_training_frame(weekly: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    if TARGET_COLUMN not in weekly.columns:
        raise ValueError(f"Weekly data must include '{TARGET_COLUMN}'.")
    if "player_id" not in weekly.columns:
        raise ValueError("Weekly data must include 'player_id'.")

    frame = weekly.copy()
    frame = frame.sort_values(["player_id", "season", "week"]).reset_index(drop=True)

    available_numeric = [column for column in BASE_NUMERIC_FEATURES if column in frame.columns]
    available_categorical = [column for column in BASE_CATEGORICAL_FEATURES if column in frame.columns]

    engineered_numeric: list[str] = []
    for column in available_numeric:
        lag1 = f"{column}_lag1"
        lag3 = f"{column}_lag3_mean"
        frame[lag1] = frame.groupby("player_id")[column].shift(1)
        frame[lag3] = frame.groupby("player_id")[column].transform(
            lambda series: series.shift(1).rolling(3, min_periods=1).mean()
        )
        engineered_numeric.extend([lag1, lag3])

    frame["week_norm"] = frame["week"] / 18.0
    frame["season_offset"] = frame["season"] - frame["season"].min()
    engineered_numeric.extend(["week_norm", "season_offset"])

    frame["target_next_week"] = frame.groupby("player_id")[TARGET_COLUMN].shift(-1)

    training = frame.dropna(subset=["target_next_week"]).copy()
    if training.empty:
        raise ValueError("No training examples after target construction.")

    features = engineered_numeric + available_categorical
    return training, features, available_categorical



def train_projection_model(
    settings: Settings,
    validation_season: int | None = None,
) -> dict[str, Any]:
    weekly = pd.read_parquet(settings.raw_weekly_path)
    training, features, categorical_features = build_training_frame(weekly)

    if validation_season is None:
        validation_season = int(training["season"].max())

    train_mask = training["season"] < validation_season
    if train_mask.sum() == 0:
        train_mask = pd.Series([True] * len(training), index=training.index)

    train_frame = training[train_mask]
    valid_frame = training[~train_mask]
    if valid_frame.empty:
        valid_frame = train_frame.tail(min(5000, len(train_frame)))

    numeric_features = [feature for feature in features if feature not in categorical_features]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric_features),
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("one_hot", _one_hot_encoder()),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=250,
        max_depth=12,
        min_samples_leaf=2,
        n_jobs=-1,
        random_state=42,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    x_train = train_frame[features]
    y_train = train_frame["target_next_week"]
    x_valid = valid_frame[features]
    y_valid = valid_frame["target_next_week"]

    pipeline.fit(x_train, y_train)
    predictions = pipeline.predict(x_valid)

    metrics = {
        "mae": float(mean_absolute_error(y_valid, predictions)),
        "r2": float(r2_score(y_valid, predictions)),
        "validation_rows": int(len(valid_frame)),
        "validation_season": int(validation_season),
    }

    latest_rows = training.sort_values(["player_id", "season", "week"]).groupby("player_id").tail(1)
    latest_rows.to_parquet(settings.player_state_path, index=False)

    artifact: dict[str, Any] = {
        "model": pipeline,
        "features": features,
        "categorical_features": categorical_features,
        "trained_at": datetime.now(UTC).isoformat(),
        "target": "target_next_week",
        "metrics": metrics,
    }
    joblib.dump(artifact, settings.projection_model_path)

    logger.info(
        "Saved projection model to %s (rows=%d, mae=%.3f, r2=%.3f)",
        settings.projection_model_path,
        len(train_frame),
        metrics["mae"],
        metrics["r2"],
    )
    return metrics
