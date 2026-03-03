from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any

import joblib
import pandas as pd
from sqlalchemy import text

from gridsight.config import Settings
from gridsight.db.schema import create_engine_from_settings
from gridsight.pipeline.play_vectors import transform_play_query
from gridsight.vector_store import PlayVectorStore


@dataclass
class GridSightService:
    settings: Settings

    @cached_property
    def projection_artifact(self) -> dict[str, Any]:
        return joblib.load(self.settings.projection_model_path)

    @cached_property
    def latest_player_state(self) -> pd.DataFrame:
        return pd.read_parquet(self.settings.player_state_path)

    @cached_property
    def play_embedding_artifact(self) -> dict[str, Any]:
        return joblib.load(self.settings.play_embedding_model_path)

    @cached_property
    def play_store(self) -> PlayVectorStore:
        return PlayVectorStore.from_settings(self.settings)

    def project_player(self, player_id: str) -> dict[str, Any]:
        frame = self.latest_player_state
        row = frame.loc[frame["player_id"] == player_id]
        if row.empty:
            raise KeyError(f"Player '{player_id}' not found in latest features.")

        artifact = self.projection_artifact
        model = artifact["model"]
        features = artifact["features"]

        x = row[features]
        prediction = float(model.predict(x)[0])
        latest = row.iloc[0]

        return {
            "player_id": player_id,
            "player_name": latest.get("player_display_name"),
            "position": latest.get("position"),
            "source_season": int(latest.get("season")),
            "source_week": int(latest.get("week")),
            "projected_fantasy_points_ppr": round(prediction, 2),
            "model_metrics": artifact.get("metrics", {}),
        }

    def find_similar_plays(
        self,
        query: dict[str, Any],
        limit: int,
    ) -> list[dict[str, Any]]:
        query_frame = pd.DataFrame([query])
        vectors = transform_play_query(self.play_embedding_artifact, query_frame)

        hits = self.play_store.search(vectors[0], limit=limit)
        return [
            {
                "score": hit.score,
                "payload": hit.payload,
            }
            for hit in hits
        ]

    def health_checks(self) -> dict[str, str]:
        checks = {"database": "unknown", "qdrant": "unknown"}

        try:
            engine = create_engine_from_settings(self.settings)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            checks["database"] = "ok"
        except Exception:  # noqa: BLE001
            checks["database"] = "error"

        try:
            self.play_store.client.get_collections()
            checks["qdrant"] = "ok"
        except Exception:  # noqa: BLE001
            checks["qdrant"] = "error"

        return checks
