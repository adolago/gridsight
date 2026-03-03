from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any

import joblib
import pandas as pd
from sqlalchemy import and_, func, select, text

from gridsight.config import Settings
from gridsight.db.schema import create_engine_from_settings, plays
from gridsight.pipeline.play_vectors import transform_play_query
from gridsight.vector_store import PlayVectorStore

PLAY_SORT_COLUMNS = {
    "season": plays.c.season,
    "week": plays.c.week,
    "game_id": plays.c.game_id,
    "play_id": plays.c.play_id,
    "posteam": plays.c.posteam,
    "defteam": plays.c.defteam,
    "play_type": plays.c.play_type,
    "down": plays.c.down,
    "ydstogo": plays.c.ydstogo,
    "yardline_100": plays.c.yardline_100,
    "yards_gained": plays.c.yards_gained,
    "epa": plays.c.epa,
    "wpa": plays.c.wpa,
}

PLAY_RESULT_COLUMNS = [
    "play_uid",
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
    "game_seconds_remaining",
    "score_differential",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
    "yards_gained",
    "epa",
    "wpa",
    "passer_player_id",
    "rusher_player_id",
    "receiver_player_id",
    "description",
]


@dataclass
class GridSightService:
    settings: Settings

    @cached_property
    def db_engine(self):
        return create_engine_from_settings(self.settings)

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
            with self.db_engine.connect() as connection:
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

    def get_play_filter_options(self) -> dict[str, Any]:
        with self.db_engine.connect() as connection:
            seasons = list(
                connection.execute(
                    select(plays.c.season).where(plays.c.season.is_not(None)).distinct().order_by(plays.c.season.asc())
                ).scalars()
            )
            weeks = list(
                connection.execute(
                    select(plays.c.week).where(plays.c.week.is_not(None)).distinct().order_by(plays.c.week.asc())
                ).scalars()
            )
            posteams = list(
                connection.execute(
                    select(plays.c.posteam)
                    .where(plays.c.posteam.is_not(None))
                    .distinct()
                    .order_by(plays.c.posteam.asc())
                ).scalars()
            )
            defteams = list(
                connection.execute(
                    select(plays.c.defteam)
                    .where(plays.c.defteam.is_not(None))
                    .distinct()
                    .order_by(plays.c.defteam.asc())
                ).scalars()
            )
            play_types = list(
                connection.execute(
                    select(plays.c.play_type)
                    .where(plays.c.play_type.is_not(None))
                    .distinct()
                    .order_by(plays.c.play_type.asc())
                ).scalars()
            )
            downs = list(
                connection.execute(
                    select(plays.c.down).where(plays.c.down.is_not(None)).distinct().order_by(plays.c.down.asc())
                ).scalars()
            )
            total_plays = int(connection.execute(select(func.count()).select_from(plays)).scalar_one())

        return {
            "seasons": [int(value) for value in seasons],
            "weeks": [int(value) for value in weeks],
            "posteams": [str(value) for value in posteams],
            "defteams": [str(value) for value in defteams],
            "play_types": [str(value) for value in play_types],
            "downs": [float(value) for value in downs],
            "total_plays": total_plays,
        }

    def list_plays(
        self,
        *,
        seasons: list[int] | None = None,
        weeks: list[int] | None = None,
        posteams: list[str] | None = None,
        defteams: list[str] | None = None,
        play_types: list[str] | None = None,
        downs: list[float] | None = None,
        game_id: str | None = None,
        passer_player_id: str | None = None,
        rusher_player_id: str | None = None,
        receiver_player_id: str | None = None,
        description_search: str | None = None,
        ydstogo_min: float | None = None,
        ydstogo_max: float | None = None,
        yardline_100_min: float | None = None,
        yardline_100_max: float | None = None,
        yards_gained_min: float | None = None,
        yards_gained_max: float | None = None,
        epa_min: float | None = None,
        epa_max: float | None = None,
        wpa_min: float | None = None,
        wpa_max: float | None = None,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "season",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        sort_column = PLAY_SORT_COLUMNS.get(sort_by)
        if sort_column is None:
            allowed = ", ".join(sorted(PLAY_SORT_COLUMNS))
            raise ValueError(f"Unsupported sort_by '{sort_by}'. Allowed: {allowed}")

        normalized_sort_dir = sort_dir.lower().strip()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("sort_dir must be either 'asc' or 'desc'")

        ranges = [
            ("ydstogo", ydstogo_min, ydstogo_max),
            ("yardline_100", yardline_100_min, yardline_100_max),
            ("yards_gained", yards_gained_min, yards_gained_max),
            ("epa", epa_min, epa_max),
            ("wpa", wpa_min, wpa_max),
        ]
        for label, minimum, maximum in ranges:
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValueError(f"{label}_min cannot be greater than {label}_max")

        conditions = []
        if seasons:
            conditions.append(plays.c.season.in_(seasons))
        if weeks:
            conditions.append(plays.c.week.in_(weeks))
        if posteams:
            conditions.append(plays.c.posteam.in_(posteams))
        if defteams:
            conditions.append(plays.c.defteam.in_(defteams))
        if play_types:
            conditions.append(plays.c.play_type.in_(play_types))
        if downs:
            conditions.append(plays.c.down.in_(downs))

        if game_id:
            conditions.append(plays.c.game_id == game_id.strip())
        if passer_player_id:
            conditions.append(plays.c.passer_player_id == passer_player_id.strip())
        if rusher_player_id:
            conditions.append(plays.c.rusher_player_id == rusher_player_id.strip())
        if receiver_player_id:
            conditions.append(plays.c.receiver_player_id == receiver_player_id.strip())

        if description_search:
            token = description_search.strip()
            if token:
                conditions.append(plays.c.description.ilike(f"%{token}%"))

        if ydstogo_min is not None:
            conditions.append(plays.c.ydstogo >= ydstogo_min)
        if ydstogo_max is not None:
            conditions.append(plays.c.ydstogo <= ydstogo_max)

        if yardline_100_min is not None:
            conditions.append(plays.c.yardline_100 >= yardline_100_min)
        if yardline_100_max is not None:
            conditions.append(plays.c.yardline_100 <= yardline_100_max)

        if yards_gained_min is not None:
            conditions.append(plays.c.yards_gained >= yards_gained_min)
        if yards_gained_max is not None:
            conditions.append(plays.c.yards_gained <= yards_gained_max)

        if epa_min is not None:
            conditions.append(plays.c.epa >= epa_min)
        if epa_max is not None:
            conditions.append(plays.c.epa <= epa_max)

        if wpa_min is not None:
            conditions.append(plays.c.wpa >= wpa_min)
        if wpa_max is not None:
            conditions.append(plays.c.wpa <= wpa_max)

        where_clause = and_(*conditions) if conditions else None
        order_expr = sort_column.asc() if normalized_sort_dir == "asc" else sort_column.desc()

        count_stmt = select(func.count()).select_from(plays)
        data_stmt = select(plays).order_by(order_expr, plays.c.play_uid.asc()).offset(offset).limit(limit)
        if where_clause is not None:
            count_stmt = count_stmt.where(where_clause)
            data_stmt = data_stmt.where(where_clause)

        with self.db_engine.connect() as connection:
            total = int(connection.execute(count_stmt).scalar_one())
            rows = connection.execute(data_stmt).mappings().all()

        items = []
        for row in rows:
            item = {column: row[column] for column in PLAY_RESULT_COLUMNS}
            items.append(item)

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": offset + limit < total,
            "items": items,
        }
