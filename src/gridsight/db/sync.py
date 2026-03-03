from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sqlalchemy.engine import Connection

from gridsight.config import Settings
from gridsight.db.schema import (
    create_engine_from_settings,
    games,
    initialize_database,
    model_runs,
    players,
    plays,
    predictions,
    weekly_stats,
)

PLAY_COLUMNS = [
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
    "desc",
]

WEEKLY_COLUMNS = [
    "player_id",
    "player_display_name",
    "position",
    "season",
    "week",
    "recent_team",
    "opponent_team",
    "fantasy_points",
    "fantasy_points_ppr",
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



def parse_game_id(game_id: str) -> tuple[int | None, int | None, str | None, str | None]:
    parts = game_id.split("_")
    if len(parts) < 4:
        return None, None, None, None

    season: int | None = None
    week: int | None = None
    try:
        season = int(parts[0])
    except ValueError:
        season = None

    try:
        week = int(parts[1])
    except ValueError:
        week = None

    away_team = parts[2] or None
    home_team = parts[3] or None
    return season, week, away_team, home_team



def _require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file missing: {path}")



def _to_nullable_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    frame = frame.where(pd.notna(frame), None)
    return frame.to_dict(orient="records")



def _build_games_frame(pbp: pd.DataFrame) -> pd.DataFrame:
    base = pbp[[column for column in ["game_id", "season", "week"] if column in pbp.columns]].copy()
    base = base.dropna(subset=["game_id"]).drop_duplicates(subset=["game_id"]).reset_index(drop=True)

    parsed = base["game_id"].astype(str).apply(parse_game_id)
    parsed_frame = pd.DataFrame(parsed.tolist(), columns=["season_parsed", "week_parsed", "away_team", "home_team"])
    games_frame = pd.concat([base, parsed_frame], axis=1)

    if "season" in games_frame.columns:
        games_frame["season"] = games_frame["season"].fillna(games_frame["season_parsed"])
    else:
        games_frame["season"] = games_frame["season_parsed"]

    if "week" in games_frame.columns:
        games_frame["week"] = games_frame["week"].fillna(games_frame["week_parsed"])
    else:
        games_frame["week"] = games_frame["week_parsed"]

    games_frame = games_frame[["game_id", "season", "week", "away_team", "home_team"]]
    games_frame = games_frame.dropna(subset=["season", "week"])
    games_frame["season"] = games_frame["season"].astype(int)
    games_frame["week"] = games_frame["week"].astype(int)
    return games_frame



def _build_plays_frame(pbp: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in PLAY_COLUMNS if column in pbp.columns]
    frame = pbp[columns].copy()

    frame = frame.rename(columns={"desc": "description"})
    frame = frame.dropna(subset=["game_id", "play_id"])
    frame["play_id"] = pd.to_numeric(frame["play_id"], errors="coerce")
    frame = frame.dropna(subset=["play_id"])
    frame["play_id"] = frame["play_id"].astype(int)

    frame["play_uid"] = frame["game_id"].astype(str) + ":" + frame["play_id"].astype(str)
    frame = frame.drop_duplicates(subset=["play_uid"]).reset_index(drop=True)

    ordered = [
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
    return frame[[column for column in ordered if column in frame.columns]]



def _build_players_frame(weekly: pd.DataFrame) -> pd.DataFrame:
    required = [column for column in ["player_id", "season", "week"] if column in weekly.columns]
    frame = weekly.dropna(subset=required).copy()
    frame = frame.sort_values(["player_id", "season", "week"])  # keep most recent team/name/pos

    keep_columns = [
        column
        for column in ["player_id", "player_display_name", "position", "recent_team"]
        if column in frame.columns
    ]
    frame = frame[keep_columns].drop_duplicates(subset=["player_id"], keep="last")
    return frame.reset_index(drop=True)



def _build_weekly_frame(weekly: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in WEEKLY_COLUMNS if column in weekly.columns]
    frame = weekly[columns].copy()
    frame = frame.dropna(subset=["player_id", "season", "week"]).reset_index(drop=True)
    frame["season"] = pd.to_numeric(frame["season"], errors="coerce")
    frame["week"] = pd.to_numeric(frame["week"], errors="coerce")
    frame = frame.dropna(subset=["season", "week"])
    frame["season"] = frame["season"].astype(int)
    frame["week"] = frame["week"].astype(int)

    frame = frame.sort_values(["player_id", "season", "week"])
    frame = frame.drop_duplicates(subset=["player_id", "season", "week"], keep="last")
    return frame.reset_index(drop=True)



def _build_model_runs_frame(settings: Settings) -> pd.DataFrame:
    if not settings.projection_model_path.exists():
        return pd.DataFrame(columns=["model_name", "metrics", "artifact_path", "created_at"])

    artifact = joblib.load(settings.projection_model_path)
    row = {
        "model_name": "player_projection_model",
        "metrics": json.dumps(artifact.get("metrics", {}), sort_keys=True),
        "artifact_path": str(settings.projection_model_path),
        "created_at": datetime.now(UTC),
    }
    return pd.DataFrame([row])



def _build_predictions_frame(settings: Settings) -> pd.DataFrame:
    if not settings.player_state_path.exists() or not settings.projection_model_path.exists():
        return pd.DataFrame(
            columns=[
                "player_id",
                "source_season",
                "source_week",
                "projected_fantasy_points_ppr",
                "model_name",
                "created_at",
            ]
        )

    player_state = pd.read_parquet(settings.player_state_path)
    artifact = joblib.load(settings.projection_model_path)
    model = artifact["model"]
    features: list[str] = artifact["features"]

    if player_state.empty:
        return pd.DataFrame()

    x = player_state[features]
    prediction = model.predict(x)

    frame = pd.DataFrame(
        {
            "player_id": player_state["player_id"],
            "source_season": player_state["season"].astype(int),
            "source_week": player_state["week"].astype(int),
            "projected_fantasy_points_ppr": prediction.astype(float),
            "model_name": "player_projection_model",
            "created_at": datetime.now(UTC),
        }
    )
    return frame



def _truncate_tables(connection: Connection) -> None:
    for table in (predictions, model_runs, weekly_stats, plays, players, games):
        connection.execute(table.delete())



def _insert_frame(connection: Connection, table_name: str, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    records = _to_nullable_records(frame)
    pd.DataFrame.from_records(records).to_sql(
        table_name,
        con=connection,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2_000,
    )
    return len(frame)



def sync_database(
    settings: Settings,
    truncate: bool = False,
    ensure_schema: bool = True,
) -> dict[str, int]:
    _require_file(settings.raw_pbp_path)
    _require_file(settings.raw_weekly_path)

    pbp = pd.read_parquet(settings.raw_pbp_path)
    weekly = pd.read_parquet(settings.raw_weekly_path)

    game_frame = _build_games_frame(pbp)
    play_frame = _build_plays_frame(pbp)
    player_frame = _build_players_frame(weekly)
    weekly_frame = _build_weekly_frame(weekly)
    model_run_frame = _build_model_runs_frame(settings)
    prediction_frame = _build_predictions_frame(settings)

    if ensure_schema:
        initialize_database(settings, drop_existing=False)

    engine = create_engine_from_settings(settings)
    with engine.begin() as connection:
        if truncate:
            _truncate_tables(connection)

        counts = {
            "games": _insert_frame(connection, "games", game_frame),
            "plays": _insert_frame(connection, "plays", play_frame),
            "players": _insert_frame(connection, "players", player_frame),
            "weekly_stats": _insert_frame(connection, "weekly_stats", weekly_frame),
            "model_runs": _insert_frame(connection, "model_runs", model_run_frame),
            "predictions": _insert_frame(connection, "predictions", prediction_frame),
        }

    return counts
