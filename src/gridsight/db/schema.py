from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
    text,
)
from sqlalchemy.engine import Engine

from gridsight.config import Settings

metadata = MetaData()


games = Table(
    "games",
    metadata,
    Column("game_id", String(32), primary_key=True),
    Column("season", Integer, nullable=False),
    Column("week", Integer, nullable=False),
    Column("away_team", String(3)),
    Column("home_team", String(3)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

plays = Table(
    "plays",
    metadata,
    Column("play_uid", String(64), primary_key=True),
    Column("game_id", String(32), ForeignKey("games.game_id", ondelete="CASCADE"), nullable=False),
    Column("play_id", Integer, nullable=False),
    Column("season", Integer, nullable=False),
    Column("week", Integer, nullable=False),
    Column("posteam", String(3)),
    Column("defteam", String(3)),
    Column("play_type", String(16)),
    Column("down", Float),
    Column("ydstogo", Float),
    Column("yardline_100", Float),
    Column("game_seconds_remaining", Float),
    Column("score_differential", Float),
    Column("posteam_timeouts_remaining", Float),
    Column("defteam_timeouts_remaining", Float),
    Column("yards_gained", Float),
    Column("epa", Float),
    Column("wpa", Float),
    Column("passer_player_id", String(32)),
    Column("rusher_player_id", String(32)),
    Column("receiver_player_id", String(32)),
    Column("description", Text),
)

Index("ix_plays_game_id", plays.c.game_id)
Index("ix_plays_season_week", plays.c.season, plays.c.week)

players = Table(
    "players",
    metadata,
    Column("player_id", String(32), primary_key=True),
    Column("player_display_name", String(128)),
    Column("position", String(8)),
    Column("recent_team", String(3)),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

weekly_stats = Table(
    "weekly_stats",
    metadata,
    Column("player_id", String(32), ForeignKey("players.player_id", ondelete="CASCADE"), primary_key=True),
    Column("season", Integer, primary_key=True),
    Column("week", Integer, primary_key=True),
    Column("player_display_name", String(128)),
    Column("recent_team", String(3)),
    Column("opponent_team", String(3)),
    Column("position", String(8)),
    Column("fantasy_points", Float),
    Column("fantasy_points_ppr", Float),
    Column("targets", Float),
    Column("receptions", Float),
    Column("receiving_yards", Float),
    Column("receiving_tds", Float),
    Column("carries", Float),
    Column("rushing_yards", Float),
    Column("rushing_tds", Float),
    Column("passing_yards", Float),
    Column("passing_tds", Float),
    Column("interceptions", Float),
    Column("completions", Float),
    Column("attempts", Float),
    Column("air_yards", Float),
)

Index("ix_weekly_stats_season_week", weekly_stats.c.season, weekly_stats.c.week)

model_runs = Table(
    "model_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("model_name", String(64), nullable=False),
    Column("metrics", Text, nullable=False),
    Column("artifact_path", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

predictions = Table(
    "predictions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("player_id", String(32), ForeignKey("players.player_id", ondelete="CASCADE"), nullable=False),
    Column("source_season", Integer, nullable=False),
    Column("source_week", Integer, nullable=False),
    Column("projected_fantasy_points_ppr", Float, nullable=False),
    Column("model_name", String(64), nullable=False, server_default=text("'player_projection_model'")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

Index("ix_predictions_player_id", predictions.c.player_id)



def create_engine_from_settings(settings: Settings) -> Engine:
    return create_engine(settings.database_url, future=True)



def initialize_database(settings: Settings, drop_existing: bool = False) -> None:
    engine = create_engine_from_settings(settings)
    with engine.begin() as connection:
        if drop_existing:
            metadata.drop_all(connection)
        metadata.create_all(connection)
