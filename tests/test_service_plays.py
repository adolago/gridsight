from __future__ import annotations

from pathlib import Path

import pytest

from gridsight.config import Settings
from gridsight.db.schema import create_engine_from_settings, games, initialize_database, plays
from gridsight.service import GridSightService


def _build_settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    models_dir = data_dir / "models"
    vector_db_dir = data_dir / "vector_db"

    for directory in (raw_dir, processed_dir, models_dir, vector_db_dir):
        directory.mkdir(parents=True, exist_ok=True)

    db_path = tmp_path / "gridsight.db"

    return Settings(
        project_root=tmp_path,
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
        qdrant_collection="plays",
        qdrant_url=None,
        qdrant_api_key=None,
        qdrant_timeout_seconds=30.0,
        database_url=f"sqlite+pysqlite:///{db_path}",
    )


def _seed_sample_plays(settings: Settings) -> None:
    initialize_database(settings=settings, drop_existing=True)
    engine = create_engine_from_settings(settings)

    with engine.begin() as connection:
        connection.execute(
            games.insert(),
            [
                {
                    "game_id": "2024_01_KC_BAL",
                    "season": 2024,
                    "week": 1,
                    "away_team": "KC",
                    "home_team": "BAL",
                },
                {
                    "game_id": "2024_02_BUF_MIA",
                    "season": 2024,
                    "week": 2,
                    "away_team": "BUF",
                    "home_team": "MIA",
                },
                {
                    "game_id": "2023_17_DAL_PHI",
                    "season": 2023,
                    "week": 17,
                    "away_team": "DAL",
                    "home_team": "PHI",
                },
            ],
        )

        connection.execute(
            plays.insert(),
            [
                {
                    "play_uid": "2024_01_KC_BAL:1",
                    "game_id": "2024_01_KC_BAL",
                    "play_id": 1,
                    "season": 2024,
                    "week": 1,
                    "posteam": "KC",
                    "defteam": "BAL",
                    "play_type": "pass",
                    "down": 1.0,
                    "ydstogo": 10.0,
                    "yards_gained": 11.0,
                    "epa": 0.35,
                    "wpa": 0.02,
                    "description": "Mahomes short pass to Kelce for 11 yards",
                },
                {
                    "play_uid": "2024_01_KC_BAL:2",
                    "game_id": "2024_01_KC_BAL",
                    "play_id": 2,
                    "season": 2024,
                    "week": 1,
                    "posteam": "KC",
                    "defteam": "BAL",
                    "play_type": "run",
                    "down": 2.0,
                    "ydstogo": 4.0,
                    "yards_gained": 3.0,
                    "epa": -0.15,
                    "wpa": -0.01,
                    "description": "Pacheco up the middle for 3 yards",
                },
                {
                    "play_uid": "2024_02_BUF_MIA:3",
                    "game_id": "2024_02_BUF_MIA",
                    "play_id": 3,
                    "season": 2024,
                    "week": 2,
                    "posteam": "BUF",
                    "defteam": "MIA",
                    "play_type": "pass",
                    "down": 3.0,
                    "ydstogo": 7.0,
                    "yards_gained": 22.0,
                    "epa": 0.82,
                    "wpa": 0.06,
                    "description": "Allen deep pass complete for 22 yards",
                },
                {
                    "play_uid": "2023_17_DAL_PHI:4",
                    "game_id": "2023_17_DAL_PHI",
                    "play_id": 4,
                    "season": 2023,
                    "week": 17,
                    "posteam": "DAL",
                    "defteam": "PHI",
                    "play_type": "run",
                    "down": 1.0,
                    "ydstogo": 10.0,
                    "yards_gained": 8.0,
                    "epa": 0.18,
                    "wpa": 0.01,
                    "description": "Pollard left tackle for 8 yards",
                },
            ],
        )


def test_list_plays_filters_by_team_and_epa(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _seed_sample_plays(settings)
    service = GridSightService(settings=settings)

    result = service.list_plays(
        posteams=["KC"],
        epa_min=0.1,
        epa_max=0.5,
        sort_by="epa",
        sort_dir="desc",
        limit=50,
        offset=0,
    )

    assert result["total"] == 1
    assert len(result["items"]) == 1
    assert result["items"][0]["play_uid"] == "2024_01_KC_BAL:1"


def test_list_plays_paginates_and_sorts(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _seed_sample_plays(settings)
    service = GridSightService(settings=settings)

    result = service.list_plays(sort_by="play_id", sort_dir="asc", limit=2, offset=1)

    assert result["total"] == 4
    assert len(result["items"]) == 2
    assert result["items"][0]["play_id"] == 2
    assert result["items"][1]["play_id"] == 3
    assert result["has_next"] is True


def test_get_play_filter_options_returns_distinct_values(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _seed_sample_plays(settings)
    service = GridSightService(settings=settings)

    options = service.get_play_filter_options()

    assert options["seasons"] == [2023, 2024]
    assert options["weeks"] == [1, 2, 17]
    assert options["play_types"] == ["pass", "run"]
    assert options["posteams"] == ["BUF", "DAL", "KC"]
    assert options["defteams"] == ["BAL", "MIA", "PHI"]
    assert options["total_plays"] == 4


def test_list_plays_rejects_invalid_sort_column(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    _seed_sample_plays(settings)
    service = GridSightService(settings=settings)

    with pytest.raises(ValueError, match="Unsupported sort_by"):
        service.list_plays(sort_by="not_a_column")
