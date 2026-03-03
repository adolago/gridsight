from __future__ import annotations

import pandas as pd

from gridsight.pipeline.projections import build_training_frame



def test_build_training_frame_creates_lag_features() -> None:
    frame = pd.DataFrame(
        {
            "player_id": ["a", "a", "a", "b", "b", "b"],
            "player_display_name": ["A"] * 3 + ["B"] * 3,
            "position": ["WR", "WR", "WR", "RB", "RB", "RB"],
            "season": [2024, 2024, 2024, 2024, 2024, 2024],
            "week": [1, 2, 3, 1, 2, 3],
            "recent_team": ["AAA"] * 3 + ["BBB"] * 3,
            "opponent_team": ["CCC"] * 6,
            "fantasy_points_ppr": [10.0, 12.0, 14.0, 9.0, 11.0, 8.0],
            "fantasy_points": [8.0, 10.0, 12.0, 7.0, 9.0, 6.0],
            "targets": [6, 7, 8, 2, 3, 1],
            "receptions": [4, 5, 6, 1, 2, 1],
            "receiving_yards": [50, 60, 70, 5, 10, 4],
            "receiving_tds": [0, 1, 1, 0, 0, 0],
            "carries": [0, 0, 0, 12, 13, 9],
            "rushing_yards": [0, 0, 0, 55, 62, 48],
            "rushing_tds": [0, 0, 0, 0, 1, 0],
            "passing_yards": [0, 0, 0, 0, 0, 0],
            "passing_tds": [0, 0, 0, 0, 0, 0],
            "interceptions": [0, 0, 0, 0, 0, 0],
            "completions": [0, 0, 0, 0, 0, 0],
            "attempts": [0, 0, 0, 0, 0, 0],
            "air_yards": [80, 95, 110, 0, 0, 0],
        }
    )

    training, features, categorical = build_training_frame(frame)

    assert len(training) == 4
    assert "fantasy_points_ppr_lag1" in features
    assert "fantasy_points_ppr_lag3_mean" in features
    assert set(categorical) == {"position", "recent_team", "opponent_team"}
