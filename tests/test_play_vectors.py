from __future__ import annotations

import pandas as pd

from gridsight.pipeline.play_vectors import _fit_embedding_model, transform_play_query



def test_play_embedding_and_query_transform() -> None:
    plays = pd.DataFrame(
        {
            "down": [1, 2, 3, 1],
            "ydstogo": [10, 7, 3, 6],
            "yardline_100": [75, 52, 35, 20],
            "game_seconds_remaining": [3200, 2500, 1200, 600],
            "score_differential": [0, 7, -3, 4],
            "posteam_timeouts_remaining": [3, 3, 2, 1],
            "defteam_timeouts_remaining": [3, 2, 2, 1],
            "posteam": ["NE", "BUF", "KC", "DAL"],
            "defteam": ["NYJ", "MIA", "LAC", "PHI"],
            "play_type": ["pass", "run", "pass", "run"],
        }
    )

    artifact, vectors = _fit_embedding_model(plays, embedding_dim=8)

    assert vectors.shape[0] == len(plays)
    assert vectors.shape[1] <= 8

    query = pd.DataFrame(
        [
            {
                "down": 2,
                "ydstogo": 8,
                "yardline_100": 60,
                "game_seconds_remaining": 2100,
                "score_differential": 3,
                "posteam_timeouts_remaining": 3,
                "defteam_timeouts_remaining": 2,
                "posteam": "NE",
                "defteam": "MIA",
                "play_type": "pass",
            }
        ]
    )
    query_vec = transform_play_query(artifact, query)

    assert query_vec.shape[0] == 1
    assert query_vec.shape[1] == vectors.shape[1]
