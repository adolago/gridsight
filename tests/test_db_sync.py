from __future__ import annotations

import pandas as pd

from gridsight.db.sync import _build_games_frame, parse_game_id



def test_parse_game_id() -> None:
    season, week, away, home = parse_game_id("2024_17_KC_DEN")
    assert season == 2024
    assert week == 17
    assert away == "KC"
    assert home == "DEN"



def test_build_games_frame_parses_teams_from_game_id() -> None:
    pbp = pd.DataFrame(
        {
            "game_id": ["2024_01_KC_BAL", "2024_01_KC_BAL", "2024_01_BUF_MIA"],
            "season": [2024, 2024, 2024],
            "week": [1, 1, 1],
        }
    )

    games = _build_games_frame(pbp)

    assert len(games) == 2
    kc_bal = games.loc[games["game_id"] == "2024_01_KC_BAL"].iloc[0]
    assert kc_bal["away_team"] == "KC"
    assert kc_bal["home_team"] == "BAL"
