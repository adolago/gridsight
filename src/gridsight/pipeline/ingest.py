from __future__ import annotations

import logging
from typing import Sequence
from urllib.error import HTTPError

import pandas as pd

from gridsight.config import Settings

logger = logging.getLogger(__name__)

PBP_COLUMNS = [
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



def _is_http_404(exc: Exception) -> bool:
    return (isinstance(exc, HTTPError) and exc.code == 404) or "HTTP Error 404" in str(exc)



def _select_available_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    keep = [column for column in columns if column in frame.columns]
    return frame[keep]



def _import_pbp_one_season(season: int, columns: list[str]) -> pd.DataFrame:
    import nfl_data_py as nfl  # Lazy import keeps local tooling usable without full deps.

    try:
        return nfl.import_pbp_data([season], columns=columns, downcast=True, cache=False)
    except TypeError:
        frame = nfl.import_pbp_data([season], downcast=True, cache=False)
        return _select_available_columns(frame, columns)



def _import_weekly_one_season(season: int, columns: list[str]) -> pd.DataFrame:
    import nfl_data_py as nfl  # Lazy import keeps local tooling usable without full deps.

    try:
        frame = nfl.import_weekly_data([season], downcast=True)
    except TypeError:
        frame = nfl.import_weekly_data([season], downcast=True)
    return _select_available_columns(frame, columns)



def _import_pbp_data(seasons: Sequence[int], columns: list[str]) -> pd.DataFrame:
    import nfl_data_py as nfl  # Lazy import keeps local tooling usable without full deps.

    try:
        return nfl.import_pbp_data(list(seasons), columns=columns, downcast=True, cache=False)
    except TypeError:
        frame = nfl.import_pbp_data(list(seasons), downcast=True, cache=False)
        return _select_available_columns(frame, columns)
    except Exception as exc:  # noqa: BLE001
        if not _is_http_404(exc):
            raise
        logger.warning(
            "Bulk PBP pull failed due to missing season data. Falling back to per-season import."
        )

    frames: list[pd.DataFrame] = []
    skipped: list[int] = []
    for season in seasons:
        try:
            frames.append(_import_pbp_one_season(int(season), columns))
        except Exception as exc:  # noqa: BLE001
            if _is_http_404(exc):
                skipped.append(int(season))
                logger.warning("Skipping PBP season %s (upstream data not found).", season)
                continue
            raise

    if not frames:
        raise RuntimeError("No play-by-play data could be imported for the requested seasons.")
    if skipped:
        logger.info("Imported PBP data with skipped seasons: %s", skipped)
    return pd.concat(frames, ignore_index=True)



def _import_weekly_data(seasons: Sequence[int], columns: list[str]) -> pd.DataFrame:
    import nfl_data_py as nfl  # Lazy import keeps local tooling usable without full deps.

    try:
        frame = nfl.import_weekly_data(list(seasons), downcast=True)
        return _select_available_columns(frame, columns)
    except TypeError:
        frame = nfl.import_weekly_data(list(seasons), downcast=True)
        return _select_available_columns(frame, columns)
    except Exception as exc:  # noqa: BLE001
        if not _is_http_404(exc):
            raise
        logger.warning(
            "Bulk weekly pull failed due to missing season data. Falling back to per-season import."
        )

    frames: list[pd.DataFrame] = []
    skipped: list[int] = []
    for season in seasons:
        try:
            frames.append(_import_weekly_one_season(int(season), columns))
        except Exception as exc:  # noqa: BLE001
            if _is_http_404(exc):
                skipped.append(int(season))
                logger.warning("Skipping weekly season %s (upstream data not found).", season)
                continue
            raise

    if not frames:
        raise RuntimeError("No weekly data could be imported for the requested seasons.")
    if skipped:
        logger.info("Imported weekly data with skipped seasons: %s", skipped)
    return pd.concat(frames, ignore_index=True)



def ingest_nfl_data(
    settings: Settings,
    start_season: int,
    end_season: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    seasons = list(range(start_season, end_season + 1))
    logger.info("Ingesting NFL data for seasons %s", seasons)

    pbp = _import_pbp_data(seasons, PBP_COLUMNS)
    pbp = pbp.copy()
    pbp = pbp[pbp["play_type"].isin(["run", "pass"])]
    pbp["play_id"] = pd.to_numeric(pbp.get("play_id"), errors="coerce").fillna(-1).astype(int)
    pbp.to_parquet(settings.raw_pbp_path, index=False)

    weekly = _import_weekly_data(seasons, WEEKLY_COLUMNS)
    weekly = weekly.copy()
    weekly["week"] = pd.to_numeric(weekly.get("week"), errors="coerce")
    weekly = weekly[weekly["week"].notna()]
    weekly["week"] = weekly["week"].astype(int)
    weekly.to_parquet(settings.raw_weekly_path, index=False)

    logger.info(
        "Saved data: pbp rows=%d path=%s, weekly rows=%d path=%s",
        len(pbp),
        settings.raw_pbp_path,
        len(weekly),
        settings.raw_weekly_path,
    )
    return pbp, weekly
