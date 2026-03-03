from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    checks: dict[str, str] | None = None


class ProjectionResponse(BaseModel):
    player_id: str
    player_name: str | None
    position: str | None
    source_season: int
    source_week: int
    projected_fantasy_points_ppr: float
    model_metrics: dict[str, Any]


class SimilarPlayQuery(BaseModel):
    down: float = Field(default=1)
    ydstogo: float = Field(default=10)
    yardline_100: float = Field(default=75)
    game_seconds_remaining: float = Field(default=1800)
    score_differential: float = Field(default=0)
    posteam_timeouts_remaining: float = Field(default=3)
    defteam_timeouts_remaining: float = Field(default=3)
    posteam: str = Field(default="NE")
    defteam: str = Field(default="BUF")
    play_type: str = Field(default="pass")
    limit: int = Field(default=10, ge=1, le=50)


class SimilarPlayResult(BaseModel):
    score: float
    payload: dict[str, Any]


class SimilarPlayResponse(BaseModel):
    matches: list[SimilarPlayResult]


class PlayRecord(BaseModel):
    play_uid: str
    game_id: str
    play_id: int
    season: int
    week: int
    posteam: str | None = None
    defteam: str | None = None
    play_type: str | None = None
    down: float | None = None
    ydstogo: float | None = None
    yardline_100: float | None = None
    game_seconds_remaining: float | None = None
    score_differential: float | None = None
    posteam_timeouts_remaining: float | None = None
    defteam_timeouts_remaining: float | None = None
    yards_gained: float | None = None
    epa: float | None = None
    wpa: float | None = None
    passer_player_id: str | None = None
    rusher_player_id: str | None = None
    receiver_player_id: str | None = None
    description: str | None = None


class PlayListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    has_next: bool
    items: list[PlayRecord]


class PlayFilterOptionsResponse(BaseModel):
    seasons: list[int]
    weeks: list[int]
    posteams: list[str]
    defteams: list[str]
    play_types: list[str]
    downs: list[float]
    total_plays: int
