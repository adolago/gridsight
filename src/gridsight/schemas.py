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
