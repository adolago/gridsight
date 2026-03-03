from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi import Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from gridsight.config import get_settings
from gridsight.schemas import (
    HealthResponse,
    PlayFilterOptionsResponse,
    PlayListResponse,
    ProjectionResponse,
    SimilarPlayQuery,
    SimilarPlayResponse,
    SimilarPlayResult,
)
from gridsight.service import GridSightService

settings = get_settings()
service = GridSightService(settings=settings)

app = FastAPI(title="GridSight API", version="0.1.0")
web_dir = Path(__file__).resolve().parent / "web"
static_dir = web_dir / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", include_in_schema=False)
def ui_index() -> FileResponse:
    index_path = web_dir / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="UI not found.")
    return FileResponse(index_path)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    checks = service.health_checks()
    status = "ok" if all(value == "ok" for value in checks.values()) else "degraded"
    return HealthResponse(status=status, checks=checks)


@app.get("/v1/projections/{player_id}", response_model=ProjectionResponse)
def get_player_projection(player_id: str) -> ProjectionResponse:
    try:
        result = service.project_player(player_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail="Model artifacts not found. Run training pipeline.") from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return ProjectionResponse(**result)


@app.post("/v1/similar-plays", response_model=SimilarPlayResponse)
def similar_plays(query: SimilarPlayQuery) -> SimilarPlayResponse:
    try:
        payload = query.model_dump()
        limit = payload.pop("limit")
        matches = service.find_similar_plays(payload, limit=limit)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail="Vector artifacts not found. Run vector indexing pipeline.") from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return SimilarPlayResponse(matches=[SimilarPlayResult(**row) for row in matches])


@app.get("/v1/plays/filter-options", response_model=PlayFilterOptionsResponse)
def play_filter_options() -> PlayFilterOptionsResponse:
    try:
        options = service.get_play_filter_options()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Failed to load filter options: {exc}") from exc
    return PlayFilterOptionsResponse(**options)


@app.get("/v1/plays", response_model=PlayListResponse)
def list_plays(
    season: list[int] | None = Query(default=None),
    week: list[int] | None = Query(default=None),
    posteam: list[str] | None = Query(default=None),
    defteam: list[str] | None = Query(default=None),
    play_type: list[str] | None = Query(default=None),
    down: list[float] | None = Query(default=None),
    game_id: str | None = Query(default=None),
    passer_player_id: str | None = Query(default=None),
    rusher_player_id: str | None = Query(default=None),
    receiver_player_id: str | None = Query(default=None),
    description_search: str | None = Query(default=None),
    ydstogo_min: float | None = Query(default=None),
    ydstogo_max: float | None = Query(default=None),
    yardline_100_min: float | None = Query(default=None),
    yardline_100_max: float | None = Query(default=None),
    yards_gained_min: float | None = Query(default=None),
    yards_gained_max: float | None = Query(default=None),
    epa_min: float | None = Query(default=None),
    epa_max: float | None = Query(default=None),
    wpa_min: float | None = Query(default=None),
    wpa_max: float | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="season"),
    sort_dir: str = Query(default="desc"),
) -> PlayListResponse:
    try:
        payload = service.list_plays(
            seasons=season,
            weeks=week,
            posteams=posteam,
            defteams=defteam,
            play_types=play_type,
            downs=down,
            game_id=game_id,
            passer_player_id=passer_player_id,
            rusher_player_id=rusher_player_id,
            receiver_player_id=receiver_player_id,
            description_search=description_search,
            ydstogo_min=ydstogo_min,
            ydstogo_max=ydstogo_max,
            yardline_100_min=yardline_100_min,
            yardline_100_max=yardline_100_max,
            yards_gained_min=yards_gained_min,
            yards_gained_max=yards_gained_max,
            epa_min=epa_min,
            epa_max=epa_max,
            wpa_min=wpa_min,
            wpa_max=wpa_max,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Failed to query plays: {exc}") from exc

    return PlayListResponse(**payload)
