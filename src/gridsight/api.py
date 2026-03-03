from __future__ import annotations

from fastapi import FastAPI, HTTPException

from gridsight.config import get_settings
from gridsight.schemas import (
    HealthResponse,
    ProjectionResponse,
    SimilarPlayQuery,
    SimilarPlayResponse,
    SimilarPlayResult,
)
from gridsight.service import GridSightService

settings = get_settings()
service = GridSightService(settings=settings)

app = FastAPI(title="GridSight API", version="0.1.0")


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
