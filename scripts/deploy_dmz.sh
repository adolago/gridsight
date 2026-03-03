#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/docker/gridsight}"
ENV_FILE="${ENV_FILE:-${APP_DIR}/.env}"
START_SEASON="${GRIDSIGHT_START_SEASON:-2018}"
END_SEASON="${GRIDSIGHT_END_SEASON:-2025}"
MAX_ROWS="${GRIDSIGHT_MAX_ROWS:-50000}"

cd "$APP_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: missing env file at $ENV_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

API_PORT="${GRIDSIGHT_API_PORT:-8000}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  COMPOSE=(docker compose)
fi

echo "[deploy] using compose command: ${COMPOSE[*]}"

echo "[deploy] starting/updating containers"
"${COMPOSE[@]}" --env-file "$ENV_FILE" up -d --build

HAS_ARTIFACTS=0
if [[ -f data/raw/pbp.parquet \
   && -f data/raw/weekly.parquet \
   && -f data/processed/latest_player_features.parquet \
   && -f data/models/player_projection_model.joblib \
   && -f data/models/play_embedding_model.joblib ]]; then
  HAS_ARTIFACTS=1
fi

if [[ "$HAS_ARTIFACTS" -eq 1 ]]; then
  echo "[deploy] artifacts found, refreshing DB only"
  "${COMPOSE[@]}" --env-file "$ENV_FILE" exec -T api gridsight db-load --truncate
else
  echo "[deploy] artifacts missing, running full bootstrap"
  "${COMPOSE[@]}" --env-file "$ENV_FILE" exec -T api gridsight bootstrap \
    --start-season "$START_SEASON" \
    --end-season "$END_SEASON" \
    --max-rows "$MAX_ROWS"
  "${COMPOSE[@]}" --env-file "$ENV_FILE" exec -T api gridsight db-bootstrap --drop-existing
fi

echo "[deploy] health check"
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${API_PORT}/health" >/tmp/gridsight-health.json 2>/dev/null; then
    cat /tmp/gridsight-health.json
    rm -f /tmp/gridsight-health.json
    echo
    echo "[deploy] success"
    exit 0
  fi
  sleep 2
done

echo "[deploy] ERROR: health endpoint did not become ready"
"${COMPOSE[@]}" --env-file "$ENV_FILE" logs --tail=200 api || true
exit 1
