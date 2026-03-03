# Deploy GridSight on an Edge VM

## 1. Host prerequisites

- Docker and Docker Compose (`docker-compose` or `docker compose`)
- A non-root deploy user with Docker access
- Reverse proxy or tunnel in front of the app if exposing publicly

## 2. App setup

```bash
mkdir -p /opt/docker/gridsight
cd /opt/docker/gridsight
# clone or rsync this repo into this directory
```

Create runtime env file:

```bash
cp .env.example .env
```

Recommended `.env` overrides:

```bash
POSTGRES_PASSWORD=<strong-random-password>
GRIDSIGHT_API_PORT=8010
GRIDSIGHT_DATABASE_URL=postgresql+psycopg://gridsight:${POSTGRES_PASSWORD}@postgres:5432/gridsight
GRIDSIGHT_QDRANT_URL=http://qdrant:6333
```

## 3. Start stack

```bash
docker compose --env-file .env up -d --build
```

## 4. Build data + DB

```bash
docker compose --env-file .env exec api gridsight bootstrap --start-season 2018 --end-season 2025 --max-rows 50000
docker compose --env-file .env exec api gridsight db-bootstrap --drop-existing
```

## 5. Verify

```bash
curl -sS http://127.0.0.1:8010/health
docker compose --env-file .env ps
docker compose --env-file .env logs -f --tail=200 api
```

## 6. Update workflow

```bash
cd /opt/docker/gridsight
git pull
docker compose --env-file .env up -d --build
docker compose --env-file .env exec api gridsight db-load --truncate
```
