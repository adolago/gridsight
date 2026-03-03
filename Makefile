.PHONY: test lint ingest train index bootstrap api db-init db-load db-bootstrap docker-up docker-down docker-logs

test:
	pytest

lint:
	ruff check src tests

ingest:
	gridsight ingest --start-season 2018 --end-season 2025

train:
	gridsight train --validation-season 2025

index:
	gridsight index --embedding-dim 48 --max-rows 250000

bootstrap:
	gridsight bootstrap --start-season 2018 --end-season 2025 --validation-season 2025 --embedding-dim 48 --max-rows 250000

api:
	gridsight api --reload

db-init:
	gridsight db-init

db-load:
	gridsight db-load

db-bootstrap:
	gridsight db-bootstrap

docker-up:
	docker compose --env-file .env up -d --build

docker-down:
	docker compose --env-file .env down

docker-logs:
	docker compose --env-file .env logs -f --tail=200
