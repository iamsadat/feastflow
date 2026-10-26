.PHONY: help up down test lint migrate seed connect-register connect-status generate sink test-integration

help:
	@echo "targets: up down test lint migrate seed generate sink" 

up:
	docker compose up -d

down:
	docker compose down

test:
	uv run pytest

lint:
	uv run ruff check .

RUN = uv run --env-file .env python

migrate:
	$(RUN) db/migrate.py

seed:
	$(RUN) db/seed.py

generate:
	$(RUN) db/generate.py --rate 1

connect-register:
	curl -s -X PUT -H 'Content-Type: application/json' localhost:8083/connectors/feastflow-pg/config -d @connect/pg-source.json

connect-status:
	curl -s localhost:8083/connectors/feastflow-pg/status

sink:
	$(RUN) ingest/kafka_to_s3.py

test-integration:
	uv run --env-file .env pytest -m integration