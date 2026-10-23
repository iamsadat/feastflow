.PHONY: help up down test lint migrate seed connect-register connect-status

help:
	@echo "targets: up down test lint migrate seed" 

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

connect-register:
	curl -s -X PUT -H 'Content-Type: application/json' localhost:8083/connectors/feastflow-pg/config -d @connect/pg-source.json

connect-status:
	curl -s localhost:8083/connectors/feastflow-pg/status