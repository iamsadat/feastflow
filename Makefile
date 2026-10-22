.PHONY: help up down test lint migrate seed

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
