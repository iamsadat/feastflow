.PHONY: help up down test lint

help:
	@echo "targets: up down test lint"

up:
	docker compose up -d

down:
	docker compose down

test:
	uv run pytest

lint:
	uv run ruff check .
