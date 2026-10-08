.PHONY: help test lint

help:
	@echo "targets: test lint"

test:
	uv run pytest

lint:
	uv run ruff check .
