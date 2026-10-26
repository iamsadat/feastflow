.PHONY: help up down test lint migrate seed connect-register connect-status generate sink test-integration lambda-zip

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

# Lambda package: our two modules + contracts + PyYAML built for the Lambda
# runtime (Linux x86_64, Python 3.12). boto3 is already in the runtime.
LAMBDA_BUILD = build/lambda

lambda-zip:
	rm -rf $(LAMBDA_BUILD) build/contract-check.zip
	mkdir -p $(LAMBDA_BUILD)
	uv pip install pyyaml --target $(LAMBDA_BUILD) \
		--python-platform x86_64-manylinux2014 --python-version 3.12 --only-binary :all:
	cp ingest/contract.py ingest/lambda_contract_check.py $(LAMBDA_BUILD)/
	cp -r contracts $(LAMBDA_BUILD)/
	cd $(LAMBDA_BUILD) && zip -qr ../contract-check.zip .
