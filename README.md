# FeastFlow

FeastFlow is an end-to-end lakehouse for a food-delivery business, built milestone by milestone.

An order generator writes orders into PostgreSQL. Debezium reads Postgres's write-ahead log (WAL) and publishes every insert, update and delete as a change event to Kafka. Those events land continuously in a Delta Lake on Amazon S3. Databricks then refines them through Bronze, Silver (CDC merge and SCD2 history) and Gold (star schema for analytics). Locally, MinIO stands in for S3, so the whole storage layer can run on a laptop.

**Status:** M0 (environment and storage foundation) is complete. Everything from M1 onwards is planned; see the [roadmap](#roadmap).

## What works today (M0)

- **Local stack** in Docker Compose: PostgreSQL 16, MinIO (S3-compatible), Kafka 3.9 in KRaft mode and Redis 7, with named volumes so data survives restarts.
- **Delta Lake on MinIO** from PySpark (Spark 3.5.3, Delta 3.2.1): write, read, UPDATE and table history (`scripts/delta_smoke.py`).
- **Delta Lake on Amazon S3 from Databricks**: an S3 bucket in `ap-south-2` (Block Public Access on), a cross-account IAM role assumed by Databricks with an ExternalId, and a Unity Catalog storage credential plus external location. A test table was written, updated (deletion vector) and auto-compacted, and its `_delta_log` was verified from the AWS CLI.
- **ADR 0001**: [why Delta Lake, MinIO locally and S3 in the cloud](docs/adr/0001-lakehouse-storage.md).

## Architecture (target)

```text
 Order generator ──> PostgreSQL ──WAL──> Debezium ──> Kafka ──> S3 landing
                                                                   │
                                          Lambda data-contract check
                                                                   │
                                                                   ▼
          Databricks (Unity Catalog):  Bronze ──> Silver ──> Gold ──> Dashboards
                                     Auto Loader  CDC merge  star schema
                                                  + SCD2

 Local dev: MinIO replaces S3 · Airflow orchestrates · Redis serves ETA features
 Cloud infra: Terraform (S3, IAM, Lambda, Glue/Athena)
```

## Roadmap

| Milestone | Adds | Status |
| --- | --- | --- |
| M0 | Toolchain, local Docker stack, Delta on MinIO and on S3 via Databricks | Done |
| M1 | Source and CDC: Postgres schema, order generator, Debezium into Kafka, files landing in object storage | Planned |
| M2 | Bronze, Silver, SCD2: data contracts, Auto Loader into Bronze, CDC applied in order into Silver, history tables | Planned |
| M3 | Gold, governance, orchestration, AWS: star schema, access rules, Airflow, Terraform, dashboards | Planned |
| M4 | Real-time ETA features: streaming jobs writing features to Redis and Delta | Planned |
| M5 | Model and serving: a trained ETA model and an `/eta` API | Planned |
| M6 | CI/CD and cost: automated deploys, a cost report, demo | Planned |

## Tech stack

| Area | Today (M0) | Planned |
| --- | --- | --- |
| Language and tooling | Python 3.11, uv, pytest, ruff | |
| Source and CDC | PostgreSQL 16 | Debezium |
| Streaming | Kafka 3.9 (KRaft) | |
| Storage | Delta Lake on MinIO (local) and Amazon S3 | |
| Compute | PySpark 3.5 (local), Databricks + Unity Catalog | Auto Loader, Lakeflow pipelines |
| Cloud | AWS S3, IAM, STS | Lambda, Glue/Athena, Terraform |
| Serving and orchestration | Redis 7 (running, not used yet) | Airflow, Redis ETA features |

## Getting started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ and [uv](https://docs.astral.sh/uv/)
- Java 17 (needed by Spark for the Delta smoke test)

### 1. Configure environment variables

```bash
cp .env.example .env   # then set real passwords
```

### 2. Install dependencies

```bash
uv sync
```

### 3. Start the local services

```bash
make up
```

This starts PostgreSQL, MinIO, Kafka and Redis from `docker-compose.yml`.

### 4. Run the tests and linter

```bash
make test
make lint
```

### 5. Run the Delta smoke test against MinIO

```bash
uv run --group spark --env-file .env python scripts/delta_smoke.py
```

It writes a small `orders` Delta table to `s3a://lake/smoke/orders` on MinIO, updates one row, and prints the table history. It needs `MINIO_ROOT_PASSWORD` from `.env`.

## Useful commands

```bash
make up      # start the Docker services
make down    # stop the Docker services
make test    # run the tests
make lint    # run ruff checks
```

## Repository layout

```text
.
├── contracts/           # data contracts (planned, M2)
├── docs/adr/            # architecture decision records
├── infra/terraform/     # AWS infrastructure as code (planned, M3)
├── orchestration/       # Airflow DAGs (planned, M3)
├── scripts/             # helper scripts, incl. delta_smoke.py
├── src/feastflow/       # Python package source
├── tests/               # unit and integration tests
├── docker-compose.yml   # local service definitions
├── Makefile             # common development commands
├── pyproject.toml       # project metadata and dependencies
├── .env.example         # example environment configuration
└── uv.lock              # locked dependency versions
```
