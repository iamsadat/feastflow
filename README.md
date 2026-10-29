# FeastFlow

FeastFlow is an end-to-end lakehouse for a food-delivery business, built milestone by milestone.

An order generator writes orders into PostgreSQL. Debezium reads Postgres's write-ahead log (WAL) and publishes every insert, update and delete as a change event to Kafka. A landing sink writes those events continuously to Amazon S3, where a Lambda function checks every line against a data contract. Databricks Auto Loader picks up the checked files as soon as they arrive and appends them to Bronze Delta tables; Silver (CDC merge and SCD2 history) and Gold (star schema for analytics) come next. MinIO can stand in for S3, so the source half also runs entirely on a laptop.

**Status:** M0 (environment and storage), M1 (source and CDC) and the Bronze half of M2 are complete. Silver and SCD2 are next; see the [roadmap](#roadmap).

## What works today

### M2: contract-checked Bronze on AWS and Databricks
- **Landing on S3.** The same sink now writes to `s3://feastflow-lake-<account-id>/bronze_landing/` in `ap-south-2` when `S3_ENDPOINT_URL` is unset. It runs as IAM user `feastflow-sink`, which may only `PutObject` under `bronze_landing/` (no read, no list).
- **Data contracts** (`contracts/<table>.yaml`): every column's Debezium-serialized type, nullability, and optional `enum`/`min` rules. `ingest/contract.py` checks the envelope (`op`, `before`/`after`) and each row; unknown columns pass so additive schema changes don't break ingestion.
- **Contract check Lambda** (`ingest/lambda_contract_check.py`, function `feastflow-contract-check`, Python 3.12, 256 MB). An EventBridge rule (`landing-to-contract-check`) invokes it for each new landing file; it writes clean lines to `bronze_validated/` and rejected lines, with the reason, to `bronze_quarantine/`, both under the original key, so re-runs are idempotent. `ingest/redrive.py` finds landing files with no validated twin and re-sends them.
- **Bronze with Auto Loader** (`ingest/bronze_autoloader.py`, `ingest/bronze_setup.sql`). One append-only Delta table per source table, `feastflow.bronze.<table>_cdc`, with an explicit schema generated from the contracts (`ingest/bronze_schema.py`), a `_rescued_data` column and source-file lineage columns. File discovery uses Databricks **managed file events** on the Unity Catalog external location `feastflow_lake`, and a Lakeflow Job with a **file arrival trigger** runs it (`availableNow`, max one concurrent run).
- **One writer per Delta table**: only Databricks writes Delta in the AWS bucket, and only one job per Bronze table.
- **Measured** (Oct 26 to 28, 2026): Lambda cold start 380 to 420 ms, peak memory 94 MB of 256 MB; validated file written about 1 s after the landing file; 9 of 9 live files checked with 0 rejected; Bronze row counts equal the validated line counts for all 6 tables (orders 13,881, order_items 10,226, riders 3,686, customers 501, menu_items 624, restaurants 51) with 0 rescued rows. A 3-minute live test was loaded by 2 file-arrival runs; a run lasts 6.5 to 9 minutes for the 6 tables, so new files reach Bronze within about one run.
- **ADR 0003**: [Bronze ingestion decisions](docs/adr/0003-bronze-ingestion.md).

### M1: source database and change data capture
- **Source schema** of 6 tables (customers, restaurants, riders, menu_items, orders, order_items), with money in integer paise, lifecycle timestamps and an `updated_at` trigger (`db/migrations/0001_init.sql`).
- **Migration runner** (`db/migrate.py`). It keeps a `schema_migrations` ledger with SHA-256 checksums, takes a Postgres advisory lock so concurrent runs are safe, and applies each file in its own transaction. It was tested with 10 runners racing on a fresh database.
- **Seed** (`db/seed.py`): 50 restaurants, 624 menu items, 500 customers and 100 riders, loaded with COPY in one transaction. It is idempotent.
- **Order generator** (`db/generate.py`). It places orders and moves them through PLACED → ACCEPTED → PREPARING → PICKED_UP → DELIVERED (or CANCELLED), claims riders with `FOR UPDATE SKIP LOCKED`, and purges abandoned orders.
- **CDC**: Postgres runs with `wal_level=logical`. Migration `0002_cdc_publication.sql` sets `REPLICA IDENTITY FULL` and creates publication `feastflow_pub`. Kafka Connect runs Debezium 3.0 (`connect/pg-source.json`), which snapshots, then streams every change to `feastflow.public.<table>` topics.
- **Landing sink** (`ingest/kafka_to_s3.py`). It writes the events as JSON lines to `s3://lake/bronze_landing/<table>/dt=<date>/`, skips tombstones, and commits offsets only after upload (at-least-once). An integration test covers it.
- **ADR 0002**: [CDC transport and landing decisions](docs/adr/0002-cdc-transport-and-landing.md).

### M0: environment and storage
- **Local stack** in Docker Compose: PostgreSQL 16, MinIO, Kafka 3.9 (KRaft), Kafka Connect, Redis 7, with named volumes so data survives restarts.
- **Delta Lake on MinIO** from PySpark (`scripts/delta_smoke.py`), and **Delta Lake on Amazon S3 from Databricks** through a Unity Catalog storage credential and external location.
- **ADR 0001**: [Delta Lake, MinIO locally and S3 in the cloud](docs/adr/0001-lakehouse-storage.md).

## Architecture

```text
 BUILT IN M1 (local) ─────────────────────────────────────────────────────────────────────────

 db/generate.py ──┐                       publication feastflow_pub
 db/seed.py ──────┤                       slot feastflow_slot (pgoutput)
                  ▼                                │
           ┌─────────────┐   WAL (logical)   ┌─────┴──────────────┐   JSON events, key = PK
           │ PostgreSQL  │ ────────────────► │ Kafka Connect      │ ─────────────────────────┐
           │ 16, 6 tables│                   │ Debezium connector │                          │
           └─────────────┘                   └────────────────────┘                          ▼
                                                                         ┌────────────────────────────┐
                                                                         │ Kafka 3.9.1 (KRaft)        │
                                                                         │ feastflow.public.<table>   │
                                                                         └─────────────┬──────────────┘
                                                                                       │ group feastflow-bronze-sink
                                                                                       ▼
                                                     ┌──────────────────────────────────────────────────┐
                                                     │ ingest/kafka_to_s3.py (IAM user feastflow-sink)   │
                                                     └─────────────────────────┬────────────────────────┘
 BUILT IN M2 (AWS ap-south-2 + Databricks) ────────────────────────────────────┼────────────────────────
                                                                               ▼
     s3://feastflow-lake-<id>/bronze_landing/<table>/dt=<date>/*.jsonl
           │ EventBridge (default bus)                │ SNS csms topic → SQS csms queue
           ▼ rule landing-to-contract-check           ▼ (Databricks managed file events)
     Lambda feastflow-contract-check           file events cache for feastflow_lake
       ├─► bronze_validated/<same key> ──(new S3 event)──┘        │
       └─► bronze_quarantine/<same key>                           ▼ file arrival trigger
                                               Job bronze_autoloader (Auto Loader, availableNow)
                                                                  ▼
                                               feastflow.bronze.<table>_cdc  (Delta, Unity Catalog)
 NEXT ─────────────────────────────────────────────────────────────────────────────────────────────
                 Silver (CDC merge by source.lsn, SCD2) ──► Gold star schema ──► Dashboards
 Later: Airflow orchestration · Redis ETA features · Terraform for AWS
```

## Source data model

```text
 ┌──────────────────┐        ┌───────────────────┐        ┌───────────────────────┐
 │ restaurants      │ 1    * │ menu_items        │ 1    * │ order_items           │
 │──────────────────│────────│───────────────────│────────│───────────────────────│
 │ restaurant_id PK │        │ item_id PK        │        │ order_item_id PK      │
 │ name, address    │        │ restaurant_id FK  │        │ order_id FK (CASCADE) │
 │ phone UNIQUE     │        │ name, description │        │ item_id FK            │
 │ city, zip        │        │ price_paise       │        │ quantity              │
 │ rating NUMERIC   │        │ is_available      │        │ unit_price_paise      │
 │ opens_at TIME    │        └───────────────────┘        │ discount_paise        │
 │ closes_at TIME   │                                     │ tax_paise             │
 └────────┬─────────┘                                     └───────────┬───────────┘
          │ 1                                                         │ *
          │                       ┌───────────────────────┐           │
          │                     * │ orders                │ 1         │
          └───────────────────────│───────────────────────│───────────┘
                                  │ order_id PK           │
 ┌──────────────────┐ 1         * │ customer_id FK        │ *         1 ┌──────────────────┐
 │ customers        │─────────────│ restaurant_id FK      │─────────────│ riders           │
 │──────────────────│             │ rider_id FK (NULL     │  0..1       │──────────────────│
 │ customer_id PK   │             │   until PICKED_UP)    │             │ rider_id PK      │
 │ name, email      │             │ total_paise           │             │ name, phone      │
 │ phone UNIQUE     │             │ status                │             │ status AVAILABLE │
 │ address, city    │             │ placed_at accepted_at │             │   /ON_DELIVERY   │
 │ zip              │             │ picked_up_at          │             │   /OFFLINE       │
 └──────────────────┘             │ delivered_at          │             │ rating NUMERIC   │
                                  │ cancelled_at          │             └──────────────────┘
                                  └───────────────────────┘
 Every table: created_at, updated_at (trigger). Money: integer paise. Times: TIMESTAMPTZ (UTC).
 Foreign keys are ON DELETE RESTRICT except order_items.order_id (CASCADE).
```

## Roadmap

| Milestone | Adds | Status |
| --- | --- | --- |
| M0 | Toolchain, local Docker stack, Delta on MinIO and on S3 via Databricks | Done |
| M1 | Source and CDC: Postgres schema, migrations, seed, order generator, Debezium into Kafka, landing in object storage | Done |
| M2 | Bronze, Silver, SCD2: data contracts, Auto Loader into Bronze, CDC applied in order into Silver, history tables | Bronze done; Silver and SCD2 next |
| M3 | Gold, governance, orchestration, AWS: star schema, access rules, Airflow, Terraform, dashboards | Planned |
| M4 | Real-time ETA features: streaming jobs writing features to Redis and Delta | Planned |
| M5 | Model and serving: a trained ETA model and an `/eta` API | Planned |
| M6 | CI/CD and cost: automated deploys, a cost report, demo | Planned |

## Tech stack

| Area | Today | Planned |
| --- | --- | --- |
| Language and tooling | Python 3.11, uv, pytest, ruff | |
| Source | PostgreSQL 16, psycopg 3 | |
| CDC | Debezium 3.0 on Kafka Connect (pgoutput) | |
| Streaming | Kafka 3.9 (KRaft), confluent-kafka | |
| Storage | MinIO (local), Amazon S3, Delta Lake | |
| Compute | PySpark 3.5 (local), Databricks + Unity Catalog, Auto Loader, Lakeflow Jobs | Lakeflow pipelines (Silver) |
| Cloud | AWS S3, IAM, STS, Lambda, EventBridge, SNS/SQS (Databricks file events), CloudWatch Logs | Glue/Athena, Terraform |
| Serving and orchestration | Redis 7 (running, not used yet) | Airflow, Redis ETA features |

## Getting started

### Prerequisites
- Docker and Docker Compose
- Python 3.11+ and [uv](https://docs.astral.sh/uv/)
- Java 17 (only for the Spark smoke test)

### 1. Configure environment variables

```bash
cp .env.example .env   # then set real passwords
```

| Variable | Used by | Local value |
| --- | --- | --- |
| `POSTGRES_PASSWORD` | Postgres, Kafka Connect | your password |
| `DATABASE_URL` | migrate, seed, generate | `postgresql://feastflow:<password>@localhost:5432/feastflow` |
| `MINIO_ROOT_PASSWORD` | MinIO | your password |
| `KAFKA_BOOTSTRAP` | sink, tests | `localhost:9092` |
| `S3_ENDPOINT_URL` | sink, tests | `http://localhost:9000` (unset = AWS S3) |
| `S3_BUCKET` | sink, tests | `lake` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | sink, tests (MinIO only) | `feastflow` / your MinIO password |
| `AWS_REGION` | sink, tests | `us-east-1` for MinIO, `ap-south-2` for AWS |
| `AWS_PROFILE` | sink (AWS only) | `feastflow-sink`; keys live in `~/.aws/credentials`, never in `.env` |

### 2. Install dependencies and start the stack

```bash
uv sync
make up
docker compose exec minio mc mb --ignore-existing local/lake   # first run only
```

### 3. Create the schema and the CDC contract

```bash
make migrate    # applies 0001_init.sql and 0002_cdc_publication.sql
make seed       # reference data; reruns print "already seeded"
```

### 4. Start change data capture

```bash
make connect-register   # creates or updates the Debezium connector
make connect-status     # connector and task should both be RUNNING
```

The first registration snapshots the seeded tables (`op: r` events), then streams live changes.

### 5. Generate traffic and land it

```bash
make generate   # terminal 1: live orders, Ctrl-C to stop
make sink       # terminal 2: Kafka → s3://lake/bronze_landing/, Ctrl-C to stop
docker compose exec minio mc ls --recursive local/lake/bronze_landing/ | head
```

### 6. Land in AWS and build Bronze (M2)

One-time AWS and Databricks setup (IAM user and roles, Lambda, EventBridge rule, Unity Catalog location with file events) is recorded in [ADR 0003](docs/adr/0003-bronze-ingestion.md). Day to day:

```bash
# .env: unset S3_ENDPOINT_URL / S3_ACCESS_KEY / S3_SECRET_KEY,
#       S3_BUCKET=feastflow-lake-<account-id>, AWS_REGION=ap-south-2, AWS_PROFILE=feastflow-sink
make sink                               # lands in s3://<bucket>/bronze_landing/
make lambda-zip                         # build/contract-check.zip for the Lambda
aws lambda update-function-code --function-name feastflow-contract-check \
  --zip-file fileb://build/contract-check.zip
uv run python ingest/redrive.py --bucket "$BUCKET" --dry-run   # landing files with no validated twin
```

In Databricks, run `ingest/bronze_setup.sql` once, then the Lakeflow Job `bronze_autoloader` (Git folder on this repo) starts on its own whenever files arrive in `bronze_validated/`.

### 7. Run the tests

```bash
make test               # unit tests, no services needed
make test-integration   # needs the stack running
make lint
```

## Useful commands

```bash
make up / make down        # start / stop the Docker services
make migrate               # apply new migrations
make seed                  # load reference data once
make generate              # live order traffic
make connect-register      # create or update the Debezium connector
make connect-status        # connector and task state
make sink                  # land Kafka events in MinIO or S3 (per .env)
make lambda-zip            # package the contract check Lambda
make test / make test-integration / make lint
```

Watch the replication slot. A stopped connector makes Postgres keep WAL, capped at 2 GB:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS behind
FROM pg_replication_slots;
```

## Repository layout

```text
.
├── connect/pg-source.json   # Debezium connector config
├── contracts/               # data contracts, one YAML per table
├── db/
│   ├── migrations/          # 0001_init.sql, 0002_cdc_publication.sql
│   ├── checks/              # SQL checks for the schema
│   ├── migrate.py           # migration runner
│   ├── seed.py              # reference data
│   └── generate.py          # live order generator
├── docs/adr/                # architecture decision records
├── infra/terraform/         # AWS infrastructure as code (planned, M3)
├── ingest/
│   ├── kafka_to_s3.py       # Kafka → object storage landing sink
│   ├── contract.py          # contract loading and line checks
│   ├── lambda_contract_check.py  # Lambda: landing → validated / quarantine
│   ├── redrive.py           # re-send landing files the check missed
│   ├── bronze_schema.py     # Spark schema for Bronze, built from contracts
│   ├── bronze_autoloader.py # Databricks notebook: Auto Loader → Bronze
│   └── bronze_setup.sql     # catalog and schema setup
├── orchestration/           # Airflow DAGs (planned, M3)
├── scripts/                 # helper scripts, incl. delta_smoke.py
├── src/feastflow/           # Python package source
├── tests/
│   ├── unit/                # no services needed
│   └── integration/         # needs the compose stack
├── docker-compose.yml       # local services incl. Kafka Connect
├── Makefile                 # common commands
├── pyproject.toml           # metadata, dependencies, pytest markers
├── .env.example             # environment template
└── uv.lock                  # locked dependency versions
```
