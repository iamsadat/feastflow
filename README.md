# FeastFlow

FeastFlow is an end-to-end lakehouse for a food-delivery business, built milestone by milestone.

An order generator writes orders into PostgreSQL. Debezium reads Postgres's write-ahead log (WAL) and publishes every insert, update and delete as a change event to Kafka. A landing sink writes those events continuously to object storage. Databricks then refines them through Bronze, Silver (CDC merge and SCD2 history) and Gold (star schema for analytics). Locally, MinIO stands in for S3, so the whole pipeline runs on a laptop.

**Status:** M0 (environment and storage) and M1 (source and CDC) are complete. M2 (Bronze, Silver, SCD2) is next; see the [roadmap](#roadmap).

## What works today

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
                                                     │ ingest/kafka_to_s3.py → MinIO s3://lake/          │
                                                     │ bronze_landing/<table>/dt=<date>/*.jsonl          │
                                                     └─────────────────────────┬────────────────────────┘
 NEXT (M2 onwards) ────────────────────────────────────────────────────────────┼────────────────────────
                                                                               ▼
                         S3 landing (feastflow-lake-<id>) ──► Lambda data-contract check
                                                                               │
                                                                               ▼
                 Databricks (Unity Catalog):  Bronze ──► Silver ──► Gold ──► Dashboards
                                            Auto Loader  CDC merge  star schema
                                                         + SCD2
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
| M2 | Bronze, Silver, SCD2: data contracts, Auto Loader into Bronze, CDC applied in order into Silver, history tables | Next |
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
| Compute | PySpark 3.5 (local), Databricks + Unity Catalog | Auto Loader, Lakeflow pipelines |
| Cloud | AWS S3, IAM, STS | Lambda, Glue/Athena, Terraform |
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

### 6. Run the tests

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
make sink                  # land Kafka events in MinIO
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
├── contracts/               # data contracts (planned, M2)
├── db/
│   ├── migrations/          # 0001_init.sql, 0002_cdc_publication.sql
│   ├── checks/              # SQL checks for the schema
│   ├── migrate.py           # migration runner
│   ├── seed.py              # reference data
│   └── generate.py          # live order generator
├── docs/adr/                # architecture decision records
├── infra/terraform/         # AWS infrastructure as code (planned, M3)
├── ingest/kafka_to_s3.py    # Kafka → object storage landing sink
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
