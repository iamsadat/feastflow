# 0002. Change data capture with Debezium, schemaless JSON in Kafka, Python landing sink

- Status: Accepted
- Date: 2026-10-23

## Context
M1 turns the FeastFlow source database into a stream of change events and lands
them in object storage, so that M2 can build Bronze, Silver (CDC merge, SCD2)
and Gold from a complete, replayable history.

- The source is PostgreSQL 16 with 6 tables (customers, restaurants, riders,
  menu_items, orders, order_items). The generator changes them continuously:
  inserts, multi-step status updates and purges with cascading deletes.
- Silver needs every change, in commit order per row, including the previous
  values on updates (status from -> to) and the full row on deletes.
- Everything runs on one laptop (16 GB RAM, single Kafka broker), and cloud
  spend must stay near zero during development.
- The project is learning-first: every hop should be observable with plain
  tools (psql, kafka-console-consumer, mc).

```text
 Postgres (wal_level=logical)
   │  slot feastflow_slot ── pgoutput ── publication feastflow_pub (6 tables)
   ▼
 Kafka Connect ── Debezium PostgresConnector (1 task, JSON, key = primary key)
   ▼
 Kafka topics feastflow.public.<table>  (1 partition each)
   ▼
 ingest/kafka_to_s3.py  (group feastflow-bronze-sink, commit after upload)
   ▼
 MinIO s3://lake/bronze_landing/<table>/dt=<date>/<topic>+<partition>+<first>-<last>.jsonl
```

## Decision
- **Log-based CDC with Debezium (pgoutput, slot `feastflow_slot`).** Changes
  are read from the WAL through a logical replication slot. That captures
  deletes and every intermediate update, never misses a committed change and
  never shows a rolled-back one. Polling `updated_at` would miss deletes and any
  update that is overwritten between two polls.
- **We own the publication.** Migration `0002_cdc_publication.sql` creates
  `feastflow_pub FOR TABLE` the 6 tables, and the connector runs with
  `publication.autocreate.mode=disabled` plus an explicit `table.include.list`.
  The CDC contract is versioned like the schema, and `schema_migrations` stays
  out. The include list is required because Debezium's initial snapshot uses its
  own table filters, not the publication. **Rule: a new table needs
  `REPLICA IDENTITY FULL`, `ALTER PUBLICATION ... ADD TABLE` and an include-list
  entry.**
- **`REPLICA IDENTITY FULL` on all 6 tables.** Update events carry the full old
  row in `before`, and delete events carry the full deleted row. Silver can then
  record status transitions and build SCD2 without looking up previous state.
- **Schemaless JSON in Kafka** (`JsonConverter`, `schemas.enable=false`,
  `decimal.handling.mode=string`). Events are readable with the console consumer
  and in the lake, which matters while learning. Shape is enforced at the
  landing zone by the M2 data-contract check instead of at produce time.
- **Kafka topics: one per table, created by Debezium, 1 partition, key =
  primary key.** One partition gives total commit order within a table. The key
  keeps per-row order if partitions are ever added through a new topic.
- **Python landing sink instead of the Kafka Connect S3 sink.**
  `ingest/kafka_to_s3.py` (confluent-kafka + boto3) buffers per partition,
  writes JSON-lines objects named by topic, partition and offset range, skips
  tombstones, and commits Kafka offsets only after `put_object` succeeds. One
  env switch (`S3_ENDPOINT_URL`) moves it from MinIO to AWS S3.

## Alternatives considered
- **Query-based CDC (poll `updated_at`):** rejected. No deletes, intermediate
  states are lost, and it adds load on the source tables.
- **Debezium-created publication (`all_tables`/`filtered`):** rejected. It
  hides the CDC contract outside our migrations and needs owner rights on every
  table.
- **REPLICA IDENTITY DEFAULT:** rejected. `before` would be null on updates, and
  Silver could not see the previous status without a stateful lookup. Cost of
  FULL: more WAL and larger events (about 1.15 KB per orders event in the lake).
- **Avro + Schema Registry:** deferred to PayPulse. Avro is smaller (binary, a
  5-byte schema id instead of field names) and enforces compatibility at
  produce time, but it needs another service and binary payloads are not
  readable while learning. JSON compresses about 11x with zstd if size becomes a
  problem (measured in the Kafka masterbook).
- **Kafka Connect S3 sink (Confluent):** rejected for M1. It needs a plugin
  install into the Connect image and hides the commit/landing logic we wanted
  to learn. It is the likely choice at production scale, and the object layout
  and at-least-once semantics are deliberately similar.
- **Spark Structured Streaming from Kafka straight into Bronze:** rejected. The
  plan needs a file landing zone for the Lambda contract check, and Databricks
  Free Edition cannot reach a Kafka broker on a laptop.

## Consequences
- **At-least-once end to end.** Debezium flushes source offsets every 60 s and
  replays from the last flushed LSN after a crash. The sink rewrites a batch if
  it crashes between upload and commit. Silver must deduplicate: MERGE on the
  primary key and apply an event only if its `source.lsn` is greater than the
  stored one. Bronze keeps the duplicates.
- **Deletes arrive as two records:** an `op: d` event (full `before`,
  `after: null`) and a tombstone (null value). The sink skips tombstones but
  commits their offsets. Silver applies the `d` event.
- **No ordering across topics.** `order_items` can land before its `orders` row,
  so Silver must tolerate late-arriving parents.
- **Snapshot and stream overlap.** Seed rows arrive as `op: r`, and later
  changes to the same rows as `u`/`d`. LSN order resolves them.
- **The slot holds WAL while the connector is down.** `max_slot_wal_keep_size=2GB`
  caps disk use by invalidating the slot, which forces a re-snapshot. Deleting
  the connector does not drop the slot; drop it by hand.
- **Publishing a table constrains the application.** With replica identity
  NOTHING (or DEFAULT without a primary key), Postgres rejects the app's UPDATE
  and DELETE. This was reproduced in the step 7 drill.
- **Single broker, RF 1:** no durability against disk loss. Acceptable on a
  laptop only.
- **Verified 2026-10-23 on the laptop:** connector and task RUNNING, slot
  active, integration test green on Kafka 3.9.1 + MinIO. A live run landed all
  6 tables with consumer lag 0 (orders committed offset 1490). Clean shutdown
  printed `Totals: {'events': 4139, 'tombstones': 5, 'files': 20}`.
