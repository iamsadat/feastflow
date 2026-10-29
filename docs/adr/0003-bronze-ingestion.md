# 0003. Bronze ingestion: Lambda contract check via EventBridge, Auto Loader with managed file events, one writer per table

- Status: Accepted
- Date: 2026-10-28

## Context
M1 lands every Debezium change event as JSON lines in object storage. M2 moves
that landing zone to AWS and turns it into Bronze Delta tables that Silver can
merge from.

- The sink (`ingest/kafka_to_s3.py`) flushes per partition every ~30 s or 500
  events, so files are small and frequent (measured: 3 files per flush while the
  generator runs; 27 files / 4,414 events / 1,101 orders for the history so far).
- The JSON is schemaless in Kafka (ADR 0002), so nothing has checked a line's
  shape before it reaches the lake. Bad lines must not reach Bronze silently.
- Compute is Databricks Free Edition (serverless only) with Unity Catalog; the
  bucket `feastflow-lake-072270723370` is in `ap-south-2`. Cloud spend must stay
  near zero.
- M0 left an open caveat: concurrent Delta commits to S3 from more than one
  engine are not safe without extra coordination.

```text
 laptop sink ──PutObject──► s3://feastflow-lake-072270723370/bronze_landing/<table>/dt=<date>/*.jsonl
                                 │  S3 notification config: EventBridgeConfiguration {} + csms TopicConfiguration
                 ┌───────────────┴──────────────────────┐
                 ▼ lane 1                               ▼ lane 2
 EventBridge rule landing-to-contract-check   SNS csms-topic-by-path-… ─► SQS csms-queue-…
                 ▼                                      ▼
 Lambda feastflow-contract-check              Databricks file events cache (external location feastflow_lake)
   contracts/<table>.yaml                               ▼
   ├─► bronze_validated/<same key>  ───(new S3 event)──► Lakeflow Job bronze_autoloader (file arrival trigger)
   └─► bronze_quarantine/<same key>                     ▼
                                       Auto Loader availableNow ─► feastflow.bronze.<table>_cdc (Delta, UC managed)
                                       checkpoints s3://…/_autoloader/<table>/checkpoint
```

## Decision
- **Data contracts as YAML, checked per line in Lambda.** `contracts/<table>.yaml`
  lists every column with a Debezium-serialized type (integer, string, boolean,
  decimal-as-string, timestamp, time_micros) and nullability. `ingest/contract.py`
  checks the envelope (`op` in c/u/d/r, `before`/`after` present as the op
  requires) and each row. Unknown columns pass (additive schema change is
  allowed). `ingest/lambda_contract_check.py` splits each landed file into
  `bronze_validated/` and `bronze_quarantine/` **under the same key**, so a
  re-run overwrites instead of duplicating. Quarantined lines carry the reason.
- **Least-privilege identities per hop.** IAM user `feastflow-sink` may only
  `s3:PutObject` on `bronze_landing/*` (no read, no list). Role
  `feastflow-contract-check` may read `bronze_landing/*` and write
  `bronze_validated/*` and `bronze_quarantine/*`. Databricks reaches the bucket
  only through role `feastflow-uc-access` (storage credential `feastflow_s3_cred`).
- **The Lambda is triggered by EventBridge, not a direct S3 notification.**
  `EventBridgeConfiguration {}` sends all bucket events to the default bus; rule
  `landing-to-contract-check` matches `Object Created` under `bronze_landing/`.
  Two reasons: (1) S3 rejects two notification entries with the same event type
  on overlapping prefixes, and Databricks file events needs the whole bucket
  (prefix `""`) on SNS; (2) EventBridge keys are plain text, while S3
  notification keys are URL-encoded (our keys contain `+` and `=`). The handler
  accepts both shapes (`object_refs()`), so `ingest/redrive.py` can still send
  S3-shaped events to re-process missed files.
- **Bronze is Auto Loader into append-only Delta, one table per source table.**
  `ingest/bronze_autoloader.py` reads `bronze_validated/<table>/` with an
  **explicit schema** generated from the contract (`ingest/bronze_schema.py`),
  `rescuedDataColumn = _rescued_data`, and adds `_source_file`,
  `_source_file_ts` and `_ingested_at`. Decimals and timestamps stay STRING as
  Debezium sent them; Silver casts. `before` and `after` share one struct type so
  an all-null column never infers a wrong type. Tables live in catalog
  `feastflow` (managed location `s3://…/uc/feastflow`), schema `bronze`.
- **`availableNow` runs, started by a file arrival trigger.** Each run drains
  everything new and stops, so serverless compute is billed only per run. Job
  `bronze_autoloader` has a file arrival trigger on
  `s3://feastflow-lake-072270723370/bronze_validated/`, minimum 60 s between
  triggers, **no** "wait after last change" (the sink flushes every ~30 s, and
  that timer resets on every new file, so it would hold runs back for as long as
  the sink runs), max concurrent runs 1, retries 1, timeout 30 min.
- **File discovery: managed file events.** File events are enabled on external
  location `feastflow_lake`; Databricks created an SNS topic and SQS queue
  (`csms-*`) in our account through `feastflow-uc-access` (inline policy
  `uc-file-events`, scoped to `csms-*`). Auto Loader sets
  `cloudFiles.useManagedFileEvents = true` and reads new files from the
  Databricks file cache instead of listing the prefix. Directory listing was
  built and measured first (step 5) and stays the fallback.
- **One writer per Delta table.** Only Databricks writes Delta in the AWS bucket,
  and only one job writes each Bronze table (one checkpoint, max concurrent
  runs 1). Local PySpark writes Delta only to MinIO. No other identity can write
  under `uc/` or `_autoloader/`.

## Alternatives considered
- **No contract check (load everything, rely on `_rescued_data`):** rejected.
  Rescued data catches type drift but not semantic errors (an `op` outside
  c/u/d/r, an update with no `after`), and bad rows would already be in Bronze.
- **Contract check inside the Auto Loader job:** possible and cheaper, but it
  couples validation to Databricks and hides quarantined lines in a table. A
  separate Lambda was the learning goal and keeps a raw quarantine folder.
- **Direct S3 → Lambda notification:** used in step 3, replaced in step 6 for
  the overlap rule and key-encoding reasons above.
- **Directory listing only:** simplest and needs no extra IAM. Cost and latency
  grow with the number of objects under the prefix. Fine at our size; file events
  is the production pattern and what interviewers expect to hear about.
- **Classic file notification mode (`cloudFiles.useNotifications`):** rejected.
  Every stream creates its own SNS subscription and SQS queue and needs broader
  IAM; managed file events shares one queue per bucket across all streams and
  triggers.
- **Schema inference + schema evolution:** rejected for Bronze. The contract
  already defines the schema, and inference on mostly-null `before` columns
  picks wrong types.
- **Continuous streaming (`processingTime` trigger):** rejected. It keeps
  compute running between files; a file arrival trigger gives near-real-time
  without idle cost.
- **Multiple engines writing the same Delta table (laptop Spark + Databricks):**
  rejected. Safe concurrent commits on S3 need a coordinated commit protocol
  (for open-source Delta, `S3DynamoDBLogStore` with a DynamoDB table);
  Databricks coordinates its own commits. One writer avoids running that.

## Consequences
- **At-least-once upstream, exactly-once into Bronze per file.** The sink and the
  Lambda can rewrite the same key; Auto Loader records each file in its
  checkpoint and ignores a re-written path (`cloudFiles.allowOverwrites` is
  false), and the Delta `txn` (queryId, epochId) makes each micro-batch commit
  idempotent. Bronze still holds every event, so Silver must keep the latest
  event per key by `source.lsn` (dedup on `(order_id, source.lsn)`).
- **Rebuilding a table means deleting both the table and its checkpoint.** Drop
  only the table and the next run loads nothing; delete only the checkpoint and
  every file is loaded again on top of the old rows.
- **Run streams at least weekly.** A file-events stream idle for more than 7
  days does a full listing on its next run. Toggling file events off/on
  invalidates every stream's read position.
- **The bucket notification config is shared state.** It holds Databricks'
  `TopicConfigurations` entry and our `EventBridgeConfiguration`. Any change must
  be get, merge, put. Terraform (M3) must not own that resource.
- **Lane 2 hears the whole bucket** (including `_autoloader/` and `uc/` writes);
  Databricks filters by path. Cost is negligible at this volume.
- **A run lasts 6.5 to 9 minutes for 6 tables (measured Oct 28), mostly fixed
  overhead:** the streams run one after another (an empty table still takes
  ~24 s), plus serverless start-up, `%pip install` and the check cell's full
  `read_files` scans. With max one concurrent run, files that land during a run
  wait for the next one, so worst-case file-to-Bronze latency is about one run.
  Follow-up: start the 6 streams in parallel and move the check cell to a
  separate validation notebook.
- **Quarantine needs a human.** Rejected lines stay in `bronze_quarantine/`; after
  fixing a contract or the data, `redrive.py` re-sends the landing file.
- **Measured (2026-10-26 to 10-29):** Lambda cold start (Init) 380–420 ms, max
  memory 94 MB of 256 MB; 9 live files → 9 invocations, 0 rejected; redrive dry
  run 299/299 processed, 0 missing; validated twin written ~1 s after the landing
  file. File arrival: a 3-minute live test (~18 files) was loaded by 2
  file-arrival runs (6m33s and 8m45s; 533 orders events). After the final
  rebuild (Oct 29) every table's Bronze count equals its validated line count:
  orders 13,881, order_items 10,226, riders 3,686, customers 501, menu_items 624,
  restaurants 51.
- **Lesson from the duplicate incident (Oct 27, found Oct 29):** the Oct 27
  history was loaded twice by two notebook streams with different checkpoints
  (queryIds `af70996b…` then `ad91a6b4…`), because checkpoints were deleted while
  the tables were kept. The `bronze = lines` check caught it; the fix was a full
  rebuild (drop all 6 tables + delete `_autoloader/`). The check now runs after
  every change to the job.
