# 0004. Silver: a declarative pipeline with AUTO CDC, LSN ordering, tiered expectations and SCD2 history

- Status: Accepted
- Date: 2026-11-05

## Context
Bronze (ADR 0003) holds every Debezium change event as an append-only log in
`feastflow.bronze.<table>_cdc`. Silver must turn that log into tables that say what
is true now, and for customers and restaurants what was true at any past moment.

Bronze gives Silver five properties that each force a rule:

- **At-least-once delivery.** A sink crash between its PUT and its Kafka commit
  re-sends events. Measured on Nov 2: 0 duplicates in all 6 tables, but nothing
  prevents them. An event's identity is `(primary key, source.lsn)`.
- **No usable row order.** File order, `_ingested_at` and Debezium's `ts_ms`
  (millisecond resolution) don't give commit order. All 5 events of order 182 share
  one `_ingested_at` and span two files. The WAL position (`source.lsn`) does give
  it: for one row, a later commit always has a higher LSN, because the second
  transaction can't touch the row until the first one releases its row lock.
- **Raw strings.** Decimals and timestamps arrive as strings.
- **Deletes**, including cascades: one `d` event per `order_items` row
  (176 measured), with a full `before` image (REPLICA IDENTITY FULL).
- **Snapshot rows.** Reference tables are almost all `r` events. They carry the
  snapshot's commit time (2026-10-23 06:47) and all share one LSN (121,288,832),
  not the time each row was created.

Contracts (`contracts/<table>.yaml`) are checked once in the Lambda at landing,
but the Lambda carries its own copy inside its zip. A file can also reach
`bronze_validated/` without the check (a replay bug, a manual upload).

```text
bronze.<t>_cdc ──stream──► silver.<t>_changes ──AUTO CDC SCD1──► silver.orders, order_items, riders, menu_items
                           typed, _op, _lsn,     ──AUTO CDC SCD2──► silver.customers_history, restaurants_history
                           _commit_ts, _valid_ts,
                           _failed_rules
                           fail: key/op/LSN  warn: contract column rules
```

## Decision
1. **One Lakeflow Declarative Pipeline, `feastflow_silver`** (serverless, triggered),
   separate from the Bronze job. Bronze is its only source. Every Silver table is
   rebuildable from Bronze with a full refresh. Datasets are generated per contract
   by factory functions (`silver/silver_pipeline.py`); the column logic lives in
   `silver/silver_columns.py`, so it's unit-testable without the pipeline runtime.
2. **Typed change tables first.** `<t>_changes` holds one row per Bronze event:
   - the `after` image for r/c/u and the `before` image for d;
   - each column cast to the contract type, with DECIMAL(p,s) taken from the contract
     (never guessed);
   - `_op`, `_lsn`, `_commit_ts`, `_valid_ts` and lineage.
   It's a table, not a view, so it can be inspected and reconciled.
3. **AUTO CDC does the fold, ordered by LSN.** `keys` is the contract primary key, and
   `apply_as_deletes` is `_op = 'd'`. For SCD1, `sequence_by` is `_lsn`. The engine
   handles duplicates, late events and the delete/update race, using tombstones.
   It was proven equal to our own `row_number()` fold: EXCEPT in both directions
   returned 0 rows.
4. **SCD1 for orders, order_items, riders and menu_items. SCD2 for customers and
   restaurants.** History is tracked only for the columns that matter to a
   point-in-time question: customers `address, city, zip`; restaurants
   `address, city, zip, rating`. Other changes update the current version in place.
5. **SCD2 is sequenced by `struct(_valid_ts, _lsn)`.** Time comes first, so
   `__START_AT._valid_ts` and `__END_AT._valid_ts` are real timestamps that an order's
   `placed_at` can join to. The LSN breaks ties between commits in the same
   millisecond.
6. **Snapshot rows are valid from `created_at`.** `_valid_ts` is `created_at` for `r`
   and the commit time otherwise. Nothing before CDC started is known, so the
   snapshot value is assumed to have held since the row was created. Without this,
   orders placed before the snapshot would match no customer version.
7. **Tiered expectations, generated from the contracts:**
   - **Fail** on envelope rules (`<pk>_present`, `op_known`, `lsn_present`). A row
     the fold can't place means the pipeline is broken, so the whole update stops
     and Silver stays at its last good state.
   - **Warn** on column rules (`_not_null`, `_enum`, `_pattern`, `_min`, `_max`). The
     row is kept, the violation is counted in the event log, and the row carries
     `_failed_rules`, which AUTO CDC copies into the current tables. Postgres
     accepted the value, so it's the source's truth; a break almost always means
     the contract is behind the source.
   - **Never drop** on a change log.
   - Each rule is written so that it never returns NULL.
8. **Contracts load from the repo on every update** (`feastflow.repo_root`), so Silver
   applies the current contract even when the Lambda zip is older. That skew is
   visible as warn counts.

## Alternatives considered
- **A hand-written `foreachBatch` + `MERGE` instead of AUTO CDC.** It needs our own
  dedup, ordering, late-event and delete handling (tombstones), and our own
  checkpoint management. AUTO CDC does all of it and was proven equal to our fold.
  Rejected: more code, more ways to be subtly wrong.
- **Drop rows that break column rules** (the first step 5 version, with
  `<t>_quarantine`). It was proven harmful: removing CANCELLED from the enum made
  163 orders silently revert to their previous status. Order 50 showed PLACED at
  LSN 121,659,912 instead of CANCELLED, and the count, the update status and the
  fail rules all stayed green. Rejected for change logs.
- **Fail on column rules.** Silver would be correct but stale until a human fixes the
  contract, and every contract lag would stop all tables. Rejected for column
  rules; kept for envelope rules.
- **`sequence_by = _lsn` alone for SCD2.** Validity would be in WAL positions, which
  can't be joined to `placed_at`. Rejected.
- **`sequence_by = _commit_ts` alone.** Millisecond ties between commits; restaurant
  5's two re-ratings were 3 ms apart. Rejected; the LSN stays as the tie-breaker.
- **Snapshot rows valid from `_commit_ts`.** Every pre-snapshot order would find no
  version. Rejected; the `created_at` assumption is stated instead.
- **A separate quarantine or violations table.** It reads Bronze a second time, and it
  keeps the flag away from the row Gold uses. Replaced by the `_failed_rules`
  column.

## Consequences
- **Verified** (Nov 3 to 5, 2026):
  - Counts: orders 3,927 (4,000 keys − 73 deletes); order_items 9,874.
  - customers_history 501 versions with 499 current, which equals Postgres;
    restaurants_history 51 versions with 50 current.
  - Full refresh: all 12 orders rules passed 13,881 and failed 0.
  - The scripted SCD2 drill: customer 12 has 2 versions, restaurant 5 has 3.
- **A full refresh is safe only while Bronze keeps every event.** No retention or
  VACUUM policy may delete Bronze history without revisiting this ADR.
- **Tombstones are garbage-collected** (`pipelines.cdc.tombstoneGCThresholdInSeconds`).
  An update arriving later than that after a delete would resurrect the key. That
  bounds how late a replay may be.
- **A fail message prints the whole offending row** into the event log, including
  name, email and phone. That's acceptable on synthetic data; production needs
  masking or restricted event log access.
- **Warn needs an alert, which isn't built yet.** A warning nobody reads equals a
  drop. Next: a scheduled check on event log `failed_records > 0` or
  `size(_failed_rules) > 0`.
- **Contracts check shape, not meaning.** A Bengaluru customer with a Hyderabad zip
  passes. Cross-column rules are future work.
- **The Lambda's boundary quarantine is still a drop.** Customer 7 showed Hyderabad
  in Silver until the replay. It's acceptable only because quarantine is
  replayable, and the replayed event keeps its original LSN, so the fold places it
  correctly.
- Removing a dataset from the pipeline code doesn't drop its table. The old
  `<t>_quarantine` tables were dropped by hand.
