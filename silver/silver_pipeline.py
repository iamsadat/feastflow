"""Lakeflow Declarative Pipeline feastflow_silver (ADR 0004).

Step 3: one streaming table feastflow.silver.<table>_changes per contract,
fed by a streaming read of feastflow.bronze.<table>_cdc. Each Bronze row is
read once (Bronze is append-only) and becomes one typed change row.

Step 4: for the SCD1 tables, an AUTO CDC flow folds <table>_changes into
feastflow.silver.<table>: one row per primary key, the change with the highest
LSN wins, `d` deletes. Duplicates and late events are handled by the engine.

Step 5: <table>_changes carries expectations generated from the contract.
Envelope rules (key, op, LSN) fail the update. Column rules only warn: the row
is kept and the violation is counted in the event log, because dropping a change
event silently reverts its key to an older version (ADR 0004). Each row also
carries _failed_rules (the names of the column rules it breaks, empty when clean),
which AUTO CDC copies into the current table, so Gold can filter and checks can alert.

Step 6: customers and restaurants keep history. An AUTO CDC flow in SCD type 2
writes feastflow.silver.<table>_history: one row per version, __START_AT and
__END_AT (NULL = current) taken from sequence_by = struct(_valid_ts, _lsn), so
validity is a real timestamp and the LSN breaks ties. Only the tracked columns
open a new version; other changes update the current version in place. A delete
closes the current version.

Pipeline configuration must set feastflow.repo_root to the Git folder, e.g.
/Workspace/Users/<you>/feastflow, so contracts/ and ingest/ can be found.
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()  # the pipeline's session; explicit so linters see it
ROOT = spark.conf.get("feastflow.repo_root")
sys.path[:0] = [f"{ROOT}/ingest", f"{ROOT}/silver"]
import contract
import silver_columns

CONTRACTS = contract.load_contracts(f"{ROOT}/contracts")

# Current state only. customers and restaurants keep history (SCD2) in step 6.
SCD1_TABLES = ["orders", "order_items", "riders", "menu_items"]
# Bookkeeping that means nothing on a current row: the op of a live row is never
# `d`, and lineage stays in <table>_changes. _lsn and _commit_ts stay: "version"
# and "last changed at" of each row.
NOT_IN_CURRENT = ["_op", "_source_file", "_ingested_at"]
# History: the columns whose change opens a new version (track_history_column_list).
SCD2_TABLES = {
    "customers": ["address", "city", "zip"],
    "restaurants": ["address", "city", "zip", "rating"],
}


def typed_changes(table, spec):
    bronze = spark.readStream.table(f"feastflow.bronze.{table}_cdc")
    return silver_columns.changes(bronze, spec)


def define_changes(table, spec):
    """Register <table>_changes. A factory, so each closure keeps its table."""
    warn = silver_columns.warn_rules(spec)

    @dp.table(
        name=f"feastflow.silver.{table}_changes",
        comment=f"Typed CDC events of {table}, one per Bronze row (contract v{spec['version']}).",
    )
    @dp.expect_all_or_fail(silver_columns.fail_rules(spec))
    @dp.expect_all(warn)
    def _changes():
        rows = typed_changes(table, spec)
        return rows.withColumn("_failed_rules", silver_columns.failed_rules(warn))


def define_current(table, spec):
    """Register feastflow.silver.<table> and the AUTO CDC flow that keeps it current."""
    target = f"feastflow.silver.{table}"
    dp.create_streaming_table(
        name=target,
        comment=f"Current {table}: one row per key, deletes applied (SCD1, sequenced by LSN).",
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=f"feastflow.silver.{table}_changes",
        keys=spec["primary_key"],
        sequence_by=F.col("_lsn"),
        apply_as_deletes=F.expr("_op = 'd'"),
        except_column_list=NOT_IN_CURRENT,
        stored_as_scd_type=1,
    )


def define_history(table, spec, tracked):
    """Register feastflow.silver.<table>_history and its SCD2 AUTO CDC flow."""
    target = f"feastflow.silver.{table}_history"
    dp.create_streaming_table(
        name=target,
        comment=f"Every version of {table} in {tracked} (SCD2); __END_AT NULL = current.",
    )
    dp.create_auto_cdc_flow(
        target=target,
        source=f"feastflow.silver.{table}_changes",
        keys=spec["primary_key"],
        sequence_by=F.struct("_valid_ts", "_lsn"),
        apply_as_deletes=F.expr("_op = 'd'"),
        except_column_list=NOT_IN_CURRENT,
        stored_as_scd_type=2,
        track_history_column_list=tracked,
    )


for table, spec in CONTRACTS.items():
    define_changes(table, spec)
    if table in SCD1_TABLES:
        define_current(table, spec)
    if table in SCD2_TABLES:
        define_history(table, spec, SCD2_TABLES[table])
