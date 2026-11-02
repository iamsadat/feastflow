"""Typed Silver columns for one table, built from its data contract.

Bronze keeps each Debezium event as it landed: the row images sit inside
value.before / value.after, and decimals and timestamps are still strings.
changes() turns one Bronze row into one flat, typed change row:

    <every contract column, cast to its Silver type>, _op, _lsn, _commit_ts,
    _source_file, _ingested_at

The row image is `after` for r/c/u and `before` for d, because a delete has
no `after` (REPLICA IDENTITY FULL gives the full `before`). Underscored
columns are bookkeeping for AUTO CDC (step 4) and lineage; they never clash
with a source column.

No pipeline imports, so pytest can run it on a local SparkSession.
"""

from pyspark.sql import functions as F

SILVER_TYPES = {
    "integer": "BIGINT",
    "string": "STRING",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",   # "2026-10-26T10:15:30.123456Z" -> instant in UTC
    "time_micros": "BIGINT",    # microseconds since midnight, kept as a number
}


def silver_type(rule):
    """Spark SQL type for one contract column."""
    if rule["type"] == "decimal":
        # The contract must say the Postgres NUMERIC(p,s): guessing would round money or ratings.
        return f"DECIMAL({rule['precision']},{rule['scale']})"
    return SILVER_TYPES[rule["type"]]


def changes(bronze, contract):
    """Bronze envelope rows -> one flat, typed row per change."""
    image = F.when(F.col("value.op") == "d", F.col("value.before")).otherwise(F.col("value.after"))
    columns = [image.getField(name).cast(silver_type(rule)).alias(name)
               for name, rule in contract["columns"].items()]
    return bronze.select(
        *columns,
        F.col("value.op").alias("_op"),
        F.col("value.source.lsn").alias("_lsn"),
        F.timestamp_millis(F.col("value.source.ts_ms")).alias("_commit_ts"),
        "_source_file",
        "_ingested_at",
    )
