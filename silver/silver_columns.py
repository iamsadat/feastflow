"""Typed Silver columns for one table, built from its data contract.

Bronze keeps each Debezium event as it landed: the row images sit inside
value.before / value.after, and decimals and timestamps are still strings.
changes() turns one Bronze row into one flat, typed change row:

    <every contract column, cast to its Silver type>, _op, _lsn, _commit_ts,
    _valid_ts, _source_file, _ingested_at

The row image is `after` for r/c/u and `before` for d, because a delete has
no `after` (REPLICA IDENTITY FULL gives the full `before`). Underscored
columns are bookkeeping for AUTO CDC (step 4) and lineage; they never clash
with a source column.

_valid_ts is when this row image became true at the source (valid time), for
SCD2 validity: created_at for r and c, updated_at for u, the commit time for d
(a delete has no business timestamp). It falls back to the commit time if the
column is NULL. _commit_ts is when Postgres recorded it (system time), so
Silver keeps both clocks (ADR 0005). History mode writes backdated
created_at/updated_at, and migration 0004 lets the trigger keep them.

fail_rules() and warn_rules() turn the same contract into expectations
(step 5): envelope rules stop the update, column rules warn (the row is kept),
and failed_rules() names what a row breaks, as the _failed_rules flag column.

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
    op = F.col("value.op")
    commit_ts = F.timestamp_millis(F.col("value.source.ts_ms"))
    created = image.getField("created_at").cast("TIMESTAMP")
    updated = image.getField("updated_at").cast("TIMESTAMP")
    valid_ts = (F.when(op.isin("r", "c"), created)
                    .when(op == "u", updated)
                    .otherwise(commit_ts))
    return bronze.select(
        *columns,
        op.alias("_op"),
        F.col("value.source.lsn").alias("_lsn"),
        commit_ts.alias("_commit_ts"),
        F.coalesce(valid_ts, commit_ts).alias("_valid_ts"),
        "_source_file",
        "_ingested_at",
    )


def _sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def fail_rules(contract):
    """Envelope rules: a row breaking one means the pipeline itself is broken, so stop."""
    rules = {f"{key}_present": f"{key} IS NOT NULL" for key in contract["primary_key"]}
    rules["op_known"] = "_op IN ('c', 'u', 'd', 'r')"
    rules["lsn_present"] = "_lsn IS NOT NULL"
    return rules


def warn_rules(contract):
    """Column rules from the contract, as SQL that is never NULL (NULL would be no verdict).

    Same rules the Lambda checked on landing; here they guard Silver against rows that
    reached Bronze another way, or a Lambda deployed with an older contract.
    """
    rules = {}
    for name, rule in contract["columns"].items():
        if name in contract["primary_key"]:
            continue                                   # already a fail rule
        if not rule["nullable"]:
            rules[f"{name}_not_null"] = f"{name} IS NOT NULL"
        if "enum" in rule:
            allowed = ", ".join(_sql_str(v) for v in rule["enum"])
            rules[f"{name}_enum"] = f"{name} IS NULL OR {name} IN ({allowed})"
        if "pattern" in rule:
            rules[f"{name}_pattern"] = f"{name} IS NULL OR {name} RLIKE {_sql_str(rule['pattern'])}"
        if "min" in rule:
            rules[f"{name}_min"] = f"{name} IS NULL OR {name} >= {rule['min']}"
        if "max" in rule:
            rules[f"{name}_max"] = f"{name} IS NULL OR {name} <= {rule['max']}"
    return rules


def failed_rules(rules):
    """Column: names of the rules a row breaks, [] when it passes them all."""
    names = [F.when(~F.expr(sql), F.lit(name)) for name, sql in rules.items()]
    return F.filter(F.array(*names), lambda n: n.isNotNull())
