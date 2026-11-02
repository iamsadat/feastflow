"""Unit tests for silver/silver_columns.py on a local SparkSession (needs Java)."""

import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "ingest"), str(ROOT / "silver")]
import bronze_schema
import contract
import silver_columns
from test_contract import ORDER_ROW, changed, landed

CONTRACTS = contract.load_contracts(ROOT / "contracts")


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("silver-columns")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def bronze_rows(spark, table, *lines):
    """Lines as Auto Loader reads them (contract DDL), plus the lineage columns."""
    df = spark.read.schema(bronze_schema.bronze_ddl(CONTRACTS[table])).json(
        spark.sparkContext.parallelize(lines))
    return df.withColumn("_source_file", F.lit("s3://b/f.jsonl")).withColumn(
        "_ingested_at", F.current_timestamp())


def with_commit_ms(line, ms):
    """landed() leaves out source.ts_ms (the commit time in Postgres); real lines carry it."""
    record = json.loads(line)
    record["value"]["source"]["ts_ms"] = ms
    return json.dumps(record)


def test_types_follow_the_contract(spark):
    out = silver_columns.changes(bronze_rows(spark, "orders", landed("c", None, ORDER_ROW)),
                                 CONTRACTS["orders"])
    types = dict(out.dtypes)
    assert types["order_id"] == "bigint"
    assert types["placed_at"] == "timestamp"
    assert types["_lsn"] == "bigint" and types["_commit_ts"] == "timestamp"


def epoch_micros(dt):
    return int((dt - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1_000_000 + dt.microsecond


def test_values_are_cast_not_lost(spark):
    out = silver_columns.changes(
        bronze_rows(spark, "orders", with_commit_ms(
            landed("u", ORDER_ROW, changed(status="ACCEPTED")), 1793000000123)),
        CONTRACTS["orders"])
    # Compare instants as epoch microseconds: collect() turns a TIMESTAMP into a naive
    # datetime in the laptop's local zone (IST), which would hide or fake an offset bug.
    [row] = out.select("*", F.unix_micros("placed_at").alias("placed_us"),
                       F.unix_micros("_commit_ts").alias("commit_us")).collect()
    assert row.status == "ACCEPTED" and row._op == "u" and row._lsn == 123456789
    assert row.placed_us == epoch_micros(datetime(2026, 10, 26, 10, 15, 30, 123456, tzinfo=UTC))
    assert row.commit_us == 1793000000123 * 1000


def test_delete_takes_the_before_image(spark):
    [row] = silver_columns.changes(bronze_rows(spark, "orders", landed("d", ORDER_ROW, None)),
                                   CONTRACTS["orders"]).collect()
    assert row._op == "d" and row.order_id == 42 and row.status == "PLACED"


def test_decimal_uses_contract_precision():
    rule = CONTRACTS["restaurants"]["columns"]["rating"]
    assert silver_columns.silver_type(rule) == "DECIMAL(2,1)"


def test_decimal_value(spark):
    line = landed("r", None, {"rating": "4.5"}, topic="feastflow.public.restaurants")
    [row] = silver_columns.changes(bronze_rows(spark, "restaurants", line),
                                   CONTRACTS["restaurants"]).collect()
    assert row.rating == Decimal("4.5")


def test_rules_come_from_the_contract():
    drop = silver_columns.drop_rules(CONTRACTS["customers"])
    assert drop["city_enum"] == "city IS NULL OR city IN ('Hyderabad', 'Bengaluru', 'Chennai')"
    assert drop["zip_pattern"] == "zip IS NULL OR zip RLIKE '^[0-9]{6}$'"
    assert "customer_id_not_null" not in drop                 # the key is a fail rule
    assert "customer_id_present" in silver_columns.fail_rules(CONTRACTS["customers"])


def test_good_row_breaks_no_rule(spark):
    out = silver_columns.changes(bronze_rows(spark, "orders", landed("c", None, ORDER_ROW)),
                                 CONTRACTS["orders"])
    rules = {**silver_columns.fail_rules(CONTRACTS["orders"]),
             **silver_columns.drop_rules(CONTRACTS["orders"])}
    [row] = out.select(silver_columns.failed_rules(rules).alias("failed")).collect()
    assert row.failed == []


def test_bad_row_names_the_rules_it_breaks(spark):
    bad = changed(status="LOST", total_paise=0)
    out = silver_columns.changes(bronze_rows(spark, "orders", landed("c", None, bad)),
                                 CONTRACTS["orders"])
    [row] = out.select(silver_columns.failed_rules(
        silver_columns.drop_rules(CONTRACTS["orders"])).alias("failed")).collect()
    assert sorted(row.failed) == ["status_enum", "total_paise_min"]


def test_no_rule_is_ever_null(spark):
    """A NULL verdict is neither pass nor fail; every rule must answer on an all-null row."""
    line = landed("c", None, {}, topic="feastflow.public.customers")
    out = silver_columns.changes(bronze_rows(spark, "customers", line), CONTRACTS["customers"])
    rules = {**silver_columns.fail_rules(CONTRACTS["customers"]),
             **silver_columns.drop_rules(CONTRACTS["customers"])}
    [row] = out.select(*[F.expr(sql).isNull().alias(n) for n, sql in rules.items()]).collect()
    assert not any(row)
