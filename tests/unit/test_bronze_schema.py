"""Unit tests for ingest/bronze_schema.py: the DDL Auto Loader reads with."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ingest"))
import bronze_schema  # noqa: E402
import contract  # noqa: E402

CONTRACTS = contract.load_contracts(ROOT / "contracts")


def test_before_and_after_share_one_struct():
    ddl = bronze_schema.bronze_ddl(CONTRACTS["orders"])
    row = bronze_schema.row_struct(CONTRACTS["orders"], CONTRACTS["orders"]["columns"])
    assert f"before: {row}, after: {row}" in ddl


def test_key_holds_only_the_primary_key():
    ddl = bronze_schema.bronze_ddl(CONTRACTS["orders"])
    assert "key STRUCT<order_id: BIGINT>" in ddl


def test_landed_strings_stay_strings():
    row = bronze_schema.row_struct(CONTRACTS["restaurants"], CONTRACTS["restaurants"]["columns"])
    assert "rating: STRING" in row
    assert "created_at: STRING" in row
    assert "opens_at: BIGINT" in row


def test_every_contract_type_has_a_spark_type():
    for spec in CONTRACTS.values():
        for rule in spec["columns"].values():
            assert rule["type"] in bronze_schema.SPARK_TYPES

def test_envelope_has_every_debezium_field():
    ddl = bronze_schema.bronze_ddl(CONTRACTS["orders"])
    for field in ("before:", "after:", "source:", "transaction:", "op:", "ts_ms:", "ts_us:", "ts_ns:"):
        assert field in ddl