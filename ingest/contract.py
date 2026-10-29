"""Check landed CDC lines against a table's data contract (contracts/<table>.yaml).

Each line is one JSON object written by ingest/kafka_to_s3.py:

    {"kafka_topic": ..., "kafka_partition": ..., "kafka_offset": ...,
     "kafka_timestamp_ms": ..., "key": {...}, "value": <Debezium envelope>}

A line is valid when it parses, carries a known Debezium op with the row
images that op needs, and every contracted column in those images has the
declared type and respects nullable / enum / pattern / min / max.
Columns that are not in the contract are let through: a new column is schema
evolution for Auto Loader, not bad data.

No AWS imports, so the same module runs in pytest and in the Lambda.
"""

import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

ENVELOPE_FIELDS = ("kafka_topic", "kafka_partition", "kafka_offset", "value")

# Debezium op -> row images it must carry (REPLICA IDENTITY FULL gives `before`).
ROW_IMAGES = {
    "c": ("after",),            # insert
    "r": ("after",),            # snapshot read
    "u": ("before", "after"),   # update
    "d": ("before",),           # delete
}

MICROS_PER_DAY = 86_400_000_000


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)  # bool is a subclass of int


def _is_decimal(v):
    # decimal.handling.mode=string: NUMERIC arrives as "4.5"
    if not isinstance(v, str):
        return False
    try:
        Decimal(v)
    except InvalidOperation:
        return False
    return True


def _is_timestamp(v):
    # TIMESTAMPTZ arrives as an ISO-8601 UTC string, e.g. "2026-10-23T10:15:30.123456Z"
    if not isinstance(v, str):
        return False
    try:
        datetime.fromisoformat(v)
    except ValueError:
        return False
    return True


def _is_time_micros(v):
    # TIME arrives as microseconds since midnight (Debezium MicroTime)
    return _is_int(v) and 0 <= v < MICROS_PER_DAY


TYPE_CHECKS = {
    "integer": _is_int,
    "string": lambda v: isinstance(v, str),
    "boolean": lambda v: isinstance(v, bool),
    "decimal": _is_decimal,
    "timestamp": _is_timestamp,
    "time_micros": _is_time_micros,
}


def load_contract(path):
    """Read one contract and fail fast on a type the validator does not know."""
    contract = yaml.safe_load(Path(path).read_text())
    for name, rule in contract["columns"].items():
        if rule["type"] not in TYPE_CHECKS:
            raise ValueError(f"{path}: column {name} has unknown type {rule['type']}")
    return contract


def load_contracts(directory):
    """All contracts in a folder, keyed by table name."""
    contracts = {}
    for path in sorted(Path(directory).glob("*.yaml")):
        contract = load_contract(path)
        contracts[contract["table"]] = contract

    if not contracts:
        raise FileNotFoundError(f"no *.yaml contracts in {directory}")
    
    return contracts


def check_row(row, contract, image):
    """Errors for one row image ("before" or "after"); empty list = valid."""
    errors = []
    for name, rule in contract["columns"].items():
        if name not in row:
            errors.append(f"{image}.{name}: missing")
            continue
        value = row[name]
        if value is None:
            if not rule["nullable"]:
                errors.append(f"{image}.{name}: null not allowed")
            continue
        if not TYPE_CHECKS[rule["type"]](value):
            errors.append(f"{image}.{name}: expected {rule['type']}, got {value!r}")
            continue
        if "enum" in rule and value not in rule["enum"]:
            errors.append(f"{image}.{name}: {value!r} not in {rule['enum']}")
        if "pattern" in rule and not re.fullmatch(rule["pattern"], value):
            errors.append(f"{image}.{name}: {value!r} does not match {rule['pattern']}")
        number = Decimal(value) if rule["type"] == "decimal" else value
        if "min" in rule and number < rule["min"]:
            errors.append(f"{image}.{name}: {value!r} below min {rule['min']}")
        if "max" in rule and number > rule["max"]:
            errors.append(f"{image}.{name}: {value!r} above max {rule['max']}")
    return errors


def check_line(line, contract):
    """Errors for one landed line; empty list = valid."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        return [f"not JSON: {exc.msg}"]

    missing = [f for f in ENVELOPE_FIELDS if f not in record]
    if missing:
        return [f"envelope missing {missing}"]

    table = record["kafka_topic"].rsplit(".", 1)[-1]
    if table != contract["table"]:
        return [f"topic {record['kafka_topic']} does not belong to contract {contract['table']}"]

    value = record["value"]
    op = value.get("op")
    if op not in ROW_IMAGES:
        return [f"op {op!r} not one of {sorted(ROW_IMAGES)}"]

    errors = []
    if value.get("source", {}).get("lsn") is None:
        errors.append("source.lsn missing")
    if value.get("ts_ms") is None:
        errors.append("ts_ms missing")
    for image in ROW_IMAGES[op]:
        row = value.get(image)
        if not isinstance(row, dict):
            errors.append(f"op {op} needs {image}, got {row!r}")
            continue
        errors.extend(check_row(row, contract, image))
    return errors


def split_lines(text, contract):
    """Split a landed file into valid lines and rejects (line + its errors)."""
    valid, rejected = [], []
    for line in text.splitlines():
        if not line.strip():
            continue
        errors = check_line(line, contract)
        if errors:
            rejected.append({"line": line, "errors": errors})
        else:
            valid.append(line)
    return valid, rejected
