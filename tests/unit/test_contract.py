"""Unit tests for ingest/contract.py: no Kafka, no S3, no network."""

import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ingest"))
import contract  # noqa: E402

CONTRACTS = contract.load_contracts(ROOT / "contracts")
ORDERS = CONTRACTS["orders"]

ORDER_ROW = {
    "order_id": 42,
    "customer_id": 7,
    "restaurant_id": 3,
    "rider_id": None,
    "total_paise": 45000,
    "status": "PLACED",
    "placed_at": "2026-10-26T10:15:30.123456Z",
    "accepted_at": None,
    "picked_up_at": None,
    "delivered_at": None,
    "cancelled_at": None,
    "created_at": "2026-10-26T10:15:30.123456Z",
    "updated_at": "2026-10-26T10:15:30.123456Z",
}


def landed(op="c", before=None, after=None, topic="feastflow.public.orders"):
    """One line as the sink writes it."""
    return json.dumps({
        "kafka_topic": topic,
        "kafka_partition": 0,
        "kafka_offset": 100,
        "kafka_timestamp_ms": 1793000000000,
        "key": {"order_id": 42},
        "value": {
            "before": before,
            "after": after,
            "source": {"lsn": 123456789, "table": "orders"},
            "op": op,
            "ts_ms": 1793000000123,
        },
    })


def changed(**fields):
    row = copy.deepcopy(ORDER_ROW)
    row.update(fields)
    return row


def test_all_six_contracts_load():
    assert sorted(CONTRACTS) == [
        "customers", "menu_items", "order_items", "orders", "restaurants", "riders"]


@pytest.mark.parametrize("op, before, after", [
    ("c", None, ORDER_ROW),
    ("r", None, ORDER_ROW),
    ("u", ORDER_ROW, changed(status="ACCEPTED", accepted_at="2026-10-26T10:16:00Z")),
    ("d", ORDER_ROW, None),
])
def test_valid_lines_pass(op, before, after):
    assert contract.check_line(landed(op, before, after), ORDERS) == []


@pytest.mark.parametrize("line, expected", [
    ("{not json", "not JSON"),
    (landed("x", None, ORDER_ROW), "op 'x'"),
    (landed("u", None, ORDER_ROW), "op u needs before"),
    (landed("d", None, None), "op d needs before"),
    (landed("c", None, changed(status="LOST")), "after.status: 'LOST' not in"),
    (landed("c", None, changed(total_paise=0)), "after.total_paise: 0 below min 1"),
    (landed("c", None, changed(order_id=None)), "after.order_id: null not allowed"),
    (landed("c", None, changed(customer_id="7")), "after.customer_id: expected integer"),
    (landed("c", None, changed(placed_at="yesterday")), "after.placed_at: expected timestamp"),
    (landed("c", None, ORDER_ROW, topic="feastflow.public.riders"), "does not belong"),
])
def test_bad_lines_are_rejected_with_a_reason(line, expected):
    errors = contract.check_line(line, ORDERS)
    assert any(expected in e for e in errors), errors


def test_missing_column_is_an_error():
    row = changed()
    del row["status"]
    assert "after.status: missing" in contract.check_line(landed("c", None, row), ORDERS)


def test_unknown_column_is_let_through():
    assert contract.check_line(landed("c", None, changed(promo_code="DIWALI")), ORDERS) == []


def test_decimal_and_time_types():
    restaurants = CONTRACTS["restaurants"]
    row = {
        "restaurant_id": 1, "name": "Paradise", "address": "MG Road",
        "phone": "9000000001", "city": "Hyderabad", "zip": "500003",
        "rating": "4.5", "opens_at": 36_000_000_000, "closes_at": 82_800_000_000,
        "created_at": "2026-10-23T09:00:00Z", "updated_at": "2026-10-23T09:00:00Z",
    }
    assert contract.check_row(row, restaurants, "after") == []
    row.update(rating="5.5", zip="5000")
    errors = contract.check_row(row, restaurants, "after")
    assert any("above max 5" in e for e in errors)
    assert any("does not match" in e for e in errors)


def test_split_lines_keeps_good_and_quarantines_bad():
    text = "\n".join([
        landed("c", None, ORDER_ROW),
        landed("c", None, changed(status="LOST")),
        landed("d", ORDER_ROW, None),
    ]) + "\n"
    valid, rejected = contract.split_lines(text, ORDERS)
    assert len(valid) == 2
    assert len(rejected) == 1
    assert "LOST" in rejected[0]["errors"][0]


def test_empty_contract_folder_fails_loudly(tmp_path):
    with pytest.raises(FileNotFoundError):
        contract.load_contracts(tmp_path)
