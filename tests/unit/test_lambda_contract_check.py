"""Unit tests for the contract-check Lambda with an in-memory fake S3."""

import io
import json
import sys
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ingest"))
import lambda_contract_check as lcc  # noqa: E402
from test_contract import ORDER_ROW, changed, landed  # noqa: E402

BUCKET = "test-bucket"
KEY = ("bronze_landing/orders/dt=2026-10-26/"
       "feastflow.public.orders+0+00000000000000000100-00000000000000000102.jsonl")


class FakeS3:
    """Just the two calls the Lambda makes."""

    def __init__(self, objects):
        self.objects = dict(objects)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body


def s3_event(key):
    """What S3 sends: the key URL-encoded, so '+' arrives as '%2B'."""
    encoded = urllib.parse.quote_plus(key, safe="/")
    return {"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": encoded}}}]}


def eventbridge_event(key):
    """What EventBridge sends for S3 "Object Created": the key as plain text"""
    return {"source": "aws.s3", "detail-type": "Object Created", "detail": {"bucket": {"name": BUCKET}, "object": {"key": KEY}}}


def lines(fake, key):
    return fake.objects[(BUCKET, key)].decode().splitlines()


def test_event_key_is_url_encoded():
    assert "%2B" in s3_event(KEY)["Records"][0]["s3"]["object"]["key"]


def test_good_and_bad_lines_are_split(monkeypatch):
    text = "\n".join([
        landed("c", None, ORDER_ROW),
        landed("c", None, changed(status="LOST")),
        landed("d", ORDER_ROW, None),
    ]) + "\n"
    fake = FakeS3({(BUCKET, KEY): text.encode()})
    monkeypatch.setattr(lcc, "_s3", fake)

    result = lcc.handler(s3_event(KEY), None)

    assert result == [{"key": KEY, "valid": 2, "rejected": 1}]
    assert len(lines(fake, KEY.replace("bronze_landing/", "bronze_validated/"))) == 2
    bad = [json.loads(x) for x in lines(fake, KEY.replace("bronze_landing/", "bronze_quarantine/"))]
    assert "LOST" in bad[0]["errors"][0]


def test_clean_file_writes_no_quarantine(monkeypatch):
    fake = FakeS3({(BUCKET, KEY): (landed("c", None, ORDER_ROW) + "\n").encode()})
    monkeypatch.setattr(lcc, "_s3", fake)

    lcc.handler(s3_event(KEY), None)

    assert (BUCKET, KEY.replace("bronze_landing/", "bronze_quarantine/")) not in fake.objects


def test_duplicate_event_rewrites_identical_objects(monkeypatch):
    fake = FakeS3({(BUCKET, KEY): (landed("c", None, ORDER_ROW) + "\n").encode()})
    monkeypatch.setattr(lcc, "_s3", fake)

    lcc.handler(s3_event(KEY), None)
    first = dict(fake.objects)
    lcc.handler(s3_event(KEY), None)

    assert fake.objects == first


def test_unknown_table_is_quarantined_whole(monkeypatch):
    key = KEY.replace("/orders/", "/coupons/")
    fake = FakeS3({(BUCKET, key): (landed("c", None, ORDER_ROW) + "\n").encode()})
    monkeypatch.setattr(lcc, "_s3", fake)

    assert lcc.handler(s3_event(key), None) == [{"key": key, "valid": 0, "rejected": 1}]

def test_eventbridge_key_is_used_as_in():
    key = "bronze_landing/orders/dt=2026-10-27/x+y z.jsonl"
    assert lcc.object_refs(eventbridge_event(key)) == [(BUCKET, KEY)]

def test_both_event_shapes_name_the_same_object():
    assert lcc.object_refs(s3_event(KEY)) == lcc.object_refs(eventbridge_event(KEY))

def test_eventbridge_event_is_processed(monkeypatch):
    fake = FakeS3({(BUCKET, KEY): (landed("c", None, ORDER_ROW) + "\n").encode()})
    monkeypatch.setattr(lcc, "_s3", fake)

    assert lcc.handler(eventbridge_event(KEY), None) == [{"key": KEY, "valid": 1, "rejected": 0}]
