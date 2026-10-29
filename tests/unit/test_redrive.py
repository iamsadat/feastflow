"""Unit tests for ingest/redrive.py with a fake S3 paginator."""

import sys
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ingest"))
import redrive  # noqa: E402


class FakePaginator:
    def __init__(self, keys):
        self.keys = keys

    def paginate(self, Bucket, Prefix):
        matching = [{"Key": k} for k in self.keys if k.startswith(Prefix)]
        # Two pages, like a real listing over 1,000 keys.
        yield {"Contents": matching[:1]}
        yield {"Contents": matching[1:]} if matching[1:] else {}


class FakeS3:
    def __init__(self, keys):
        self.keys = keys

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self.keys)


def test_list_relative_strips_prefix_and_skips_other_files():
    s3 = FakeS3([
        "bronze_landing/orders/dt=2026-10-26/a+0+1-2.jsonl",
        "bronze_landing/orders/dt=2026-10-26/b+0+3-4.jsonl",
        "bronze_landing/orders/_notes.txt",
        "bronze_validated/orders/dt=2026-10-26/a+0+1-2.jsonl",
    ])
    assert redrive.list_relative(s3, "b", "bronze_landing/") == {
        "orders/dt=2026-10-26/a+0+1-2.jsonl",
        "orders/dt=2026-10-26/b+0+3-4.jsonl",
    }


def test_s3_event_encodes_key_like_s3():
    key = "bronze_landing/orders/dt=2026-10-26/a+0+1-2.jsonl"
    encoded = redrive.s3_event("b", key)["Records"][0]["s3"]["object"]["key"]
    assert "%2B" in encoded and "%3D" in encoded
    assert urllib.parse.unquote_plus(encoded) == key
