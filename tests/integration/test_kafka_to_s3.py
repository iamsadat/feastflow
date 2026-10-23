"""Integration test: needs the compose stack (Kafka + MinIO) running.

    uv run --env-file .env pytest -m integration tests/test_kafka_to_s3.py
"""

import json
import os
import sys
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))
import kafka_to_s3  # noqa: E402

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
BUCKET = os.environ.get("S3_BUCKET", "lake")


@pytest.fixture
def run_id():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def s3():
    return kafka_to_s3.make_s3_client()


@pytest.fixture
def topic(run_id):
    """A throwaway topic, so the test never touches the real CDC topics."""
    name = f"ittest.public.orders_{run_id}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result()
    yield name
    admin.delete_topics([name])[name].result()


def test_events_land_tombstones_skipped_offsets_committed(s3, topic, run_id):
    events = [
        ({"order_id": 1}, {"op": "c", "after": {"order_id": 1, "status": "PLACED"}}),
        ({"order_id": 1}, {"op": "u", "after": {"order_id": 1, "status": "ACCEPTED"}}),
        ({"order_id": 2}, {"op": "d", "before": {"order_id": 2}, "after": None}),
    ]
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for key, value in events:
        producer.produce(topic, key=json.dumps(key), value=json.dumps(value))
    producer.produce(topic, key=json.dumps({"order_id": 2}), value=None)  # tombstone
    assert producer.flush(10) == 0

    prefix = f"ittest/{run_id}"
    group = f"ittest-{run_id}"
    exit_code = kafka_to_s3.main([
        "--bootstrap", BOOTSTRAP, "--topics", f"^{topic}$", "--group", group,
        "--bucket", BUCKET, "--prefix", prefix, "--exit-when-idle", "5",
    ])
    assert exit_code == 0

    keys = []
    try:
        listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
        keys = [obj["Key"] for obj in listed]
        assert len(keys) == 1
        assert keys[0].endswith(f"{topic}+0+{0:020d}-{3:020d}.jsonl")

        body = s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read().decode()
        rows = [json.loads(line) for line in body.splitlines()]
        assert [r["kafka_offset"] for r in rows] == [0, 1, 2]       # tombstone not landed
        assert [r["value"]["op"] for r in rows] == ["c", "u", "d"]
        assert rows[0]["key"] == {"order_id": 1}

        checker = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
        committed = checker.committed([TopicPartition(topic, 0)], timeout=10)
        checker.close()
        assert committed[0].offset == 4                              # past the tombstone
    finally:
        for key in keys:
            s3.delete_object(Bucket=BUCKET, Key=key)
