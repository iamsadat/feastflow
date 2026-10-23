"""Land FeastFlow CDC events from Kafka in the lake (MinIO / S3) as JSON-lines files.

For every Kafka partition the sink buffers records, then writes one object per batch:

    s3://<bucket>/<prefix>/<table>/dt=<YYYY-MM-DD>/<topic>+<partition>+<first>-<last>.jsonl

and only after the upload succeeds commits the Kafka offset (last + 1).
A crash between upload and commit means the batch is read and written again,
so delivery is at-least-once; Silver deduplicates by primary key + LSN.

Usage:
    uv run --env-file .env python ingest/kafka_to_s3.py
    uv run --env-file .env python ingest/kafka_to_s3.py --exit-when-idle 10
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaException, TopicPartition


stop_requested = False


def request_stop(signum, frame):
    """Flush what is buffered, commit, then exit (Ctrl-C or docker stop)."""
    global stop_requested
    stop_requested = True


class PartitionBuffer:
    """Records read from one Kafka partition that are not in the lake yet."""

    def __init__(self):
        self.lines = []
        self.first_offset = None
        self.last_offset = None
        self.first_timestamp_ms = None
        self.started = None

    def add(self, line, offset, timestamp_ms):
        if self.first_offset is None:
            self.first_offset = offset
            self.first_timestamp_ms = timestamp_ms
            self.started = time.monotonic()
        if line is not None:
            self.lines.append(line)
        self.last_offset = offset

    def is_empty(self):
        return self.first_offset is None


def to_line(msg):
    """One JSON line: the Debezium event plus where it came from in Kafka."""
    return json.dumps(
        {
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
            "kafka_timestamp_ms": msg.timestamp()[1],
            "key": json.loads(msg.key()) if msg.key() else None,
            "value": json.loads(msg.value()),
        },
        separators=(",", ":"),
    )


def object_key(prefix, topic, partition, buf):
    """Deterministic name: replaying the same offsets overwrites the same object."""
    table = topic.rsplit(".", 1)[-1]
    day = datetime.fromtimestamp(buf.first_timestamp_ms / 1000, tz=timezone.utc)
    return (
        f"{prefix}/{table}/dt={day:%Y-%m-%d}/"
        f"{topic}+{partition}+{buf.first_offset:020d}-{buf.last_offset:020d}.jsonl"
    )


def make_s3_client():
    """MinIO when S3_ENDPOINT_URL is set, real AWS S3 when it is not.

    Explicit keys are only for MinIO. On AWS leave them unset so boto3 uses its
    credential chain (env, ~/.aws profile, IAM role) and never a key in .env.
    """
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION"))


class Sink:
    def __init__(self, consumer, s3, bucket, prefix, batch_size, flush_seconds):
        self.consumer = consumer
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.buffers = {}  # (topic, partition) -> PartitionBuffer
        self.counts = {"events": 0, "tombstones": 0, "files": 0}

    def add(self, msg):
        tp = (msg.topic(), msg.partition())
        buf = self.buffers.setdefault(tp, PartitionBuffer())
        if msg.value() is None:
            # Tombstone after a delete: nothing to land, but its offset must
            # still be committed or the sink would re-read it forever.
            buf.add(None, msg.offset(), msg.timestamp()[1])
            self.counts["tombstones"] += 1
        else:
            buf.add(to_line(msg), msg.offset(), msg.timestamp()[1])
            self.counts["events"] += 1

    def flush(self, tp):
        buf = self.buffers.pop(tp, None)
        if buf is None or buf.is_empty():
            return
        topic, partition = tp
        if buf.lines:
            key = object_key(self.prefix, topic, partition, buf)
            body = ("\n".join(buf.lines) + "\n").encode()
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
            self.counts["files"] += 1
            print(f"wrote s3://{self.bucket}/{key} ({len(buf.lines)} events)", flush=True)
        # Commit only after the upload succeeded: offset = next record to read.
        self.consumer.commit(
            offsets=[TopicPartition(topic, partition, buf.last_offset + 1)],
            asynchronous=False,
        )

    def flush_due(self):
        now = time.monotonic()
        for tp, buf in list(self.buffers.items()):
            full = len(buf.lines) >= self.batch_size
            old = not buf.is_empty() and now - buf.started >= self.flush_seconds
            if full or old:
                self.flush(tp)

    def flush_all(self):
        for tp in list(self.buffers):
            self.flush(tp)

    def on_revoke(self, consumer, partitions):
        """Rebalance: land and commit what we hold before the partitions move."""
        for p in partitions:
            self.flush((p.topic, p.partition))


def run(args):
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    s3 = make_s3_client()
    sink = Sink(consumer, s3, args.bucket, args.prefix, args.batch_size, args.flush_seconds)

    consumer.subscribe([args.topics], on_revoke=sink.on_revoke)
    print(f"Reading {args.topics} as group {args.group}, writing s3://{args.bucket}/{args.prefix}/",
          flush=True)

    last_message = time.monotonic()
    try:
        while not stop_requested:
            msg = consumer.poll(1.0)
            if msg is None:
                if args.exit_when_idle and time.monotonic() - last_message >= args.exit_when_idle:
                    break
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                sink.add(msg)
                last_message = time.monotonic()
            sink.flush_due()
        sink.flush_all()
    finally:
        consumer.close()  # leave the group so partitions move immediately

    return sink.counts


def main(argv=None):
    parser = argparse.ArgumentParser(description="FeastFlow Kafka to S3 sink")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topics", default=r"^feastflow\.public\..*",
                        help="topic regex (must start with ^)")
    parser.add_argument("--group", default="feastflow-bronze-sink")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "lake"))
    parser.add_argument("--prefix", default="bronze_landing")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-seconds", type=float, default=30.0)
    parser.add_argument("--exit-when-idle", type=float, default=0,
                        help="stop after this many seconds without records (0 = run forever)")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        counts = run(args)
    except Exception as exc:
        print(f"Sink failed: {exc}", file=sys.stderr)
        return 1

    print(f"Stopped. Totals: {counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
