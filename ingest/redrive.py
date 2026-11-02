"""Re-drive landing files the contract-check Lambda never processed.

S3 only sends events for objects created after the trigger existed, and Lambda
drops an async event after 3 failed attempts. This lists bronze_landing/, finds
files with neither a bronze_validated/ nor a bronze_quarantine/ twin, and invokes
the Lambda asynchronously with the same event shape S3 sends.

With --replay it sends one replay event per bronze_quarantine/ file instead:
the Lambda re-checks the rejected lines against the current contracts and
writes the ones that now pass to a new bronze_validated/ key. Run it after
fixing a contract and deploying the new Lambda zip.

Needs s3:ListBucket and lambda:InvokeFunction, so run it with your admin profile,
not feastflow-sink:
    uv run python ingest/redrive.py --bucket feastflow-lake-<id> --dry-run
    uv run python ingest/redrive.py --bucket feastflow-lake-<id>
    uv run python ingest/redrive.py --bucket feastflow-lake-<id> --replay --dry-run
"""

import argparse
import json
import os
import sys
import urllib.parse

import boto3

LANDING = "bronze_landing/"
VALIDATED = "bronze_validated/"
QUARANTINE = "bronze_quarantine/"


def list_relative(s3, bucket, prefix):
    """Every .jsonl key under prefix, with the prefix cut off."""
    keys = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                keys.add(obj["Key"][len(prefix):])
    return keys


def s3_event(bucket, key):
    """The record shape S3 sends, key URL-encoded like the real thing."""
    return {"Records": [{
        "eventSource": "aws:s3",
        "s3": {"bucket": {"name": bucket},
               "object": {"key": urllib.parse.quote_plus(key, safe="/")}},
    }]}


def replay_event(bucket, key):
    """Our own shape: the Lambda re-checks one quarantine file (see its docstring)."""
    return {"replay": {"bucket": bucket, "key": key}}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Re-drive unprocessed landing files")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--function", default="feastflow-contract-check")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replay", action="store_true",
                        help="re-check bronze_quarantine/ files instead of re-driving landing")
    args = parser.parse_args(argv)
    if not args.bucket:
        parser.error("--bucket or S3_BUCKET is required")

    s3 = boto3.client("s3")
    lam = boto3.client("lambda")
    if args.replay:
        quarantined = sorted(list_relative(s3, args.bucket, QUARANTINE))
        print(f"quarantine={len(quarantined)}")
        for relative in quarantined:
            key = QUARANTINE + relative
            if args.dry_run:
                print(f"would replay {key}")
                continue
            lam.invoke(FunctionName=args.function, InvocationType="Event",
                       Payload=json.dumps(replay_event(args.bucket, key)).encode())
            print(f"replay sent for {key}")
        return 0

    landing = list_relative(s3, args.bucket, LANDING)
    done = list_relative(s3, args.bucket, VALIDATED) | list_relative(s3, args.bucket, QUARANTINE)
    todo = sorted(landing - done)
    print(f"landing={len(landing)} processed={len(landing & done)} to_redrive={len(todo)}")

    for relative in todo:
        key = LANDING + relative
        if args.dry_run:
            print(f"would invoke for {key}")
            continue
        # Event = async: Lambda queues it and retries like a real S3 event.
        lam.invoke(FunctionName=args.function, InvocationType="Event",
                   Payload=json.dumps(s3_event(args.bucket, key)).encode())
        print(f"invoked for {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
