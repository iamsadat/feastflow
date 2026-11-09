"""AWS Lambda: check every landed CDC file against its table's data contract.

Trigger: EventBridge rule landing-to-contract-check on S3 "Object Created" under
bronze_landing/ (async invoke, at-least-once). The S3 notification shape is still
accepted, because ingest/redrive.py sends it.

Handler setting: lambda_contract_check.handler
"""

import hashlib
import json
import urllib.parse
from pathlib import Path

import boto3
import contract

LANDING = "bronze_landing/"
VALIDATED = "bronze_validated/"
QUARANTINE = "bronze_quarantine/"

HERE = Path(__file__).resolve().parent
# In the Lambda zip contracts/ sits next to this file; in the repo it is one level up.
CONTRACTS_DIR = HERE / "contracts" if (HERE / "contracts").is_dir() else HERE.parent / "contracts"
# Loaded once per execution environment (cold start), reused by warm invokes.
CONTRACTS = contract.load_contracts(CONTRACTS_DIR)

_s3 = None


def s3_client():
    """Created on first use: credentials come from the execution role."""
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def process(s3, bucket, key):
    """Validate one landed object and write its validated / quarantine twins."""
    if not key.startswith(LANDING):
        raise ValueError(f"not a landing key: {key}")
    relative = key[len(LANDING):]              # <table>/dt=<day>/<file>.jsonl
    table = relative.split("/", 1)[0]

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    spec = CONTRACTS.get(table)
    if spec is None:
        # No contract = nothing to check against: quarantine everything, never guess.
        valid = []
        rejected = [{"line": line, "errors": [f"no contract for table {table}"]}
                    for line in body.splitlines() if line.strip()]
    else:
        valid, rejected = contract.split_lines(body, spec)

    if valid:
        s3.put_object(Bucket=bucket, Key=VALIDATED + relative,
                      Body=("\n".join(valid) + "\n").encode())
    if rejected:
        lines = [json.dumps({"errors": r["errors"], "line": r["line"]}) for r in rejected]
        s3.put_object(Bucket=bucket, Key=QUARANTINE + relative,
                      Body=("\n".join(lines) + "\n").encode())

    return {"key": key, "valid": len(valid), "rejected": len(rejected)}


def replay(s3, bucket, key):
    """Re-check one quarantine file; write the lines that now pass under a new key."""
    if not key.startswith(QUARANTINE):
        raise ValueError(f"not a quarantine key: {key}")
    relative = key[len(QUARANTINE):]           # <table>/dt=<day>/<file>.jsonl
    table = relative.split("/", 1)[0]

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    lines = [json.loads(entry)["line"] for entry in body.splitlines() if entry.strip()]
    spec = CONTRACTS.get(table)
    if spec is None:
        valid, still_rejected = [], lines
    else:
        valid, still_rejected = contract.split_lines("\n".join(lines), spec)

    output = None
    if valid:
        data = ("\n".join(valid) + "\n").encode()
        digest = hashlib.sha256(data).hexdigest()[:12]
        output = VALIDATED + relative.removesuffix(".jsonl") + f".replay-{digest}.jsonl"
        s3.put_object(Bucket=bucket, Key=output, Body=data)

    return {"key": key, "replayed": len(valid), "still_rejected": len(still_rejected),
            "output": output}


def object_refs(event):
    """(bucket, key) for every object an event names, in either event shape."""
    if "Records" in event:
        # S3 notification: keys are URL-encoded, our "+" arrives as "%2B", "=" as "%3D".
        return [(r["s3"]["bucket"]["name"], urllib.parse.unquote_plus(r["s3"]["object"]["key"]))
                for r in event["Records"]]
    # EventBridge "Object Created": one object, key as plain text (seen on a captured
    # event, Oct 27). Decoding it again would turn every "+" into a space.
    detail = event["detail"]
    return [(detail["bucket"]["name"], detail["object"]["key"])]


def handler(event, context):
    if "replay" in event:
        ref = event["replay"]
        results = [replay(s3_client(), ref["bucket"], ref["key"])]
        print(json.dumps(results))
        return results
    results = []
    for bucket, key in object_refs(event):
        results.append(process(s3_client(), bucket, key))
    print(json.dumps(results))  # one line per invoke in CloudWatch Logs
    return results
