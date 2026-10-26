"""AWS Lambda: check every landed CDC file against its table's data contract.

Trigger: S3 ObjectCreated on bronze_landing/*.jsonl (async invoke, at-least-once).
For s3://<bucket>/bronze_landing/<table>/dt=<day>/<file>.jsonl it writes

    bronze_validated/<table>/dt=<day>/<file>.jsonl    the lines that pass
    bronze_quarantine/<table>/dt=<day>/<file>.jsonl   one {"errors", "line"} per line that fails

Output keys are derived from the input key, so a duplicate event or a retry
overwrites the same objects with the same bytes.

Handler setting: lambda_contract_check.handler
"""

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


def handler(event, context):
    results = []
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # Keys in S3 events are URL-encoded: our "+" arrives as "%2B", "=" as "%3D".
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        results.append(process(s3_client(), bucket, key))
    print(json.dumps(results))  # one line per invoke in CloudWatch Logs
    return results
