# 0001. Delta Lake on S3-compatible storage (MinIO local, S3 + Unity Catalog in cloud)

- Status: Accepted
- Date: 2026-10-12

## Context
The pipeline needs ACID updates. CDC from Debezium means UPDATEs and DELETEs, not just appends.
It needs history and time travel for SCD2 and debugging.
The laptop has 16 GB RAM and WSL capped at about 10 GB.
AWS spend is limited ($100 credits, budgets at $5 and $15).
Databricks is Free Edition, which is serverless-only.

## Decision
The table format is Delta Lake.
Locally, Delta goes on MinIO through s3a (path-style access).
In the cloud, the bucket is feastflow-lake-<id> in ap-south-2.
Databricks reaches it only through a Unity Catalog storage credential + external location backed by the IAM role feastflow-uc-access. That means no access keys in notebooks, a trust policy guarded by an External ID, and self-assume.

## Alternatives considered
Plain Parquet without a table format: no ACID, and no UPDATE without rewriting by hand.
Apache Iceberg: a strong option that PayPulse will use, but Databricks’ native path is Delta.
Access keys in Spark config instead of a role: long-lived secrets in code, versus temporary ASIA keys.
Local filesystem instead of MinIO: it wouldn’t exercise the S3 API, listing behaviour or s3a settings that production uses.

## Consequences
Databricks enables deletion vectors by default (you saw numCopiedRows 0), and that requires reader v3/writer v7. Other engines (Athena/Glue) may not read these tables: an open item to verify in M3.
Cross-region access: the bucket is in Hyderabad, and Databricks compute probably isn’t, so data transfer costs grow with volume.
File events are deferred to M2, so Auto Loader falls back to listing until then.
feastflow-dev holds AdministratorAccess temporarily, replaced by least-privilege Terraform in M3.
Behaviour differs between MinIO and Databricks: copy-on-write versus deletion vectors and auto compaction. Tests on local Spark won’t catch everything.