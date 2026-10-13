# 0001. Delta Lake on S3-compatible storage (MinIO local, S3 + Unity Catalog in cloud)

- Status: Accepted
- Date: 2026-10-13

## Context
FeastFlow ingests food-delivery orders from Postgres. Debezium captures every
INSERT, UPDATE and DELETE from the database log and streams them through Kafka
into a lakehouse with Bronze, Silver and Gold layers.

- Storage must apply UPDATEs and DELETEs atomically (ACID); plain files in S3
  are immutable and can only be replaced whole.
- Silver keeps full history (SCD2), and we want time travel for debugging.
- Development runs on a laptop: WSL capped at 10 GB RAM, no cloud cost.
- Cloud spend must stay small: AWS $100 credits, budget alerts at $5 and $15.
- Databricks Free Edition is serverless-only, so compute runs in Databricks'
  AWS account and must be granted access to ours.

## Decision
- **Table format: Delta Lake**, because it gives versioning, time travel and
  atomic transactions (optimistic concurrency) on top of plain Parquet files,
  and Unity Catalog, Auto Loader and Lakeflow on Databricks work with it natively.
- **Local storage: MinIO via s3a** (path-style access), because it lets us
  experience S3-style buckets, the S3 API and s3a settings locally instead of
  a plain filesystem, at zero cloud cost.
- **Cloud storage: bucket `feastflow-lake-<account-id>` in ap-south-2
  (Hyderabad).** Bucket names are global, so the account ID makes the name
  unique. The region was the CLI default; we keep it because it is closest to
  the only developer. Caveat: Databricks compute runs in another region, so
  data crosses regions (see Consequences).
- **Access: Databricks reaches the bucket only through a Unity Catalog storage
  credential `feastflow_s3_cred` + external location `feastflow_lake`,** backed
  by IAM role `feastflow-uc-access`, because it uses short-lived STS keys
  instead of long-lived access keys. The role trusts only Databricks' Unity
  Catalog role and itself, guarded by an External ID.

## Alternatives considered
- **Plain Parquet, no table format:** rejected; no ACID, and an UPDATE means
  rewriting files by hand with no history.
- **Apache Iceberg:** rejected for FeastFlow because Databricks' native path is
  Delta (PayPulse will use Iceberg). Reconsider if Athena/Glue cannot read
  our Delta tables in M3.
- **Access keys in Spark config:** rejected; a leaked key works from anywhere
  until revoked, while role keys expire on their own.
- **Local filesystem instead of MinIO:** rejected; it does not exercise the S3
  API, listing behaviour or s3a configuration that the cloud uses.

## Consequences
- Verified on 2026-10-12. Locally (Spark 3.5.3, Delta 3.2.1), an UPDATE
  rewrote the whole file (copy-on-write). On Databricks Runtime 19.9
  serverless, the same UPDATE wrote a 43-byte deletion vector with 0 rows
  copied, and auto compaction then merged 2 files (2580 bytes) into 1 (1306).
  The external-location test passed Read, List, Write, Delete, Assume Role,
  Self Assume and External ID checks.
- Deletion vectors raise the table protocol to reader v3 / writer v7; other
  engines (Athena/Glue) may not read these tables. To verify in M3.
- Cross-region reads from ap-south-2 add data-transfer cost as volume grows.
- ap-south-2 is an opt-in region; STS tokens from the global endpoint may be
  rejected there (not hit so far).
- File events (S3 notifications via SNS/SQS) are deferred to M2; until then
  Auto Loader discovers files by listing.
- `feastflow-dev` has AdministratorAccess temporarily; replaced by
  least-privilege Terraform in M3.
- Local and Databricks behaviour differ, so local tests do not prove
  Databricks behaviour.