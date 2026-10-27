# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: contract-checked CDC files -> Delta with Auto Loader
# MAGIC
# MAGIC Reads `s3://<bucket>/bronze_validated/<table>/` and appends every new line to
# MAGIC `feastflow.bronze.<table>_cdc`. Discovery is directory listing (ADR 0003).
# MAGIC Trigger is `availableNow`: process everything new, then stop, so a scheduled
# MAGIC job costs nothing between runs. Run it from a Git folder of the repo so
# MAGIC `contracts/` and `ingest/*.py` are next to it.

# COMMAND ----------

# MAGIC %pip install pyyaml==6.0.2

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = Path.cwd()                        # the notebook's folder: <git folder>/ingest
sys.path.insert(0, str(HERE))
import bronze_schema  # noqa: E402
import contract  # noqa: E402

spark = SparkSession.builder.getOrCreate()  # the notebook's session; explicit so linters see it

BUCKET = "feastflow-lake-072270723370"
SOURCE = f"s3://{BUCKET}/bronze_validated"
STATE = f"s3://{BUCKET}/_autoloader"     # one checkpoint per table, outside any table
TARGET = "feastflow.bronze"

CONTRACTS = contract.load_contracts(HERE.parent / "contracts")

# COMMAND ----------

def load_table(table, spec):
    """One availableNow stream: new files under SOURCE/<table>/ -> TARGET.<table>_cdc."""
    landed = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(bronze_schema.bronze_ddl(spec))
        .load(f"{SOURCE}/{table}/")
    )
    rows = landed.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_file_ts"),
        F.current_timestamp().alias("_ingested_at"),
    )
    query = (
        rows.writeStream
        .option("checkpointLocation", f"{STATE}/{table}/checkpoint")
        .trigger(availableNow=True)
        .toTable(f"{TARGET}.{table}_cdc")
    )
    query.awaitTermination()
    progress = query.recentProgress
    return sum(p["numInputRows"] for p in progress)


for table, spec in CONTRACTS.items():
    print(f"{table}: {load_table(table, spec)} new rows")