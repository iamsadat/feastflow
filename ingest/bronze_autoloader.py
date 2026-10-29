# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: contract-checked CDC files -> Delta with Auto Loader
# MAGIC
# MAGIC Reads `s3://<bucket>/bronze_validated/<table>/` and appends every new line to
# MAGIC `feastflow.bronze.<table>_cdc`. Discovery is managed file events on the UC external location `feastflow_lake` (ADR 0003): new files come from the Databricks file cache, not from listing S3. Trigger is availableNow: process everything new, then stop; the job’s file arrival trigger starts each run.

# COMMAND ----------

# MAGIC %pip install pyyaml==6.0.2

# COMMAND ----------

import sys
import time
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

def row_count(name):
    return spark.table(name).count() if spark.catalog.tableExists(name) else 0


def load_table(table, spec):
    """One availableNow stream: new files under SOURCE/<table>/ -> TARGET.<table>_cdc."""
    target = f"{TARGET}.{table}_cdc"
    before = row_count(target)
    landed = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")
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
        .toTable(target)
    )
    query.awaitTermination()
    # Progress metrics are best-effort on serverless (numInputRows can be None);
    # the committed table is the truth, and Bronze is append-only, so the
    # row delta is exactly what this run added.
    return row_count(target) - before


for table, spec in CONTRACTS.items():
    started = time.monotonic()
    added = load_table(table, spec)
    print(f"{table} {added} new rows in {time.monotonic() - started:.1f}s")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS t,
# MAGIC        (SELECT count(*) FROM feastflow.bronze.orders_cdc) AS bronze,
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/orders/', format => 'json')) AS files
# MAGIC UNION ALL
# MAGIC SELECT 'restaurants',
# MAGIC        (SELECT count(*) FROM feastflow.bronze.restaurants_cdc),
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/restaurants/', format => 'json'))
# MAGIC UNION ALL
# MAGIC SELECT 'riders',
# MAGIC        (SELECT count(*) FROM feastflow.bronze.riders_cdc),
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/riders/', format => 'json'))
# MAGIC UNION ALL
# MAGIC SELECT 'customers',
# MAGIC        (SELECT count(*) FROM feastflow.bronze.customers_cdc),
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/customers/', format => 'json'))
# MAGIC UNION ALL
# MAGIC SELECT 'order_items',
# MAGIC        (SELECT count(*) FROM feastflow.bronze.order_items_cdc),
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/order_items/', format => 'json'))
# MAGIC UNION ALL
# MAGIC SELECT 'menu_items',
# MAGIC        (SELECT count(*) FROM feastflow.bronze.menu_items_cdc),
# MAGIC        (SELECT count(*) FROM read_files('s3://feastflow-lake-072270723370/bronze_validated/menu_items/', format => 'json'));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows, count(_rescued_data) AS rescued FROM feastflow.bronze.orders_cdc;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY feastflow.bronze.orders_cdc LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT value.op, value.before.status AS was, value.after.status AS now, value.source.lsn
# MAGIC FROM feastflow.bronze.orders_cdc
# MAGIC WHERE key.order_id = (SELECT key.order_id FROM feastflow.bronze.orders_cdc
# MAGIC                       WHERE value.after.status = 'DELIVERED' LIMIT 1)
# MAGIC ORDER BY value.source.lsn;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT _source_file) AS files, count(*) AS events, count(DISTINCT key.order_id) AS orders
# MAGIC FROM feastflow.bronze.orders_cdc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' t, count(_rescued_data) r FROM feastflow.bronze.orders_cdc
# MAGIC UNION ALL SELECT 'customers', count(_rescued_data) FROM feastflow.bronze.customers_cdc
# MAGIC UNION ALL SELECT 'restaurants', count(_rescued_data) FROM feastflow.bronze.restaurants_cdc
# MAGIC UNION ALL SELECT 'riders', count(_rescued_data) FROM feastflow.bronze.riders_cdc
# MAGIC UNION ALL SELECT 'menu_items', count(_rescued_data) FROM feastflow.bronze.menu_items_cdc
# MAGIC UNION ALL SELECT 'order_items', count(_rescued_data) FROM feastflow.bronze.order_items_cdc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from feastflow.bronze.orders_cdc