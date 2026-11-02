"""Lakeflow Declarative Pipeline feastflow_silver (ADR 0004).

Step 3: one streaming table feastflow.silver.<table>_changes per contract,
fed by a streaming read of feastflow.bronze.<table>_cdc. Each Bronze row is
read once (Bronze is append-only) and becomes one typed change row.

Pipeline configuration must set feastflow.repo_root to the Git folder, e.g.
/Workspace/Users/<you>/feastflow, so contracts/ and ingest/ can be found.
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()  # the pipeline's session; explicit so linters see it
ROOT = spark.conf.get("feastflow.repo_root")
sys.path[:0] = [f"{ROOT}/ingest", f"{ROOT}/silver"]
import contract
import silver_columns

CONTRACTS = contract.load_contracts(f"{ROOT}/contracts")


def define_changes(table, spec):
    """Register <table>_changes. A factory, so each closure keeps its own table."""

    @dp.table(
        name=f"feastflow.silver.{table}_changes",
        comment=f"Typed CDC events of {table}, one per Bronze row (contract v{spec['version']}).",
    )
    def _changes():
        bronze = spark.readStream.table(f"feastflow.bronze.{table}_cdc")
        return silver_columns.changes(bronze, spec)


for table, spec in CONTRACTS.items():
    define_changes(table, spec)
