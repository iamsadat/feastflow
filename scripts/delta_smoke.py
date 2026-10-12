"""Write a Delta table to MinIO, read it back, update it, and show the history."""
import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
USER = os.environ.get("MINIO_ROOT_USER", "feastflow")
PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
PATH = "s3a://lake/smoke/orders"

builder = (
    SparkSession.builder.appName("delta-smoke")
    .master("local[2]")
    # Delta: the SQL extension and catalog make Spark understand Delta tables.
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # S3A: Spark's connector for S3-style storage, pointed at MinIO instead of AWS.
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", USER)
    .config("spark.hadoop.fs.s3a.secret.key", PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)
# Downloads the jars Spark needs to talk to S3 (hadoop-aws must match Spark's Hadoop 3.3.4).
spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ],
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Write 1: create the table.
rows = [(1042, "placed", 640), (1043, "placed", 220)]
df = spark.createDataFrame(rows, ["order_id", "status", "order_total"])
df.write.format("delta").mode("overwrite").save(PATH)

# Read it back.
spark.read.format("delta").load(PATH).orderBy("order_id").show()

# Write 2: an update, like order 1042 moving from placed to accepted.
table = DeltaTable.forPath(spark, PATH)
table.update("order_id = 1042", {"status": "'accepted'"})

# Current state, an old version (time travel), and the commit history.
spark.read.format("delta").load(PATH).orderBy("order_id").show()
spark.read.format("delta").option("versionAsOf", 0).load(PATH).orderBy("order_id").show()
table.history().select("version", "operation", "timestamp").show(truncate=False)

spark.stop()