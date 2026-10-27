"""Spark DDL schema for one Bronze table, built from its data contract.

Auto Loader can infer a schema, but inference guesses from a sample: a table
whose sample holds only inserts and snapshot reads sees `before` as always null
and infers it as STRING, then the first update's `before` object no longer fits.
Building the schema from contracts/<table>.yaml makes `before` and `after` the
same struct for every table, from the first file.

Bronze keeps values as they landed: Debezium's decimal and timestamp strings stay
STRING (Silver casts them), integers become BIGINT. Fields that are not in this
schema (a new column, a new Debezium field) go to _rescued_data, never dropped.

No Spark imports, so pytest can check the DDL without a cluster.
"""

SPARK_TYPES = {
    "integer": "BIGINT",
    "string": "STRING",
    "boolean": "BOOLEAN",
    "decimal": "STRING",      # "4.5" as Debezium sent it (decimal.handling.mode=string)
    "timestamp": "STRING",    # "2026-10-26T10:15:30.123456Z"
    "time_micros": "BIGINT",  # microseconds since midnight
}

# Debezium PostgreSQL `source` block (connector 3.0).
SOURCE = ("version STRING, connector STRING, name STRING, ts_ms BIGINT, ts_us BIGINT, "
          "ts_ns BIGINT, snapshot STRING, db STRING, sequence STRING, schema STRING, "
          "`table` STRING, txId BIGINT, lsn BIGINT, xmin BIGINT")

TRANSACTION = "id STRING, total_order BIGINT, data_collection_order BIGINT"

def row_struct(contract, columns):
    fields = ", ".join(f"{name}: {SPARK_TYPES[contract['columns'][name]['type']]}"
                       for name in columns)
    return f"STRUCT<{fields}>"

def bronze_ddl(contract):
    """DDL for one landed line: the sink's Kafka fields plus the Debezium envelope."""
    row = row_struct(contract, contract["columns"])
    key = row_struct(contract, contract["primary_key"])
    value = (f"STRUCT<before: {row}, after: {row}, source: STRUCT<{SOURCE}>, "
             f"transaction: STRUCT<{TRANSACTION}>, op: STRING, ts_ms: BIGINT, ts_us: BIGINT, ts_ns: BIGINT>")
    return (f"kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, "
            f"kafka_timestamp_ms BIGINT, key {key}, value {value}")