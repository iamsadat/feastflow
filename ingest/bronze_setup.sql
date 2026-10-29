-- Run once in the Databricks SQL editor (or a notebook %sql cell).
-- The catalog's managed tables live in our own bucket, under the UC external
-- location feastflow_lake, so the Delta files stay in ap-south-2 next to the data.

CREATE CATALOG IF NOT EXISTS feastflow 
    MANAGED LOCATION 's3://feastflow-lake-072270723370/uc/feastflow';

CREATE SCHEMA IF NOT EXISTS feastflow.bronze
    COMMENT 'Conract-checked Debezium change events, one row per landed line';