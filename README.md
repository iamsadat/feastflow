# FeastFlow
FeastFlow is a modern lakehouse which handles batch ingestion using a PostgreSQL generator which also has the feature of CDC. It follows a medallion architecture which handles SCD2 in the silver layer while the gold layer serves analytical dashboards.

# Commands
make test: runs the unit tests