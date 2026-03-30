# firmable-nyc-taxi
# Firmable NYC Taxi Assessment

## Overview
This project models the NYC TLC Yellow Taxi 2023 dataset into an analytics-ready warehouse using DuckDB + dbt, orchestrated by an Airflow DAG, with SQL challenge queries and a PySpark historical processing script.

## Why DuckDB
I used DuckDB to mock the warehouse layer because it provides native Parquet support, is easy to run in a restricted environment, and makes the project fully reproducible without cloud credentials.

## Architecture
- Raw Parquet files loaded into DuckDB
- dbt staging models standardize and cast columns
- dbt intermediate model enriches trips with pickup/dropoff zone details and filters invalid rows
- dbt marts provide fact, dimension, and aggregated reporting layers
- Airflow DAG orchestrates the pipeline daily

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
---

# 20. Run final checks

From repo root:

```bash
python scripts/download_data.py
python scripts/init_duckdb.py
cd dbt
dbt seed
dbt run
dbt test
cd ..


python spark/process_historical.py
python scripts/run_local_checks.pygh pr create --base main --head YOUR_BRANCH_NAME --title "..."