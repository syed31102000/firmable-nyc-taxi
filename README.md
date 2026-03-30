# 🚖 Firmable NYC Taxi Assessment

## 📌 Overview

This project implements an end-to-end **data engineering pipeline** on the NYC TLC Yellow Taxi 2023 dataset (~38M rows).

The goal of this assignment is to demonstrate:

- Data modeling using dbt  
- Pipeline orchestration design (Airflow)  
- Data quality validation  
- Performance-aware SQL  
- Scalable data processing design (Spark)  

Additionally, I extended the project by building a **chat-based analytics agent** that allows users to query the mart layer using natural language. (Under progress)

---

## ⚙️ Tech Stack

| Layer | Tool |
|------|------|
| Warehouse | DuckDB |
| Modeling | dbt |
| Orchestration | Airflow (DAG code) |
| Processing (Scale) | PySpark |
| Agent Layer | Streamlit + LLM (Ollama/OpenAI) |
| Language | SQL, Python |

---

## 🏗️ Architecture


Raw Parquet Files
↓
DuckDB Raw Table
↓
dbt Staging Layer
↓
dbt Intermediate Layer
↓
dbt Mart Layer
↓
SQL Analysis / Agent Queries
↓
Airflow DAG (Orchestration)
↓
Spark (Historical Processing)


---

## 🧠 Modeling Decisions

### Staging Layer
- Standardized column names (snake_case)
- Explicit type casting
- Derived:
  - trip_duration_minutes
  - pickup_date, pickup_month, pickup_hour

👉 Ensures schema consistency and prevents downstream issues.

---

### Intermediate Layer
- Joins taxi trips with zone lookup
- Filters invalid records:
  - trip_distance > 0
  - fare_amount > 0
  - passenger_count > 0
  - duration between 1–180 minutes

👉 Keeps staging raw while applying business logic cleanly.

---

### Mart Layer

#### fct_trips
- Cleaned fact table
- Deduplicated
- Surrogate key generated

#### dim_zones
- Zone dimension table

#### agg_daily_revenue
- Daily KPIs:
  - total trips
  - total fare
  - avg fare
  - total tips
  - tip rate %

#### agg_zone_performance
- Zone-level performance:
  - revenue rank (monthly)
  - high-volume zones

👉 Monthly ranking is used instead of global ranking because it reflects seasonality and is more useful for business decisions.

---

## 🚀 Setup Instructions

### 1. Install dependencies
```bash
pip install -r requirements.txt
2. Download data
python scripts/download_data.py
3. Initialize warehouse
python scripts/init_duckdb.py
4. Run dbt
cd dbt
dbt seed
dbt run
dbt test
cd ..
5. Run validation checks
python scripts/run_local_checks.py
🔁 Airflow Pipeline

DAG: nyc_taxi_daily_pipeline

Flow
Check source freshness
Run staging models
Run intermediate models
Run mart models
Run dbt tests (fail on error)
Notify success
Features
retries = 2
retry delay = 5 minutes
backfill support
📈 SQL Challenges

Located in queries/

Top zones by revenue (window function)
Hour-of-day demand pattern
Trip gap analysis (LAG)
⚡ Spark (Scale Challenge)

File: spark/process_historical.py

What it does
Reads Parquet files
Applies cleaning logic
Computes daily aggregation
Writes partitioned output
⚠️ Spark Execution Issue
Problem
java.lang.UnsupportedOperationException: getSubject is not supported
Root Cause
Environment uses Java 25
Spark supports Java 17 / 21
Solution
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
unset JAVA_TOOL_OPTIONS

python spark/process_historical.py

👉 In production, this would run on EMR / Glue / Databricks.

🤖 Analytics Agent (Bonus)

Built a lightweight AI agent:

Features
Natural language → SQL
Queries mart tables
Returns results + explanation
Streamlit UI + CLI
Run
streamlit run agent/app.py
🧪 Data Quality
Built-in tests
not_null
unique
Custom tests
total_amount >= fare_amount
trip duration range
Deduplication
Removed duplicates using business grain
Generated surrogate trip_id
🧠 Brainstormer Answer

If dbt tests fail:

👉 marts should NOT be exposed

Approach

Use blue/green deployment:

build in staging
validate
publish only if tests pass
⚖️ Trade-offs
Used DuckDB instead of Snowflake (simpler setup)
Airflow DAG implemented but not deployed fully
Spark execution limited due to Java constraints
Focused on logic + structure over infra complexity
🤖 AI Usage

Used AI tools to:

scaffold dbt models
generate DAG structure
assist SQL queries
debug errors

All outputs were reviewed and refined manually.

📂 Project Structure
dbt/
dags/
queries/
spark/
agent/
scripts/
data/
README.md
🏆 Summary

This project demonstrates:

End-to-end data pipeline design
dbt modeling best practices
Data quality validation
Orchestration thinking
Performance-aware SQL
AI-powered analytics
🚀 Final Thought

The focus of this project is not just correctness, but:

designing systems
ensuring reliability
validating data
thinking at scale