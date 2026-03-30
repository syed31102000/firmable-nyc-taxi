from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

REPO_ROOT = os.environ.get("REPO_ROOT", "/workspaces/firmable-nyc-taxi")
DBT_DIR = f"{REPO_ROOT}/dbt"
RAW_DIR = f"{REPO_ROOT}/data/raw"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def check_source_freshness_fn(**context):
    execution_date = context["ds"]  # YYYY-MM-DD
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    prev_month = dt.strftime("%Y-%m")
    expected_file = os.path.join(RAW_DIR, f"yellow_tripdata_{prev_month}.parquet")

    if not os.path.exists(expected_file):
        raise FileNotFoundError(f"Expected source file missing: {expected_file}")

def notify_success_fn():
    import duckdb
    con = duckdb.connect(f"{REPO_ROOT}/data/warehouse/nyc_taxi.duckdb")
    result = con.execute("""
        select
            count(*) as trip_count,
            sum(total_fare) as total_revenue
        from agg_daily_revenue
    """).fetchone()
    print(f"Pipeline succeeded. Trip count summary rows: {result[0]}, total revenue: {result[1]}")

with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    default_args=default_args,
    description="Daily NYC taxi dbt pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=["nyc", "taxi", "dbt"],
) as dag:

    check_source_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness_fn,
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging"
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate"
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts"
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_DIR} && dbt test"
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    check_source_freshness >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts >> run_dbt_tests >> notify_success