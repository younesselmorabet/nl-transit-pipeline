"""
NL Transit Pipeline — Load & Transform DAG
=============================================
Runs load -> dbt run -> dbt test, once an hour.

Split out from the original single DAG (nl_transit_dag.py). Rebuilding
dbt models and re-running tests every 5 minutes (the ingest cadence) was
wasteful: it burns BigQuery Sandbox quota and adds no real benefit, since
nothing consumes the mart table that often. Ingestion still runs every
5 minutes, in the separate nl_transit_ingest_dag.py — this DAG just
processes whatever has accumulated in data/raw since it last ran.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nl_transit_transform",
    description="Loads raw NS data into BigQuery and runs dbt, hourly.",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["nl-transit", "portfolio", "transform"],
) as dag:

    load = BashOperator(
        task_id="load",
        bash_command="cd /opt/airflow && python load_to_bigquery.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/transit_transform && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/transit_transform && dbt test",
    )

    load >> dbt_run >> dbt_test
