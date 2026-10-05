"""
NL Transit Pipeline — Ingestion DAG
=====================================
Runs ONLY the ingest step, every 5 minutes.

Split out from the original single DAG (nl_transit_dag.py) because
ingest needs fresh data often, but load/dbt do not — see
nl_transit_transform_dag.py for the hourly counterpart.
"""

from airflow.sdk import DAG, task
from datetime import datetime, timedelta
from airflow.models import Variable


@task(task_id="ingest")
def run_ingest():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingest import fetch_departures

    primary_key = Variable.get("ns_api_key")
    secondary_key = Variable.get("ns_api_key_secondary")

    success = fetch_departures(primary_key=primary_key, secondary_key=secondary_key)

    if not success:
        raise RuntimeError("Ingestion failed: see task logs above for the specific cause (transient failure or both keys rejected).")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nl_transit_ingest",
    description="Pulls NS departures every 5 minutes and saves raw JSON.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["nl-transit", "portfolio", "ingest"],
) as dag:

    run_ingest()
