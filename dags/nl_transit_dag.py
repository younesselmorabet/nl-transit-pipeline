"""
NL Transit Pipeline DAG
========================
Runs the full pipeline every 5 minutes: ingest -> load -> dbt run -> dbt test.

Each task here has already been run manually, by hand, inside the worker
container, and confirmed working BEFORE this file was written:
  - ingest:    python /opt/airflow/ingest.py  (as a function call, see below)
  - load:      python /opt/airflow/load_to_bigquery.py
  - dbt_run:   dbt run   (from /opt/airflow/transit_transform)
  - dbt_test:  dbt test  (from /opt/airflow/transit_transform)

This DAG just schedules that already-proven sequence.

Known trade-off, accepted deliberately: a single 5-minute schedule means
load/dbt_run/dbt_test also run every 5 minutes, which is more often than
they need to. Planned follow-up: split into two DAGs (ingest @5min,
load+dbt hourly). Not done yet.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable


# ── Task 1: ingest ──────────────────────────────────────────
# A PythonOperator-style task (via the @task decorator), because
# fetch_departures() is an actual importable function (we refactored it
# for exactly this reason). Variable.get() is called INSIDE the function,
# not at the top of this file — calling it at file level would hit the
# Airflow database every time the DAG file is parsed (every ~30s by the
# dag-processor), not just when the task actually runs.
@task(task_id="ingest")
def run_ingest():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingest import fetch_departures

    primary_key = Variable.get("ns_api_key")
    secondary_key = Variable.get("ns_api_key_secondary")

    success = fetch_departures(primary_key=primary_key, secondary_key=secondary_key)

    if not success:
        # PythonOperator-style tasks are marked failed by raising an
        # exception, not by a return value or exit code — unlike the
        # standalone script, which uses sys.exit(). This is the DAG-side
        # equivalent of that same signal.
        raise RuntimeError("Ingestion failed: both primary and secondary keys exhausted.")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nl_transit_pipeline",
    description="Ingest NS departures, load to BigQuery, transform with dbt.",
    schedule="*/5 * * * *",   # every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,            # don't backfill missed runs — only run going forward
    default_args=default_args,
    tags=["nl-transit", "portfolio"],
) as dag:

    ingest = run_ingest()

    # ── Task 2: load ─────────────────────────────────────────
    # Not a PythonOperator: load_to_bigquery.py has no function to import,
    # it's a top-level script. Running it as a subprocess, exactly as
    # tested manually with `docker exec`.
    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/load_to_bigquery.py",
    )

    # ── Task 3: dbt run ──────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/transit_transform && dbt run",
    )

    # ── Task 4: dbt test ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/transit_transform && dbt test",
    )

    ingest >> load >> dbt_run >> dbt_test