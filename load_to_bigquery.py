import os
import json
import sys
import glob
import shutil
import logging
from google.cloud import bigquery
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

PROJECT_ID = "nl-transit-pipeline"
DATASET_ID = "nl_transit"
TABLE_ID = "departures_raw"
STAGING_TABLE_ID = "departures_staging"  # temporary holding table for this run's batch

client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
staging_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"


def convert_timestamp(ts):
    """Converts NS's ISO 8601 format into what BigQuery's batch loader expects."""
    if ts is None:
        return None
    dt = datetime.fromisoformat(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Idempotency: only process files still sitting in data/raw/ ─────
os.makedirs("data/loaded", exist_ok=True)
files = glob.glob("data/raw/departures_asd_*.json")

if not files:
    logger.info("No new files to load.")
    exit()

logger.info(f"Found {len(files)} new files to load.")

# ── Flatten each file's departures into simple, flat rows ──────────
rows_to_insert = []
for filepath in files:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    departures = data["payload"]["departures"]
    filename = os.path.basename(filepath)

    for d in departures:
        rows_to_insert.append({
            "source_file": filename,
            "direction": d.get("direction"),
            "train_name": d.get("name"),
            "planned_datetime": convert_timestamp(d.get("plannedDateTime")),
            "actual_datetime": convert_timestamp(d.get("actualDateTime")),
            "planned_track": d.get("plannedTrack"),
            "actual_track": d.get("actualTrack"),
            "cancelled": d.get("cancelled"),
            "category": d.get("trainCategory"),
            "departure_status": d.get("departureStatus"),
        })

logger.info(f"Prepared {len(rows_to_insert)} rows total.")

# ── Deduplicate within this batch before it ever reaches BigQuery ──
# Same train can be polled several times in one batch (e.g. ingest ran
# every 5 min while this hourly job was backlogged). Keep only the most
# recent actual_datetime per (train_name, planned_datetime, direction).
deduped = {}
for row in rows_to_insert:
    key = (row["train_name"], row["planned_datetime"], row["direction"])
    existing = deduped.get(key)
    if existing is None or (row["actual_datetime"] or "") > (existing["actual_datetime"] or ""):
        deduped[key] = row

dropped = len(rows_to_insert) - len(deduped)
if dropped:
    logger.info(f"Deduplicated batch: dropped {dropped} stale duplicate poll(s), kept {len(deduped)} rows.")
rows_to_insert = list(deduped.values())

# ── Table schema: explicit types, not auto-inferred ─────────────────
schema = [
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("direction", "STRING"),
    bigquery.SchemaField("train_name", "STRING"),
    bigquery.SchemaField("planned_datetime", "TIMESTAMP"),
    bigquery.SchemaField("actual_datetime", "TIMESTAMP"),
    bigquery.SchemaField("planned_track", "STRING"),
    bigquery.SchemaField("actual_track", "STRING"),
    bigquery.SchemaField("cancelled", "BOOLEAN"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("departure_status", "STRING"),
]

table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# staging table gets the same schema, created once if missing
staging_table = bigquery.Table(staging_ref, schema=schema)
staging_table = client.create_table(staging_table, exists_ok=True)

# ── Load this batch into staging (overwrite, not append) ──
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_TRUNCATE",  # staging only ever holds THIS run's batch
)

try:
    load_job = client.load_table_from_json(rows_to_insert, staging_ref, job_config=job_config)
    load_job.result()
    logger.info(f"Loaded {len(rows_to_insert)} deduplicated rows into staging.")

    # ── Rebuild departures_raw with deduplication ──
    # BigQuery Sandbox (free tier) blocks DML (MERGE/UPDATE/DELETE).
    # Workaround: rebuild the table entirely each run, combining existing
    # rows + this batch, deduplicated. CREATE OR REPLACE TABLE AS SELECT is
    # a query job, not DML, so it's allowed on the free tier.
    rebuild_sql = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * EXCEPT(rn)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY train_name, planned_datetime, direction
                    ORDER BY actual_datetime DESC
                ) AS rn
            FROM (
                SELECT * FROM `{table_ref}`
                UNION ALL
                SELECT * FROM `{staging_ref}`
            )
        )
        WHERE rn = 1
    """
    rebuild_job = client.query(rebuild_sql)
    rebuild_job.result()

    # CREATE OR REPLACE TABLE AS SELECT doesn't return a reliable row count
    # via the job result, so count explicitly after the rebuild completes.
    count_job = client.query(f"SELECT COUNT(*) AS n FROM `{table_ref}`")
    row_count = list(count_job.result())[0]["n"]
    logger.info(f"Rebuilt {TABLE_ID} with deduplication: {row_count} rows total.")

    # Only move files AFTER a confirmed successful rebuild
    for filepath in files:
        shutil.move(filepath, os.path.join("data/loaded", os.path.basename(filepath)))
    logger.info(f"Moved {len(files)} files to data/loaded/.")

except Exception as e:
    logger.error(f"Load failed, files left in place for retry: {e}")
    sys.exit(1)