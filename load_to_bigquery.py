import os
import json
import glob
import shutil                    # lets us MOVE files, not just read them
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

client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def convert_timestamp(ts):
    """Converts NS's ISO 8601 format into what BigQuery's batch loader expects."""
    if ts is None:
        return None
    dt = datetime.fromisoformat(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Idempotency: only process files still sitting in data/raw/ ─────
# Once a file is loaded successfully, it gets MOVED to data/loaded/.
# So re-running this script can never load the same file twice.
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
        # .get() instead of direct access — returns None instead of crashing
        # if a field happens to be missing from a given record.

logger.info(f"Prepared {len(rows_to_insert)} rows total.")

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
table = client.create_table(table, exists_ok=True)  # exists_ok = don't error if already there

# ── Batch load (not streaming — required for BigQuery Sandbox) ─────
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_APPEND",  # add to existing data, don't overwrite
)

try:
    load_job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
    load_job.result()  # wait until the batch job actually finishes
    logger.info(f"Successfully loaded {len(rows_to_insert)} rows into BigQuery.")

    # Only move files AFTER a confirmed successful load — if the load fails,
    # files stay in data/raw/ so nothing is silently lost, and the next run retries them.
    for filepath in files:
        shutil.move(filepath, os.path.join("data/loaded", os.path.basename(filepath)))
    logger.info(f"Moved {len(files)} files to data/loaded/.")

except Exception as e:
    logger.error(f"Load failed, files left in place for retry: {e}")