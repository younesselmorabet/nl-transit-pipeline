# NL Transit Data Pipeline

An end-to-end data pipeline that ingests live Dutch train departure data from the NS (Nederlandse Spoorwegen) API, stores it, loads it into a cloud warehouse, and transforms it into clean, tested, business-ready tables using dbt. Built as a data engineering portfolio project targeting roles in the Netherlands and Ireland.

## Architecture

This project follows an ELT pattern (Extract, Load, Transform) with a medallion architecture (raw → staging → marts) inside the warehouse.

```
NS Transit API
      |
      v
Ingest script (Python, Dockerized)   <- Extract
  - runs every 5 minutes
  - retries failed calls
  - logs to console + file
      |
      v
Raw JSON storage (local disk, via Docker volume mount)
  - one timestamped file per pull
      |
      v
Load script (Python)                 <- Load
  - batch loads into BigQuery
  - idempotent: moves processed
    files so nothing loads twice
      |
      v
BigQuery: departures_raw             <- "bronze" layer, untouched raw data
      |
      v
dbt: stg_departures                  <- Transform ("silver" layer)
  - cleaned, renamed, typed
  - tested (not_null checks)
  - adds delay_minutes
      |
      v
dbt: mart_delays_by_destination      <- Transform ("gold" layer)
  - aggregated, business-facing
  - one row per destination
      |
      v
  [Dashboard - planned]
```

## Why these choices

**ELT instead of ETL.** Data is loaded into BigQuery raw and transformed afterward, using the warehouse's own compute (via dbt) rather than transforming before loading. This is the modern standard pattern for cloud warehouses, which are powerful enough to make transform-after-load more efficient than the older transform-before-load approach.

**BigQuery Sandbox, not paid cloud storage.** Built without a billing account, using BigQuery's free sandbox tier. This meant skipping a separate cloud storage (GCS) staging layer — raw files are loaded directly from local disk into BigQuery instead. A deliberate cost trade-off, not an oversight.

**Batch loading, not streaming inserts.** BigQuery Sandbox blocks streaming inserts. Batch loading is used instead, which also happens to be the more appropriate pattern for a pipeline that runs on a fixed schedule rather than reacting to real-time events.

**Docker for the ingestion script, with a bind-mounted data folder.** The ingestion script is containerized so it runs identically on any machine, without depending on a local Python version or virtual environment. The API key is never baked into the image; it is passed at runtime with `--env-file`. A container's filesystem is isolated and disposable, so the local `data/` folder is mounted into the container (`-v`). Without the mount, raw files stay inside the container and are lost when it is removed, and the load script cannot see them. In a production setup, raw files would go to object storage (GCS or S3) instead of a shared volume; that was ruled out here to avoid needing a billing account.

**A simple Python scheduler, not Airflow (yet).** Automation is currently handled with the `schedule` library, to prove the pipeline logic end-to-end before adding orchestration overhead. Migrating to Airflow — to properly sequence ingest → load → transform with dependency tracking — is the next planned step.

**Idempotent loading.** The load script tracks which raw files have already been loaded by moving them to `data/loaded/` on success. Re-running the script can never duplicate data, and a failed load leaves files in place to retry safely.

**Retry logic + structured logging.** The ingestion script retries failed API calls with backoff instead of silently failing, and logs to both console and file with timestamps and severity levels instead of using print statements.

**Staging and marts, no intermediate/dimension layers.** The dbt project uses only `stg_` and `mart_` models. Intermediate (`int_`) models exist to share logic across multiple marts, and dimension (`dim_`) tables exist for separate reference entities (e.g. station metadata) — neither applies at this project's current scale, so they were deliberately left out rather than added for their own sake.

## Project structure

```
nl-transit-pipeline/
├── .env                          # API key (not committed)
├── .gitignore
├── README.md
├── Dockerfile                    # Container image for the ingestion script
├── requirements.txt              # Python dependencies
├── ingest.py                     # Extract: pulls from NS API, saves raw JSON
├── load_to_bigquery.py           # Load: batch loads raw JSON into BigQuery
├── pipeline.log                  # generated log output (not committed)
├── data/                         # not committed (gitignored)
│   ├── raw/                      # files waiting to be loaded
│   └── loaded/                   # files already loaded (idempotency record)
├── venv/                         # Python virtual environment (not committed)
└── transit_transform/            # dbt project: Transform
    ├── dbt_project.yml
    └── models/
        ├── sources.yml           # declares departures_raw as a source
        ├── stg_departures.sql    # staging model
        ├── mart_delays_by_destination.sql   # marts model
        └── schema.yml            # model documentation + tests
```

## Known limitations

- Single station (Amsterdam Centraal) — scope kept small and demonstrable; the pipeline logic generalizes to any station.
- BigQuery Sandbox data expires after 60 days without a billing account.
- No true real-time streaming — data is pulled every 5 minutes, not event-driven.
- Orchestration is currently manual/scheduler-based, not yet Airflow-managed.
- Only the ingestion script is containerized. The load script and dbt still run locally because they rely on local Google Cloud credentials.
- Raw data is shared between the ingestion container and the local machine through a bind mount, not through object storage.

## Setup

1. Clone the repo and create a virtual environment:
   ```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Create a `.env` file with your NS API key:
   ```
   NS_API_KEY=your_key_here
   ```
3. Authenticate with Google Cloud:
   ```
   gcloud auth application-default login
   ```
4. Run the ingestion script, either locally:
   ```
   python ingest.py
   ```
   or in Docker:
   ```
   docker build -t nl-transit-ingest .
   docker run --env-file .env -v "${PWD}/data:/app/data" nl-transit-ingest
   ```
   The `-v` flag mounts your local `data/` folder into the container, so raw files persist outside the container. The `${PWD}` syntax is for PowerShell.
5. In a separate run, load data into BigQuery:
   ```
   python load_to_bigquery.py
   ```
6. Run the dbt models:
   ```
   cd transit_transform
   dbt run
   dbt test
   ```

## Tech stack

Python, NS Transit API, Google BigQuery (Sandbox), dbt, Docker · Planned: Airflow, GitHub Actions, dashboard (Looker Studio)

## Status

- [x] Ingestion (scheduled, retries, structured logging)
- [x] Raw storage
- [x] Idempotent loading into BigQuery
- [x] dbt staging model (tested)
- [x] dbt marts model
- [x] Docker (ingestion script)
- [ ] Airflow orchestration
- [ ] CI/CD (GitHub Actions)
- [ ] Dashboard