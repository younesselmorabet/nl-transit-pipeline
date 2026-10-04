# NL Transit Data Pipeline

An end-to-end data pipeline that ingests live Dutch train departure data from the NS (Nederlandse Spoorwegen) API, stores it, loads it into a cloud warehouse, transforms it into clean, tested, business-ready tables using dbt, and orchestrates the whole thing on a schedule with Apache Airflow. Built as a data engineering portfolio project targeting roles in the Netherlands and Ireland.

## Architecture

This project follows an ELT pattern (Extract, Load, Transform) with a medallion architecture (raw → staging → marts) inside the warehouse, orchestrated by two Airflow DAGs running on different schedules.

```
NS Transit API
      |
      v
DAG 1: nl_transit_ingest (every 5 minutes)   <- Extract
  - single pull per run
  - retries transient failures (network, 5xx)
  - falls back to a secondary API key on auth failure (401/403)
  - logs to console + file
      |
      v
Raw JSON storage (local disk, via Docker bind mount)
  - one timestamped file per pull
      |
      v
DAG 2: nl_transit_transform (hourly)
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

**Two DAGs on two schedules, not one.** The pipeline started as a single DAG running all four steps every 5 minutes. That meant dbt rebuilt its models and re-ran all tests every 5 minutes too, which burns BigQuery Sandbox's usage quota for no real benefit — nothing consumes the mart table that often. It was split into `nl_transit_ingest` (every 5 minutes, keeps raw data fresh) and `nl_transit_transform` (hourly: load → dbt run → dbt test), each scheduled independently.

**Airflow orchestration via Docker Compose, with a custom image.** Airflow runs locally as a 7-container stack (API server, scheduler, worker, dag-processor, triggerer, Postgres metadata DB, Redis broker) via the official Celery-based `docker-compose.yaml`. Since dbt and the BigQuery client aren't in Airflow's base image, a custom image (`Dockerfile.airflow`) extends it. Installing `dbt-bigquery` hit a real dependency conflict: it requires `google-cloud-storage<3.2`, while Airflow's own constraints file (used to keep Airflow's core dependencies stable) pins `google-cloud-storage==3.13.1` for a Google provider package this project doesn't use. The fix: a two-step install — Airflow-related packages installed under the constraints file, `dbt-bigquery` installed separately, outside it, so pip can resolve its own dependency tree without a forced, contradictory pin.

**Service account authentication, not personal login.** The load script and dbt authenticate inside containers using a GCP service account key (scoped to `BigQuery Data Editor` + `BigQuery Job User`, least-privilege), mounted read-only from a gitignored `secrets/` folder. Containers can't open a browser for an interactive `gcloud` login, so a personal account login (used only for one-off manual queries outside the pipeline) wasn't an option for anything automated.

**NS API key stored as an Airflow Variable, not in a shared `.env`.** Airflow's Docker Compose setup loads one `env_file` into every container by default. Rather than exposing the NS API key to all 7 containers (including ones that never call the NS API), it's stored as an Airflow Variable (encrypted at rest via `FERNET_KEY`) and read only inside the ingest task. A separate `.env.airflow` holds only the two values Compose itself needs (`AIRFLOW_UID`, `FERNET_KEY`).

**Docker for the ingestion script, with a bind-mounted data folder.** The ingestion script is containerized so it runs identically on any machine, without depending on a local Python version or virtual environment. The API key is never baked into the image; it is passed at runtime via `--env-file` (standalone) or an Airflow Variable (orchestrated). A container's filesystem is isolated and disposable, so the local `data/` folder is bind-mounted in. In a production setup, raw files would go to object storage (GCS or S3) instead; that was ruled out here to avoid needing a billing account.

**Single-pull ingestion, scheduler-agnostic.** `ingest.py`'s `fetch_departures()` does one pull per run and exits with a status code (0 = success, 1 = failure) instead of looping internally — Airflow's scheduler owns the "every 5 minutes" part. The function also accepts the API keys as optional arguments (falling back to `.env` when run standalone), so the exact same function is called both by `python ingest.py` and by the Airflow DAG, with no duplicated logic.

**Idempotent loading.** The load script tracks which raw files have already been loaded by moving them to `data/loaded/` on success. Re-running the script can never duplicate data, and a failed load leaves files in place to retry safely.

**Retry logic distinguishes transient from permanent failures.** A `401`/`403` means the API key itself is rejected — retrying with the same key can't fix that, so the script stops immediately and falls back to a secondary key instead. A `5xx` or network error is treated as transient and retried with backoff. Task-level retries (2, via Airflow's `default_args`) sit on top of this as a second safety net.

**Staging and marts, no intermediate/dimension layers.** The dbt project uses only `stg_` and `mart_` models. Intermediate (`int_`) models exist to share logic across multiple marts, and dimension (`dim_`) tables exist for separate reference entities (e.g. station metadata) — neither applies at this project's current scale, so they were deliberately left out rather than added for their own sake.

## Project structure

```
nl-transit-pipeline/
├── .env                          # NS API key, for standalone/local runs (not committed)
├── .env.airflow                  # AIRFLOW_UID, FERNET_KEY (not committed)
├── .gitignore
├── README.md
├── Dockerfile                    # Ingestion script image
├── Dockerfile.airflow            # Custom Airflow image (dbt + BigQuery libraries)
├── docker-compose.yaml           # Full Airflow stack (7 services)
├── requirements.txt              # Ingestion script's dependencies
├── requirements-airflow.txt      # Extra packages installed into the Airflow image
├── ingest.py                     # Extract: one pull from NS API, saves raw JSON
├── load_to_bigquery.py           # Load: batch loads raw JSON into BigQuery
├── pipeline.log                  # generated log output (not committed)
├── secrets/                      # GCP service account key, read-only mount (not committed)
├── dbt_profiles/
│   └── profiles.yml              # dbt's BigQuery connection config (no secrets in it)
├── dags/
│   ├── nl_transit_ingest_dag.py     # DAG 1: ingest, every 5 minutes
│   └── nl_transit_transform_dag.py  # DAG 2: load -> dbt run -> dbt test, hourly
├── data/                          # not committed (gitignored)
│   ├── raw/                       # files waiting to be loaded
│   └── loaded/                    # files already loaded (idempotency record)
├── venv/                          # Python virtual environment (not committed)
└── transit_transform/             # dbt project: Transform
    ├── dbt_project.yml
    └── models/
        ├── sources.yml            # declares departures_raw as a source
        ├── stg_departures.sql     # staging model
        ├── mart_delays_by_destination.sql   # marts model
        └── schema.yml             # model documentation + tests
```

## Known limitations

- Single station (Amsterdam Centraal) — scope kept small and demonstrable; the pipeline logic generalizes to any station.
- BigQuery Sandbox data expires after 60 days without a billing account.
- No true real-time streaming — data is pulled in discrete batches, not event-driven.
- `departures_raw` only ever appends (`WRITE_APPEND`) — no deduplication or retention policy. Fine at this data volume; a real production version would need one.
- **dbt shares the Airflow worker's image rather than running in an isolated container.** A standalone, fully isolated dbt image (separate Dockerfile, no shared dependencies with Airflow at all) was built and tested successfully outside the DAG. Wiring it in as an actual task would need either mounting the Docker socket into the worker (`DockerOperator`) — which grants that container broad control over the whole Docker host, a real security trade-off, not just a config change — or a Kubernetes-based setup (`KubernetesPodOperator`), which is the production-grade answer but a substantial addition of new infrastructure. Both were deliberately deferred in favor of the current approach: install Airflow-safe packages under its constraints file, and `dbt-bigquery` separately outside it (see "Why these choices" above). This keeps a known, narrow risk — a future image rebuild could reintroduce a dependency conflict — rather than a broader one.
- Secrets (API keys, the GCP service account key) are stored as Airflow Variables and a gitignored file, not a dedicated secrets manager (Google Secret Manager, Vault). Reasonable for a solo local project; a team setting would want per-secret access control and audit logging, which Airflow Variables don't provide on their own.

## Setup

1. Clone the repo and create a virtual environment:
   ```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Create a `.env` file with your NS API key(s):
   ```
   NS_API_KEY=your_primary_key_here
   NS_API_KEY_SECONDARY=your_secondary_key_here
   ```
3. Authenticate with Google Cloud (for local/manual runs only — the orchestrated pipeline uses a service account key instead):
   ```
   gcloud auth application-default login
   ```
4. Run the ingestion script standalone, either locally or in Docker:
   ```
   python ingest.py
   ```
   ```
   docker build -t nl-transit-ingest .
   docker run --env-file .env -v "${PWD}/data:/app/data" nl-transit-ingest
   ```
5. To run the full orchestrated pipeline via Airflow:
   - Create a GCP service account (roles: `BigQuery Data Editor`, `BigQuery Job User`), download its JSON key into `secrets/gcp-key.json`.
   - Create `.env.airflow` with `AIRFLOW_UID=50000` and a generated `FERNET_KEY`.
   - Build and start the stack:
     ```
     docker compose --env-file .env.airflow build
     docker compose --env-file .env.airflow up -d
     ```
   - In the Airflow UI (`localhost:8080`, default login `airflow`/`airflow`), add `ns_api_key` and `ns_api_key_secondary` under Admin → Variables.
   - Unpause both DAGs. `nl_transit_ingest` runs every 5 minutes; `nl_transit_transform` runs hourly.

## Tech stack

Python, NS Transit API, Google BigQuery (Sandbox), dbt, Docker, Docker Compose, Apache Airflow (CeleryExecutor) · Planned: GitHub Actions, dashboard (Looker Studio)

## Status

- [x] Ingestion (single-pull, retries, secondary-key fallback, exit codes, structured logging)
- [x] Raw storage
- [x] Idempotent loading into BigQuery
- [x] dbt staging model (tested)
- [x] dbt marts model
- [x] Docker (ingestion script)
- [x] Airflow orchestration (Docker Compose, custom image, two DAGs: ingest + transform)
- [ ] CI/CD (GitHub Actions)
- [ ] Dashboard