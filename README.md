\# NL Transit Data Pipeline



An end-to-end data pipeline that ingests live Dutch train departure data (NS API),

stores it, loads it into a cloud warehouse, and models it for analysis — built as

a portfolio project targeting data engineering roles in the Netherlands and Ireland.



\## Architecture



NS Transit API → Ingestion script (Python, scheduled) → Raw JSON storage (local)

→ Load script (batch, idempotent) → BigQuery (raw table) → dbt models → \[dashboard]



\## Why these choices



\- \*\*BigQuery Sandbox, not paid cloud storage\*\*: built without a billing account,

&#x20; using BigQuery's free sandbox tier. This meant skipping a separate cloud storage

&#x20; (GCS) staging layer — raw files are loaded directly from local disk into BigQuery

&#x20; instead. A deliberate trade-off for a zero-cost setup, not an oversight.

\- \*\*Batch loading, not streaming inserts\*\*: BigQuery Sandbox blocks streaming

&#x20; inserts. Batch loading is used instead, which is also the more standard pattern

&#x20; for a pipeline that runs on a fixed schedule rather than true real-time data.

\- \*\*A simple Python scheduler, not Airflow (yet)\*\*: automation is handled with the

&#x20; `schedule` library rather than Airflow, to prove the pipeline logic end-to-end

&#x20; first before adding orchestration overhead. Migrating scheduling to Airflow is

&#x20; a planned next step.

\- \*\*Idempotent loading\*\*: the load script tracks which raw files have already been

&#x20; loaded (moving them to `data/loaded/` on success) so re-running it never

&#x20; duplicates data — a deliberate fix after an early version didn't handle this.

\- \*\*Retry logic + structured logging\*\*: the ingestion script retries failed API

&#x20; calls with backoff, and logs to both console and file with timestamps/severity,

&#x20; rather than silently failing or using plain print statements.



\## Known limitations



\- Single station (Amsterdam Centraal) — scope kept small and demonstrable rather

&#x20; than broad; the pipeline logic generalizes to any station.

\- BigQuery Sandbox data expires after 60 days without a billing account attached.

\- No true real-time streaming — data is pulled every 5 minutes, not event-driven.



\## Setup



\[instructions to come]



\## Tech stack



Python, NS Transit API, Google BigQuery, dbt, \[Docker — planned]

