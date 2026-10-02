# ── Import the tools we need ──────────────────────────────
import os                        # environment variables, folder creation
import sys                       # lets us exit with a status code (0 = success, 1 = failure)
import json                      # convert Python data <-> JSON, write to files
import time                      # pause the program between retries
import logging                   # professional-grade logging instead of print()
import requests                  # call the NS API over the internet
from datetime import datetime    # get current date/time, for timestamps
from dotenv import load_dotenv   # read our .env file

load_dotenv()
DEFAULT_PRIMARY_KEY = os.getenv("NS_API_KEY")
DEFAULT_SECONDARY_KEY = os.getenv("NS_API_KEY_SECONDARY")
# Why: keeps the actual keys out of the code itself — safe to push this file to GitHub.
# These are only DEFAULTS now — fetch_departures() below accepts keys as
# arguments, so a caller (like the Airflow DAG) can pass in keys from
# somewhere else (an Airflow Variable) instead of relying on a .env file
# that doesn't exist inside the Airflow containers.

# ── Logging setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

URL = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
PARAMS = {"station": "asd"}  # "asd" = Amsterdam Centraal's NS station code


def _try_key(api_key, key_label, max_retries=3):
    """Attempts the pull with ONE specific key, retrying only on transient
    failures (network errors, 5xx). A 401/403 means the key itself is bad,
    so it returns immediately instead of wasting retries.

    Returns "saved" if a file was written, "auth_failed" if the key was
    rejected, or "exhausted" if retries ran out on a transient failure.
    """
    headers = {"Ocp-Apim-Subscription-Key": api_key}

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(URL, headers=headers, params=PARAMS, timeout=10)

            if response.status_code == 200:
                data = response.json()
                os.makedirs("data/raw", exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data/raw/departures_asd_{timestamp}.json"

                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                logger.info(f"Saved to {filename} (using {key_label} key)")
                return "saved"

            elif response.status_code in (401, 403):
                # The key itself is invalid/expired/revoked. Retrying with
                # the SAME key can't fix that — stop immediately and let
                # the caller decide whether to try a different key.
                logger.warning(
                    f"{key_label} key rejected (status {response.status_code}) — "
                    f"not retrying this key."
                )
                return "auth_failed"

            else:
                # Any other status (5xx, etc.) is treated as transient —
                # worth retrying, since it's likely a problem on NS's side.
                logger.warning(f"Attempt {attempt} ({key_label}): status {response.status_code} — {response.text}")

        except requests.exceptions.RequestException as e:
            # Network-level failures: timeouts, connection drops, DNS issues.
            # Also transient — worth retrying.
            logger.warning(f"Attempt {attempt} ({key_label}): request failed — {e}")

        if attempt < max_retries:
            wait = attempt * 5  # backoff: wait longer each retry (5s, then 10s)
            logger.info(f"Retrying in {wait} seconds...")
            time.sleep(wait)

    logger.error(f"All retry attempts exhausted on {key_label} key (transient failures).")
    return "exhausted"


def fetch_departures(max_retries=3, primary_key=None, secondary_key=None):
    """Pulls departures from NS API once, using the primary key first.
    Falls back to the secondary key ONLY if the primary key itself is
    rejected (401/403) — never as a first choice, and never for
    transient failures.

    primary_key / secondary_key: pass these explicitly to use specific
    keys (e.g. from an Airflow Variable). If left as None, falls back to
    whatever was loaded from .env at import time — this is what keeps
    `python ingest.py` working standalone with no changes needed.

    Returns True if a file was saved, False if every option failed.
    """
    primary_key = primary_key if primary_key is not None else DEFAULT_PRIMARY_KEY
    secondary_key = secondary_key if secondary_key is not None else DEFAULT_SECONDARY_KEY

    result = _try_key(primary_key, "primary", max_retries=max_retries)

    if result == "saved":
        return True

    if result == "auth_failed" and secondary_key:
        logger.info("Falling back to secondary key.")
        result = _try_key(secondary_key, "secondary", max_retries=max_retries)
        if result == "saved":
            return True

    logger.error("All retry attempts failed.")
    return False


# ── Entry point ────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Ingestion run started.")
    success = fetch_departures()
    sys.exit(0 if success else 1)