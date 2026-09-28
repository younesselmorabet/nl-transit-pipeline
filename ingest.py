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
api_key = os.getenv("NS_API_KEY")
# Why: keeps the actual key out of the code itself — safe to push this file to GitHub.

# ── Logging setup ──────────────────────────────────────────
# Writes to BOTH the console and a file (pipeline.log), with timestamps
# and severity levels (INFO/WARNING/ERROR) — this is what a real system
# uses so you can look back later at exactly what happened, and when.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_departures(max_retries=3):
    """Pulls departures from NS API once, retrying on failure, and saves the raw response.

    Returns True if a file was saved, False if every attempt failed.
    """
    url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"station": "asd"}  # "asd" = Amsterdam Centraal's NS station code

    # ── Retry loop ───────────────────────────────────────────
    # Try up to max_retries times before giving up entirely on this run.
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                os.makedirs("data/raw", exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data/raw/departures_asd_{timestamp}.json"

                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

                logger.info(f"Saved to {filename}")
                return True  # success — exit the function immediately, no need to retry

            else:
                logger.warning(f"Attempt {attempt}: status {response.status_code} — {response.text}")

        except requests.exceptions.RequestException as e:
            # Catches network-level failures: timeouts, connection drops, DNS issues, etc.
            logger.warning(f"Attempt {attempt}: request failed — {e}")

        if attempt < max_retries:
            wait = attempt * 5  # backoff: wait longer each retry (5s, then 10s)
            logger.info(f"Retrying in {wait} seconds...")
            time.sleep(wait)

    # If every attempt failed, log it and tell the caller it failed.
    logger.error("All retry attempts failed.")
    return False


# ── Entry point ────────────────────────────────────────────
# Runs only when the file is executed directly (python ingest.py),
# not when it is imported by another file.
# The script now does ONE pull and exits. The "every 5 minutes" part
# is no longer this file's job — the scheduler (Airflow) owns it.
if __name__ == "__main__":
    logger.info("Ingestion run started.")
    success = fetch_departures()

    # Exit code is how a program tells its caller whether it worked:
    # 0 = success, anything else = failure. Airflow reads this to mark
    # the task as success or failed, and to decide whether to retry.
    sys.exit(0 if success else 1)