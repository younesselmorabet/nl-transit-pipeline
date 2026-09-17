# ── Import the tools we need ──────────────────────────────
import os                        # environment variables, folder creation
import json                      # convert Python data <-> JSON, write to files
import time                      # pause the program between checks/retries
import logging                   # professional-grade logging instead of print()
import requests                  # call the NS API over the internet
import schedule                  # lets us say "run this every N minutes" simply
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
    """Pulls departures from NS API, retrying on failure, and saves the raw response."""
    url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"station": "asd"}  # "asd" = Amsterdam Centraal's NS station code

    # ── Retry loop ───────────────────────────────────────────
    # Try up to max_retries times before giving up entirely on this cycle.
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
                return  # success — exit the function immediately, no need to retry

            else:
                logger.warning(f"Attempt {attempt}: status {response.status_code} — {response.text}")

        except requests.exceptions.RequestException as e:
            # Catches network-level failures: timeouts, connection drops, DNS issues, etc.
            logger.warning(f"Attempt {attempt}: request failed — {e}")

        if attempt < max_retries:
            wait = attempt * 5  # backoff: wait longer each retry (5s, then 10s)
            logger.info(f"Retrying in {wait} seconds...")
            time.sleep(wait)

    # If every attempt failed, log it clearly and move on — don't crash the whole pipeline.
    logger.error("All retry attempts failed. Skipping this cycle.")


# ── Scheduling ─────────────────────────────────────────────
schedule.every(5).minutes.do(fetch_departures)
# Registers the job: "every 5 minutes, call fetch_departures" — doesn't run it yet.

logger.info("Pipeline started. Fetching every 5 minutes. Press Ctrl+C to stop.")
fetch_departures()  # run once immediately so we're not waiting 5 minutes for first output

while True:
    schedule.run_pending()   # checks: "has 5 minutes passed? if so, run the job"
    time.sleep(30)           # wait 30s before checking again, to avoid wasting CPU