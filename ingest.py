import os
import json
import time
import logging
import requests
import schedule
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("NS_API_KEY")

# ── Logging setup ──────────────────────────────────────────
# Instead of print(), we log to both the console AND a file, with
# timestamps and severity levels (INFO, WARNING, ERROR). This is what
# a real system uses so you can look back at what happened, when.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()  # still prints to the terminal too
    ]
)
logger = logging.getLogger(__name__)


def fetch_departures(max_retries=3):
    """Pulls departures from NS API, retrying on failure, and saves the raw response."""
    url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"station": "asd"}

    # ── Retry loop ───────────────────────────────────────────
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
                return  # success — exit the function, no need to retry

            else:
                logger.warning(f"Attempt {attempt}: status {response.status_code} — {response.text}")

        except requests.exceptions.RequestException as e:
            # Catches network errors: timeouts, connection failures, etc.
            logger.warning(f"Attempt {attempt}: request failed — {e}")

        if attempt < max_retries:
            wait = attempt * 5  # wait longer each retry: 5s, then 10s
            logger.info(f"Retrying in {wait} seconds...")
            time.sleep(wait)

    # If we reach here, every attempt failed
    logger.error("All retry attempts failed. Skipping this cycle.")


schedule.every(5).minutes.do(fetch_departures)

logger.info("Pipeline started. Fetching every 5 minutes. Press Ctrl+C to stop.")
fetch_departures()

while True:
    schedule.run_pending()
    time.sleep(30)