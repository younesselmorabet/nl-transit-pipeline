# ── Import the tools we need ──────────────────────────────
import os                        # environment variables, folder creation
import json                      # convert Python data <-> JSON, write to files
import requests                  # call the NS API over the internet
import schedule                  # lets us say "run this every N minutes" simply
import time                      # lets us pause the program between checks
from datetime import datetime    # get current date/time, for timestamps
from dotenv import load_dotenv   # read our .env file

# ── Load your secret key ──────────────────────────────────
load_dotenv()
api_key = os.getenv("NS_API_KEY")
# Why: keeps the key out of the code itself, safe to push to GitHub.

# ── Wrap the whole "fetch and save" process in a function ─
def fetch_departures():
    """Pulls departures from NS API and saves the raw response."""
    # ^ this text in triple quotes is a "docstring" — a description of what
    # the function does. Doesn't affect how it runs, purely documentation.

    # Why put this in a function at all?
    # A function is a named, reusable block of code. Before, this logic ran
    # once, top to bottom, then the script ended. Now we can CALL this
    # function as many times as we want — which is exactly what we need
    # for something that runs on a repeating schedule.

    url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"station": "asd"}
    # ^ same as before: the address, our "ID badge", and our actual question

    response = requests.get(url, headers=headers, params=params)
    # ^ makes the actual request to NS's server

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Status code: {response.status_code}")
    # ^ prints the current time (hour:minute:second) alongside the status code,
    # so when you look back at the output, you know WHEN each pull happened.
    # .strftime('%H:%M:%S') formats the current time as e.g. "16:45:02"

    if response.status_code == 200:
        data = response.json()
        # ^ convert the raw response into a Python dictionary

        os.makedirs("data/raw", exist_ok=True)
        # ^ make sure the folder exists (no error if it already does)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/departures_asd_{timestamp}.json"
        # ^ build a unique filename using the current date+time,
        # so each pull gets saved as its OWN file, nothing gets overwritten

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            # ^ write the data to that file, formatted to be human-readable

        print(f"Saved to {filename}")
    else:
        print("Error:", response.text)
        # ^ if something went wrong, print NS's error message instead of crashing

# ── Scheduling ─────────────────────────────────────────────
schedule.every(5).minutes.do(fetch_departures)
# ^ this line REGISTERS a job with the schedule library:
# "every 5 minutes, call the function fetch_departures"
# Important: this line does NOT run the function itself — it just sets up
# the rule. The actual running happens in the loop below.

print("Pipeline started. Fetching every 5 minutes. Press Ctrl+C to stop.")

fetch_departures()
# ^ we call it once manually, right now, immediately —
# otherwise we'd have to wait a full 5 minutes before seeing ANY output,
# which would make it seem like nothing is happening.

while True:
    # ^ "while True" means: repeat this block FOREVER, until something
    # stops it manually (like you pressing Ctrl+C in the terminal).

    schedule.run_pending()
    # ^ this checks: "has 5 minutes passed since the job last ran?"
    # If yes, it runs fetch_departures() again. If no, it does nothing this time.

    time.sleep(30)
    # ^ pause for 30 seconds before looping back and checking again.
    # Why not check constantly with no pause? Because that would run this
    # check thousands of times per second for no reason, wasting CPU.
    # Checking every 30 seconds is more than often enough to catch
    # a "5 minutes have passed" moment accurately.s