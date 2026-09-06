# ── Import the tools we need ──────────────────────────────
import os                        # read environment variables (API key) and manage folders/paths
import json                      # convert Python data <-> JSON text, and write it to files
import requests                  # send/receive data over the internet (call APIs)
from datetime import datetime    # get the current date/time, to timestamp our saved files
from dotenv import load_dotenv   # read our .env file

# ── Load your secret key ──────────────────────────────────
load_dotenv()                     # loads .env contents into the environment
api_key = os.getenv("NS_API_KEY") # grabs NS_API_KEY specifically from it
# Why: keeps the actual key out of the code itself — safe to push this file to GitHub.

# ── Set up what we're asking for ──────────────────────────
url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"
# ^ the exact address of NS's "departures" endpoint

headers = {"Ocp-Apim-Subscription-Key": api_key}
# ^ "headers" = extra info sent with the request, not the question itself — like showing an ID badge.
# NS specifically requires the key under this exact name (from their docs).

params = {"station": "asd"}
# ^ "params" = the actual question: "give me departures for station asd" (Amsterdam Centraal)

# ── Make the actual request ───────────────────────────────
response = requests.get(url, headers=headers, params=params)
# ^ sends the request to NS's server: GET = "I want to receive data"
# NS replies, and that reply is stored in "response"

print("Status code:", response.status_code)
# ^ 200 = success | 401/403 = bad key | 404 = wrong URL/station | 429 = too many requests | 5xx = their server's problem

# ── If it worked, save the raw data ───────────────────────
if response.status_code == 200:
    data = response.json()
    # ^ converts the raw text response into a Python dictionary we can work with

    # Create the folder "data/raw" if it doesn't exist yet.
    # exist_ok=True means: don't throw an error if the folder is already there.
    os.makedirs("data/raw", exist_ok=True)
    # Why "data/raw" specifically: real pipelines separate RAW (untouched) data
    # from any cleaned/processed data, which will live in a different folder later.

    # Build a unique filename using the current date and time.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # ^ turns "right now" into a clean string like 20260906_161700
    # (Year-Month-Day_Hour-Minute-Second — no spaces or slashes, safe for filenames)

    filename = f"data/raw/departures_asd_{timestamp}.json"
    # ^ e.g. "data/raw/departures_asd_20260906_161700.json"
    # Why a timestamp: every time we run this script, it saves a NEW file
    # instead of overwriting the last one — so we keep a full history of pulls.

    # Open a new file in write mode ("w") and save the data into it as JSON.
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        # ^ json.dump() writes the Python dictionary "data" into the file "f",
        # formatted as JSON text. indent=2 makes it readable if you open it yourself
        # (adds line breaks and spacing instead of one giant unreadable line).

    print(f"Saved raw data to {filename}")
    # ^ confirms to us where the file landed

else:
    # If the request failed, print NS's error message instead of crashing
    print("Error:", response.text)