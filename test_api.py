import os 
import requests 
from dotenv import load_dotenv 

load_dotenv()
api_key = os.getenv("NS_API_KEY")


url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures"

headers = {"Ocp-Apim-Subscription-Key": api_key}

params = {"station": "asd"}

response = requests.get(url, headers=headers, params=params)

print("Status code :", response.status_code)

if response.status_code == 200: 
	data = response.json()

	departures = data["payload"]["departures"]
	
	print(f"\nFound {len(departures)} departures:\n")
	
	for d in departures[:5]:
		print(f"{d['direction']:20} — planned {d['plannedDateTime']}")

else: 
	print("Error:", response.text)

	
