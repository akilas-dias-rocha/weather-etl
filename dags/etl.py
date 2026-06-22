import os
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

weather_api_key = os.getenv("api_key")
base_url = "http://api.weatherapi.com/v1"
complete_url = f"{base_url}/forecast.json"

params = {
    "key" : weather_api_key,
    "q" : "Recife",
    "days" : 1,
    "alerts" : "no",
    "aqi" : "no",
    "pollen" : "no",
    "tides" : "no",
}

try:
    r = requests.get(complete_url, params = params)
    r.raise_for_status()
    weather_data = r.json()
    print(weather_data)
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
except requests.exceptions.ConnectionError as e:
    print(f"Connection Error: {e}")
except requests.exceptions.Timeout as e:
    print(f"Timeout Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"Request Error: {e}")
