from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime

import requests
import logging
import os
import pandas as pd


logger = logging.getLogger(__name__)

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    default_args={"owner": "Astro", "retries": 3},
    tags=["Pernambuco"],
    catchup=False,
)
def weather_etl():

    @task.python
    def extract_weather_data():
        """
        Extracts weather data from the WeatherAPI for a specific location
        and returns the JSON response.
        """
                    
        weather_api_key = Variable.get("api_key")
        base_url = "http://api.weatherapi.com/v1"
        complete_url = f"{base_url}/forecast.json"
        params = {
            "key": weather_api_key,
            "q": "Recife",
            "days": 1,
            "alerts": "no",
            "aqi": "no",
            "pollen": "no",
            "tides": "no",
        }
        r = requests.get(complete_url, params=params, timeout=10)

        if r.status_code == 200:
            return r.json()
        else:
            logger.error(f"Error fetching weather data: {r.status_code}")
            return None

    @task.python
    def transform_weather_data(weather_data: dict = None):
        """
        Transforms the weather data into a pandas DataFrame.
        """

        if weather_data is None:
            logger.warning("No data to transform.")
            return []

        location = weather_data.get("location", {})
        current = weather_data.get("current", {})
        forecast = weather_data.get("forecast", {}).get("forecastday", [])
        day_data = forecast[0].get("day", {})
        transformed_data = {
            "name": location.get("name"),
            "region": location.get("region"),
            "country": location.get("country"),
            "tz_id": location.get("tz_id"),
            "localtime": location.get("localtime"),
            "date": forecast[0].get("date"),
            "last_updated": current.get("last_updated"),
            "condition": day_data.get("condition", {}).get("text"),
            "maxtemp_c": day_data.get("maxtemp_c"),
            "mintemp_c": day_data.get("mintemp_c"),
            "avgtemp_c": day_data.get("avgtemp_c"),
            "chance_of_rain": day_data.get("daily_chance_of_rain"),
        }
        return [transformed_data]

    @task.python
    def load_weather_data(data: list = None, **context):
        """
        Loads the transformed weather data into a CSV file.
        """
        if not data:
            raise ValueError("No data to load.")

        df = pd.DataFrame(data)

        try:
            output_dir = "weather_files"
            os.makedirs(output_dir, exist_ok=True)
            file_date = context['logical_date'].strftime("%Y%m%d")
            file_name = f"weather_data_{file_date}.csv"
            output_path = os.path.join(output_dir, file_name)
            df.to_csv(output_path, index=False)
            logger.info(f"Weather data loaded successfully to {output_path}")
        except Exception as e:
            logger.error(f"Error loading weather data: {e}")
            raise

    # Define task dependencies with data passing
    extract_output = extract_weather_data()
    transform_output = transform_weather_data(extract_output)
    load_weather_data(transform_output)

# Instantiate the DAG
weather_etl()
