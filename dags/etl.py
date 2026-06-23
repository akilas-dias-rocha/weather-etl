import logging
import os
import pandas as pd
from dotenv import load_dotenv
from airflow.sdk import dag, task
from pendulum import datetime, now

logger = logging.getLogger(__name__)

load_dotenv()

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    default_args={"owner": "Astro", "retries": 3},
    tags=["Pernambuco"],
)
def weather_etl():

    @task.python
    def extract_weather_data():
        """
        Extracts weather data from the WeatherAPI for a specific location
        and returns the JSON response.
        """
        import requests
        try:
            weather_api_key = os.getenv("api_key")
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
            r = requests.get(complete_url, params=params)
            r.raise_for_status()
            weather_data = r.json()
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            weather_data = None
        return weather_data

    @task.python
    def transform_weather_data(weather_data: dict = None):
        """
        Transforms the weather data into a pandas DataFrame.
        """
        
        if weather_data is None:
            logger.warning("No data to transform.")
            return

        try:
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
            df = pd.DataFrame([transformed_data])
            return df
        except Exception as e:
            logger.error(f"Error transforming weather data: {e}")
            return pd.DataFrame()

    @task.python
    def load_weather_data(df: pd.DataFrame = None):
        """
        Loads the transformed weather data into a CSV file.
        """
        if df is None or df.empty:
            logger.warning("No data to load.")
            return

        try:
            output_dir = "weather_files"
            os.makedirs(output_dir, exist_ok=True)
            file_date = now().strftime("%Y%m%d")
            file_name = f"weather_data_{file_date}.csv"
            output_path = os.path.join(output_dir, file_name)
            df.to_csv(output_path, index=False)
            logger.info(f"Weather data loaded successfully to {output_path}")
        except Exception as e:
            logger.error(f"Error loading weather data: {e}")

    # Define task dependencies with data passing
    extract_output = extract_weather_data()
    transform_output = transform_weather_data(extract_output)
    load_weather_data(transform_output)

# Instantiate the DAG
weather_etl()
