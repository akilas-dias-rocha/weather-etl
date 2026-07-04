from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

import requests


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    default_args={"owner": "Astro", "retries": 2},
    tags=["Pernambuco"],
    catchup=False,
)
def weather_etl():

    @task(retries=3, retry_delay=10)
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

        if r.status_code != 200:
            raise ValueError(f"Error fetching weather data: {r.status_code}")

        else:
            return r.json()

    @task()
    def transform_weather_data(weather_data: dict = None):
        """
        Transforms the weather data into a pandas DataFrame.
        """
        if weather_data is None:
            print("No data to transform.")
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

    create_weather_table = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="postgres_default",
        sql="sql/weather_table_ddl.sql",
    )

    @task()
    def load_weather_data(data: list = None):
        """
        Loads the transformed weather data into a Postgres database.
        """
        if not data:
            raise ValueError("No data to load.")

        table_name = "weather_data"
        target_fields = [
            "name",
            "region",
            "country",
            "tz_id",
            "localtime",
            "date",
            "last_updated",
            "condition",
            "maxtemp_c",
            "mintemp_c",
            "avgtemp_c",
            "chance_of_rain",
        ]

        reserved = {"localtime", "date", "condition"}
        quoted_columns = [f'"{col}"' if col in reserved else col for col in target_fields]

        rows = [[row[col] for col in target_fields] for row in data]

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        postgres_hook.insert_rows(
            table=table_name,
            rows=rows,
            replace=True,
            replace_index=["name", "date"],
            target_fields=quoted_columns,
        )
        print(f"Loaded {len(rows)} rows into {table_name}.")

    extract_output = extract_weather_data()
    transform_output = transform_weather_data(extract_output)
    create_weather_table >> load_weather_data(transform_output)


weather_etl()
