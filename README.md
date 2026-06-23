# Weather ETL Pipeline

An Apache Airflow ETL pipeline that extracts weather forecast data from the [WeatherAPI](https://www.weatherapi.com/), transforms it into a structured format, and loads it as CSV files.

## Project Structure

```
weather-etl/
├── .astro/                  # Astronomer CLI configuration
├── dags/
│   └── etl.py              # Main DAG definition
├── include/                 # Additional files
├── plugins/                 # Custom Airflow plugins
├── tests/                   # Unit tests
├── weather_files/           # Output directory for CSV files
├── .env                     # Environment variables (API key)
├── .gitignore
├── airflow_settings.yaml    # Airflow local settings
├── Dockerfile               # Docker image definition
├── packages.txt             # System-level dependencies
└── requirements.txt         # Python dependencies
```

## Features

- **Extract**: Fetches weather forecast data from WeatherAPI for Recife, Brazil
- **Transform**: Parses the JSON response and extracts 12 key fields into a pandas DataFrame
- **Load**: Saves the transformed data as a dated CSV file (`weather_data_YYYYMMDD.csv`)

### Extracted Fields

| Field | Description |
|-------|-------------|
| `name` | Location name |
| `region` | Region or state |
| `country` | Country |
| `tz_id` | Timezone identifier |
| `localtime` | Local date and time |
| `date` | Forecast date |
| `last_updated` | Last update timestamp |
| `condition` | Weather condition text |
| `maxtemp_c` | Maximum temperature (°C) |
| `mintemp_c` | Minimum temperature (°C) |
| `avgtemp_c` | Average temperature (°C) |
| `chance_of_rain` | Daily chance of rain (%) |

## Prerequisites

- [Docker](https://www.docker.com/) installed
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed
- A free API key from [WeatherAPI](https://www.weatherapi.com/)

## Installation & Setup

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd weather-etl
   ```

2. **Set up the API key:**

   Create a `.env` file in the project root:

   ```
   api_key=YOUR_WEATHERAPI_KEY
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Start the Airflow environment:**

   ```bash
   astro dev start
   ```

5. **Access the Airflow UI:**

   Open [http://localhost:8080](http://localhost:8080) in your browser.

## DAG Schedule

The DAG runs **daily at 6:00 AM** (cron: `0 6 * * *`).

## Workflow

```
extract_weather_data() >> transform_weather_data() >> load_weather_data()
```

1. **extract_weather_data**: Makes an HTTP GET request to the WeatherAPI forecast endpoint
2. **transform_weather_data**: Parses the JSON response and extracts the relevant fields into a DataFrame
3. **load_weather_data**: Creates the `weather_files/` directory (if needed) and saves the DataFrame as a CSV file

## Output

CSV files are saved in the `weather_files/` directory with the naming convention:

```
weather_files/
├── weather_data_20260622.csv
├── weather_data_20260623.csv
└── ...
```

## Error Handling

Each task includes try-except blocks to handle:

- **Extract**: HTTP errors, connection failures, timeouts
- **Transform**: Missing or malformed data
- **Load**: File system errors

## Dependencies

| Package | Version |
|---------|---------|
| pandas | 3.0.3 |
| python-dotenv | 1.2.2 |
| requests | 2.34.2 |
