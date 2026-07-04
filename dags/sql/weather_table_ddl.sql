CREATE TABLE IF NOT EXISTS weather_data (
    name VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    tz_id VARCHAR(100),
    "localtime" TIMESTAMP,
    "date" DATE,
    last_updated TIMESTAMP,
    "condition" VARCHAR(100),
    maxtemp_c FLOAT,
    mintemp_c FLOAT,
    avgtemp_c FLOAT,
    chance_of_rain FLOAT,
    UNIQUE (name, "date")
);
