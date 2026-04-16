from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date
import requests
import time

# --------------------------------
# نفس الأماكن
# --------------------------------
villages = [
    {"name": "West Nubaria",   "lat": 30.86, "lon": 30.55},
    {"name": "Wadi El Natrun", "lat": 30.42, "lon": 30.3},
    {"name": "El-Bostan",      "lat": 30.95, "lon": 30.38},
    {"name": "Edko",           "lat": 31.3,  "lon": 30.3},
]

def fetch_forecast_weather():
    pg_hook = PostgresHook(postgres_conn_id="postgres_airflow")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for village in villages:
        print(f"Fetching forecast for {village['name']}")

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": village["lat"],
            "longitude": village["lon"],
            "hourly": "temperature_2m,relative_humidity_2m,pressure_msl,windspeed_10m,cloudcover,precipitation",
            "forecast_days": 5,
            "timezone": "Africa/Cairo"
        }

        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code != 200:
                print(f"API Error: {response.status_code}")
                continue

            if not response.text:
                print("Empty response")
                continue

            data = response.json()

        except Exception as e:
            print(f"Error: {e}")
            continue

        if "hourly" not in data:
            print(f"No forecast data for {village['name']}")
            continue

        times = data["hourly"]["time"]
        temp = data["hourly"]["temperature_2m"]
        humidity = data["hourly"]["relative_humidity_2m"]
        pressure = data["hourly"]["pressure_msl"]
        wind = data["hourly"]["windspeed_10m"]
        cloud = data["hourly"].get("cloudcover", [])
        rain = data["hourly"].get("precipitation", [])

        for i in range(len(times)):
            cursor.execute("""
                INSERT INTO weather.forecast_5days
                (city, latitude, longitude, weather_time, temperature, windspeed, humidity, pressure, cloudcover, precipitation)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                village["name"],
                village["lat"],
                village["lon"],
                times[i],
                temp[i],
                wind[i],
                humidity[i],
                pressure[i],
                cloud[i] if i < len(cloud) else None,
                rain[i] if i < len(rain) else 0
            ))

        time.sleep(2)

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="weather_forcast5days",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 1 * * *",  # كل يوم الساعة 2 صباحًا
    catchup=False,
) as dag:

    forecast_task = PythonOperator(
        task_id="fetch_forecast_weather",
        python_callable=fetch_forecast_weather
    )

    forecast_task
