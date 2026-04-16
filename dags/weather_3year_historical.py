from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date
import requests
import calendar
import time

# --------------------------------
# السنة المطلوبة
# --------------------------------
TARGET_YEAR = 2026

# --------------------------------
# الأماكن الجديدة (البحيرة)
# --------------------------------
villages = [
    {"name": "West Nubaria",   "lat": 30.86, "lon": 30.55},
    {"name": "Wadi El Natrun", "lat": 30.42, "lon": 30.3},
    {"name": "El-Bostan",      "lat": 30.95, "lon": 30.38},
    {"name": "Edko",           "lat": 31.3,  "lon": 30.3},
]

# --------------------------------
# API الأرشيف
# --------------------------------
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_and_store_weather_monthly():
    pg_hook = PostgresHook(postgres_conn_id="postgres_airflow")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for month in range(1, 13):
        start_date = date(TARGET_YEAR, month, 1)
        last_day = calendar.monthrange(TARGET_YEAR, month)[1]
        end_date = date(TARGET_YEAR, month, last_day)

        for village in villages:
            print(f"Fetching {village['name']} | {start_date} → {end_date}")

            params = {
                "latitude": village["lat"],
                "longitude": village["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "hourly": "temperature_2m,relative_humidity_2m,pressure_msl,windspeed_10m,cloudcover,precipitation",
                "timezone": "Africa/Cairo"
            }

            try:
                response = requests.get(ARCHIVE_URL, params=params, timeout=30)

                if response.status_code != 200:
                    print(f"Failed: {response.status_code}")
                    continue

                if not response.text:
                    print("Empty response")
                    continue

                data = response.json()

            except Exception as e:
                print(f"Error: {e}")
                continue

            if "hourly" not in data:
                print(f"No data for {village['name']}")
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
                    INSERT INTO weather.hourly_weather
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

            time.sleep(2)  # حماية من الـ rate limit

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="weather_3year_historical",
    start_date=datetime(2026, 3, 13),
    schedule_interval=None,
    catchup=False,
) as dag:

    historical_load = PythonOperator(
        task_id="historical_weather_load",
        python_callable=fetch_and_store_weather_monthly
    )

    historical_load
