from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="weather_from_raw_to_stg",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["weather", "staging"],
) as dag:

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/raw_to_staging/00_create_schema.sql",
    )

    stg_hourly = PostgresOperator(
        task_id="stg_hourly_weather",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/raw_to_staging/01_stg_hourly_weather.sql",
    )

    stg_forecast = PostgresOperator(
        task_id="stg_forecast",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/raw_to_staging/02_stg_forecast.sql",
    )

    stg_locations = PostgresOperator(
        task_id="stg_locations",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/raw_to_staging/03_stg_locations.sql",
    )

    create_schema >> [stg_hourly, stg_forecast] >> stg_locations
