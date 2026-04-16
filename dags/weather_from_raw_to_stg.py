from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="weather_from_raw_to_stg",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["weather", "etl", "staging"],
) as dag:

    # 1 - Schema
    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/raw_to_staging/00_create_schema.sql",
    )

