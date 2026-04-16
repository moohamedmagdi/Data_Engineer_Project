from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="weather_from_stg_to_dwh",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    tags=["olist", "analytics"],
) as dag:

    create_schema = PostgresOperator(
        task_id="create_analytics_schema",
        postgres_conn_id="postgres_source",
        sql="sql_weather/stg_to_dwh/00_create_schema.sql",
    )

