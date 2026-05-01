from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="weather_from_stg_to_dwh",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    tags=["weather", "dwh"],
) as dag:

    create_schema = PostgresOperator(
        task_id="create_dwh_schema",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/stg_to_dwh/00_create_schema.sql",
    )

    dim_location = PostgresOperator(
        task_id="dim_location",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/stg_to_dwh/01_dim_location.sql",
    )

    dim_date = PostgresOperator(
        task_id="dim_date",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/stg_to_dwh/02_dim_date.sql",
    )

    # 🔥 03 Spray Recommendation
    fact_spray_recommendation = PostgresOperator(
        task_id="fact_spray_recommendation",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/stg_to_dwh/03_fact_spray_recommendation.sql",
    )

    # 🔥 04 Chill Units
    fact_chill_units = PostgresOperator(
        task_id="fact_chill_units",
        postgres_conn_id="postgres_airflow",
        sql="sql_weather/stg_to_dwh/04_fact_chill_units.sql",
    )

    # Dependencies
    create_schema >> dim_location
    create_schema >> dim_date

    dim_location >> [fact_spray_recommendation, fact_chill_units]
    dim_date >> [fact_spray_recommendation, fact_chill_units]
