from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_and_upload():
    # connect local
    local_hook = PostgresHook(postgres_conn_id='local_postgres')
    
    # connect cloud
    cloud_hook = PostgresHook(postgres_conn_id='cloud_postgres')

    # extract data
    df = local_hook.get_pandas_df("""
        SELECT *
        FROM public.orders
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';
    """)

    # upload to temp table in cloud
    cloud_hook.insert_rows(
        table="temp_orders",
        rows=df.values.tolist(),
        target_fields=list(df.columns)
    )

with DAG(
    dag_id='local_to_cloud_dwh',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_upload',
        python_callable=extract_and_upload
    )

    load_staging = PostgresOperator(
        task_id='load_staging',
        postgres_conn_id='cloud_postgres',
        sql='sql/load_staging.sql'
    )

    load_dwh = PostgresOperator(
        task_id='load_dwh',
        postgres_conn_id='cloud_postgres',
        sql='sql/load_dwh.sql'
    )

    extract_task >> load_staging >> load_dwh
