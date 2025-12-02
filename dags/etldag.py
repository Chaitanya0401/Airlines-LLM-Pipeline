from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ingest():
    subprocess.run(["python", "/src/ingest.py"], check=True)

def run_transform():
    subprocess.run(["python", "/src/transform.py"], check=True)

def run_report():
    subprocess.run(["python", "/src/generate report.py"], check=True)

with DAG(
    'aviation_etl',
    default_args=default_args,
    description='ETL pipeline for AviationStack flights with LLM reports',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=['aviation', 'etl', 'llm'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_flights',
        python_callable=run_ingest
    )

    transform_task = PythonOperator(
        task_id='transform_flights',
        python_callable=run_transform
    )

    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=run_report
    )

    # Define DAG dependencies
    ingest_task >> transform_task >> report_task
