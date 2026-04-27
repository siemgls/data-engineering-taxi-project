from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_PATH = "/opt/airflow/project"

default_args = {
    "owner": "sieme",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_batch_pipeline",
    default_args=default_args,
    description="Batch pipeline for NYC taxi data",
    start_date=datetime(2026, 4, 27),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_taxi_pipeline = BashOperator(
        task_id="run_taxi_pipeline",
        bash_command=f"cd {PROJECT_PATH} && python run_taxi_pipeline.py",
    )

    run_taxi_pipeline