from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_PATH = "/opt/airflow/project"

default_args = {
    "owner": "sieme",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="realtime_file_check_pipeline",
    default_args=default_args,
    description="Checks realtime input folder and processes files",
    start_date=datetime(2026, 4, 27),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:

    check_realtime_folder = BashOperator(
        task_id="check_realtime_folder",
        bash_command=f"cd {PROJECT_PATH} && python src/realtime/realtime_pipeline.py",
    )