import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from dotenv import load_dotenv

ENV_PATH = "/home/usereugene/projects/Weatherstack/.env"
load_dotenv(ENV_PATH)

#Adding path for PythonOperator imports
sys.path.append("/opt/airflow/api-request")
from insert_records import main as run_weather_ingestion

default_args = {
    "owner": "eugene",
    "start_date": datetime(2026, 1, 9),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag-orchestrator",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="data_ingestion",
        python_callable=run_weather_ingestion
    )

    task2 = DockerOperator(
        task_id="data_transformation",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        environment={
            "DBT_PASSWORD": os.getenv("DBT_PASSWORD") 
        },
        mounts=[
            Mount(source="/home/usereugene/projects/Weatherstack/dbt",
                  target="/usr/app",
                  type="bind"),
            Mount(source="/home/usereugene/projects/Weatherstack/dbt/profiles.yml",
                  target="/root/.dbt/profiles.yml",
                  type="bind"),
        ],
        working_dir="/usr/app",
        network_mode="weatherstack-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
    )

    task1 >> task2