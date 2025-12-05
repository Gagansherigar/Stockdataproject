import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
import os

ROOT_DIR = os.getenv("ROOT_DIR")

sys.path.append('/opt/airflow/api_request')
from insert_records import main

default_args = {
    'description': 'DAG to orchastrate data',
    'start_date': datetime(2025, 12, 5),

}

dag = DAG(
    dag_id='stockdata_orch',
    default_args=default_args,
    schedule=timedelta(hours=1),
    catchup=True
)

with dag:
    task1 = PythonOperator(
        task_id='ingestdata_task',
        python_callable=main
    )

    task2 = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source=f"{ROOT_DIR}/dbt/my_project",
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source=f"{ROOT_DIR}/dbt/profiles.yml",
                target='/root/.dbt/profiles.yml',
                type='bind'
            ),
        ],
        network_mode='stockdataproject_my-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2
