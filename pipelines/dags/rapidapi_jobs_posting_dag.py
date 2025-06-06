import sys
import os

CURRENT_DIRECTORY = os.path.dirname(__file__)
if CURRENT_DIRECTORY == "/opt/airflow/dags":
    PIPELINES_ROOT = "/opt/airflow/repos/jobs-research/pipelines"
else:
    PIPELINES_ROOT = os.path.join(CURRENT_DIRECTORY, '..')
if PIPELINES_ROOT not in sys.path:
    sys.path.append(PIPELINES_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from rapidapi_jobs_posting import pipeline_load, pipeline_transform

default_args = {
    'start_date': datetime(2024, 1, 18),
    'retries': 0,
}

with DAG(
    dag_id='jobs_postings_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['etl'],
) as dag:

    load_task = PythonOperator(
        task_id='jobs_postings_load',
        python_callable=pipeline_load.main,
    )

    transform_task = PythonOperator(
        task_id='job_postings_transform',
        python_callable=pipeline_transform.main,
    )

    load_task >> transform_task

globals()['dag'] = dag