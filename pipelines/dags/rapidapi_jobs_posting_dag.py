from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

from rapidapi_jobs_posting import pipeline_load, pipeline_transform
from rapidapi_jobs_posting.config import PipelineParams

env = Variable.get(
    "ENV"
)  # don't add default_var to prevent accidental run with wrong config
# pulled from server OS variable
# DO NOT CHANGE MANUALLY
params = PipelineParams(env).get_params()

default_args = {
    "start_date": datetime(2024, 1, 18),
    "retries": 1,
}

with DAG(
    dag_id="jobs_postings_pipeline",
    params=params,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["etl", "jobs_research", env, "backfill_aware"],
) as dag:
    load_task = PythonOperator(
        task_id="jobs_postings_load",
        python_callable=pipeline_load.main,
    )

    transform_task = PythonOperator(
        task_id="job_postings_transform",
        python_callable=pipeline_transform.main,
    )

    load_task >> transform_task

globals()["dag"] = dag