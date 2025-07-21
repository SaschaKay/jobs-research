This folder should be mounted in /opt/airflow/dags. Bash:
    cd /opt/airflow/dags
    ln -s /opt/airflow/repos/jobs-research/pipelines/dags/*.py .

To test a dag add:
    if __name__ == "__main__":
        dag.test()
To skip tasks during the test:
    dag.test(mark_success_pattern="regexp to match task ids") 