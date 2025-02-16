import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="movie_retriever_dag",
    start_date=datetime.datetime(2025, 1, 4),
    catchup=False,
):
    extraction_pod = KubernetesPodOperator(
        task_id="movie-extract-transform-load",
        namespace="portfolio",
        image="iyadelwy/movie-extract_transform_load-image:latest",
        name="movie-extract-transform-load-pod",
        cmds=[
            "python",
            "./extract_transform_load.py",
        ],
        arguments=[
            "-t \"{{ dag_run.conf['title'] }}\"",
        ],
    )

    clean_up_pod = KubernetesPodOperator(
        task_id="clean-up-temp-directory",
        namespace="portfolio",
        image="iyadelwy/movie-extract_transform_load-image:latest",
        cmds=[
            "python",
            "./delete_temp_files.py",
        ],
        arguments=[
            "-f \"{{ti.xcom_pull(task_ids='movie-extract-transform-load', key='file_prefix')}}\"",
        ],
    )

    extraction_pod >> clean_up_pod
