import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="movie_cleaner_dag",
    start_date=datetime.datetime(2025, 1, 4),
    catchup=False,
):
    clean_up_pod = KubernetesPodOperator(
        task_id="clean-up-temp-directory",
        namespace="portfolio",
        image="bitnami/minideb:latest",
        cmds=["bash"],
        arguments=["-c", "rm -f /dag_temp_data/{{ dag_run.conf['file_prefix'] }}*"],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="movie-processing-temp-volume", mount_path="/dag_temp_data"
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="movie-processing-temp-volume",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="movie-processing-temp-pvc"
                ),
            ),
        ],
    )

    clean_up_pod
