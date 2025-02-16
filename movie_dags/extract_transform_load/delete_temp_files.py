import argparse
import logging
import os

from loki_logger_handler.loki_logger_handler import LokiLoggerHandler
from minio import Minio

logger = logging.getLogger("dag-logger")
logger.setLevel(logging.DEBUG)
custom_logging_handler = LokiLoggerHandler(
    url="http://loki.loki.svc.cluster.local:3100/loki/api/v1/push",
    labels={
        "application": "portfolio",
        "component": "airflow_dag",
        "dag_task_id": "movie_cleaning",
    },
)
logger.addHandler(custom_logging_handler)


parser = argparse.ArgumentParser(
    prog="movie_delete_temp_files_container",
)
parser.add_argument("-f", "--file_prefix")
args = parser.parse_args()
print(args)
print(args.file_prefix)
if not args.file_prefix:
    logger.error("File prefix id for deletion not specified")
    raise Exception("File prefix id for deletion not specified")

config = dict(os.environ)
minio_client = Minio(
    "minio.minio.svc.cluster.local:9000",
    secure=False,
    access_key=config["MINIO_ACCESS_KEY"],
    secret_key=config["MINIO_SECRET_KEY"],
)

unique_id = args.file_prefix
for obj in [f"{unique_id}-extracted.json", f"{unique_id}-transformed.json"]:
    print(obj)
    minio_client.remove_object(
        bucket_name="temp-dag",
        object_name=obj,
    )
