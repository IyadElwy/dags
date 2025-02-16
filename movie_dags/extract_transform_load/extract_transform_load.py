import argparse
import io
import json
import logging
import os
import sys
import uuid
from urllib.parse import quote

import psycopg2
import requests
from loki_logger_handler.loki_logger_handler import LokiLoggerHandler
from minio import Minio
from pydantic import BaseModel

config = dict(os.environ)

minio_client = Minio(
    "minio.minio.svc.cluster.local:9000",
    secure=False,
    access_key=config["MINIO_ACCESS_KEY"],
    secret_key=config["MINIO_SECRET_KEY"],
)

logger = logging.getLogger("dag-logger")
logger.setLevel(logging.DEBUG)
custom_logging_handler = LokiLoggerHandler(
    url="http://loki.loki.svc.cluster.local:3100/loki/api/v1/push",
    labels={
        "application": "portfolio",
        "component": "airflow_dag",
        "dag_task_id": "movie_extraction",
    },
)
logger.addHandler(custom_logging_handler)

unique_id = str(uuid.uuid4())

logger.info(f"{unique_id}-movie-extraction: Task started")

parser = argparse.ArgumentParser(
    prog="movie_retriever_container",
)

parser.add_argument("-t", "--title")


args = parser.parse_args()

base_url = f"https://www.omdbapi.com/?apikey={config['API_KEY']}"

if not args.title:
    logger.error(f"{unique_id}-movie-extraction: Title not specified")
    raise Exception("title must be specified")
else:
    base_url += f"&t={quote(args.title[1:])}"

logger.info(f"{unique_id}-movie-extraction: Requesting Movie data: {base_url}")
response = requests.get(base_url)
try:
    response.raise_for_status()
except Exception as e:
    logger.critical(f"{unique_id}-movie-extraction: Error: {e}", exc_info=True)
    raise e

result = response.json()
if result["Response"] == "False" and result["Error"] == "Movie not found!":
    logger.info(f"{unique_id}-movie-extraction: Movie not found: {base_url}")
    sys.exit(0)


logger.info(
    f"{unique_id}-movie-extraction: Movie data retrieved successfully: {args.title}"
)
file_name = f"{unique_id}-extracted.json"

try:
    content_bytesio = io.BytesIO(response.content)
    put_res = minio_client.put_object(
        bucket_name="temp-dag",
        object_name=file_name,
        data=content_bytesio,
        length=content_bytesio.getbuffer().nbytes,
    )
except Exception as e:
    logger.critical(
        f"{unique_id}-movie-extraction: Error saving data: {e}", exc_info=True
    )
    raise e
logger.info(f"{unique_id}-movie-extraction: Temp data saved to: {file_name}")


logger = logging.getLogger("dag-logger")
logger.setLevel(logging.DEBUG)
custom_logging_handler = LokiLoggerHandler(
    url="http://loki.loki.svc.cluster.local:3100/loki/api/v1/push",
    labels={
        "application": "portfolio",
        "component": "airflow-dag",
        "dag_task_id": "movie-transformation",
    },
)
logger.addHandler(custom_logging_handler)


logger.info(f"{unique_id}-movie-transformation: Task started")


class Movie(BaseModel):
    Title: str
    Year: str
    Rated: str
    Released: str
    Runtime: str
    Genre: str
    Director: str
    Writer: str
    Actors: str
    Plot: str
    Language: str


try:
    file_response = minio_client.get_object(
        bucket_name="temp-dag",
        object_name=f"{unique_id}-extracted.json",
    )
    json_data = file_response.json()
    movie = Movie(**json_data)
except Exception as e:
    logger.critical(
        f"{unique_id}-movie-transformation: Error transforming data: {e}", exc_info=True
    )
    raise e

logger.info(f"{unique_id}-movie-transformation: Movie transformed successfully")

try:
    file_name = f"{unique_id}-transformed.json"
    content_bytesio = io.BytesIO(bytearray(movie.model_dump_json(), encoding="utf-8"))
    minio_client.put_object(
        bucket_name="temp-dag",
        object_name=file_name,
        data=content_bytesio,
        length=content_bytesio.getbuffer().nbytes,
    )
except Exception as e:
    logger.critical(
        f"{unique_id}-movie-transformation: Error saving data: {e}", exc_info=True
    )
    raise e
logger.info(f"{unique_id}-movie-transformation: Temp data saved to: {file_name}")

logger = logging.getLogger("dag-logger")
logger.setLevel(logging.DEBUG)
custom_logging_handler = LokiLoggerHandler(
    url="http://loki.loki.svc.cluster.local:3100/loki/api/v1/push",
    labels={
        "application": "portfolio",
        "component": "airflow-dag",
        "dag_task_id": "movie-loading",
    },
)
logger.addHandler(custom_logging_handler)


logger.info(f"{unique_id}-movie-loading: Task started")


try:
    file_response = minio_client.get_object(
        bucket_name="temp-dag",
        object_name=f"{unique_id}-transformed.json",
    )
    json_data = file_response.json()
    movie = Movie(**json_data)
except Exception as e:
    logger.critical(
        f"{unique_id}-movie-loading: Error decoding data: {e}", exc_info=True
    )
    raise e

logger.info(f"{unique_id}-movie-loading: Temp data decoded successfully")


try:
    con = psycopg2.connect(
        "postgresql://applications:interlinked@postgresql.postgres.svc.cluster.local:5432/portfolio"
    )
    cur = con.cursor()
    cur.execute(
        "INSERT INTO Movies(title, normalized_title,\
            year,rated,released,runtime,\
                genre,director,writer,actors,plot,language)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)\
                 ON CONFLICT (normalized_title) DO NOTHING",
        (
            movie.Title,
            movie.Title.lower().replace(" ", ""),
            movie.Year,
            movie.Rated,
            movie.Released,
            movie.Runtime,
            movie.Genre,
            movie.Director,
            movie.Writer,
            movie.Actors,
            movie.Plot,
            movie.Language,
        ),
    )
    con.commit()
    con.close()
except Exception as e:
    logger.critical(
        f"{unique_id}-movie-transformation: Error loading data: {e}", exc_info=True
    )
    raise e

logger.info(f"{unique_id}-movie-loading: Movie loaded successfully: {movie.Title}")


with open("/airflow/xcom/return.json", "w") as f:
    json.dump({"file_prefix": unique_id}, f)
