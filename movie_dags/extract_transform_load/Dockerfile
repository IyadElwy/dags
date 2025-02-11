FROM python:3.12-bookworm

RUN ["pip", "install", "requests", "python-dotenv", "loki-logger-handler", "pydantic", "psycopg2"]

WORKDIR /app

COPY . /app

RUN mkdir ./temp_data
