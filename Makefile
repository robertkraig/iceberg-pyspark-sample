SHELL := /bin/bash

VENV := .venv
PYTHON := $(VENV)/bin/python

.PHONY: help up down restart logs venv install load query all spark-up spark-down trino-up trino-down trino-logs trino-shell cluster-load cluster-query clean

help:
	@printf "Targets:\n"
	@printf "  make up       - start Iceberg REST + MinIO\n"
	@printf "  make down     - stop local services\n"
	@printf "  make restart  - restart local services\n"
	@printf "  make logs     - follow docker compose logs\n"
	@printf "  make venv     - create uv virtualenv\n"
	@printf "  make install  - install Python dependencies\n"
	@printf "  make load     - load CSV files into Iceberg\n"
	@printf "  make query    - run SQL query script\n"
	@printf "  make all      - up + venv + install + load + query\n"
	@printf "  make spark-up - start Spark master/worker\n"
	@printf "  make spark-down - stop Spark services\n"
	@printf "  make trino-up - start Trino service\n"
	@printf "  make trino-down - stop Trino service\n"
	@printf "  make trino-logs - follow Trino logs\n"
	@printf "  make trino-shell - open Trino CLI shell\n"
	@printf "  make cluster-load - submit load.py to Spark cluster\n"
	@printf "  make cluster-query - submit query.py to Spark cluster\n"
	@printf "  make clean    - remove Python cache files\n"

up:
	docker compose -f compose.yml up -d

down:
	docker compose -f compose.yml down --remove-orphans

restart: down up

logs:
	docker compose -f compose.yml logs -f

venv:
	uv venv $(VENV)

install: venv
	uv pip install -e .

load: install
	$(PYTHON) load.py

query: install
	$(PYTHON) query.py

all: up load query

spark-up:
	docker compose -f compose.yml --profile spark up -d

spark-down:
	docker compose -f compose.yml --profile spark stop spark-worker spark-master

trino-up:
	docker compose -f compose.yml up -d trino

trino-down:
	docker compose -f compose.yml stop trino

trino-logs:
	docker compose -f compose.yml logs -f trino

trino-shell: trino-up
	docker compose -f compose.yml exec -T trino trino --server localhost:8080 --catalog iceberg --schema sales

cluster-load: spark-up
	docker compose -f compose.yml exec -T -e ICEBERG_REST_URI=http://rest:8181 -e ICEBERG_S3_ENDPOINT=http://minio:9000 -e ICEBERG_S3_REGION=us-east-1 -e ICEBERG_S3_ACCESS_KEY=admin -e ICEBERG_S3_SECRET_KEY=password spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 /workspace/load.py

cluster-query: spark-up
	docker compose -f compose.yml exec -T -e ICEBERG_REST_URI=http://rest:8181 -e ICEBERG_S3_ENDPOINT=http://minio:9000 -e ICEBERG_S3_REGION=us-east-1 -e ICEBERG_S3_ACCESS_KEY=admin -e ICEBERG_S3_SECRET_KEY=password spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 /workspace/query.py

clean:
	rm -rf __pycache__ */__pycache__ .pytest_cache
