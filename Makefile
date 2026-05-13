.PHONY: help setup install install-dev up init-airflow down reset ps logs test lint format dbt-compile dbt-debug dbt-test spark-version postgres-check clean

help:
	@echo "Available commands:"
	@echo "  make setup           Create local directories"
	@echo "  make install         Install runtime dependencies"
	@echo "  make install-dev     Install development dependencies"
	@echo "  make up              Start local services"
	@echo "  make init-airflow    Initialize Airflow metadata DB and admin user"
	@echo "  make down            Stop local services"
	@echo "  make reset           Stop services and remove volumes"
	@echo "  make ps              Show container status"
	@echo "  make logs            Tail Docker logs"
	@echo "  make test            Run pytest"
	@echo "  make lint            Run ruff and black checks"
	@echo "  make format          Format code"
	@echo "  make dbt-debug       Check dbt connection"
	@echo "  make dbt-compile     Compile dbt project"
	@echo "  make dbt-test        Run dbt tests"
	@echo "  make spark-version   Check Spark version"
	@echo "  make postgres-check  Check Postgres/PostGIS connection"
	@echo "  make clean           Remove local runtime artifacts"

setup:
	mkdir -p airflow/dags airflow/logs
	mkdir -p configs src spark_jobs tests
	mkdir -p dbt/models dbt/macros dbt/tests dbt/profiles
	mkdir -p lakehouse/bronze lakehouse/silver lakehouse/gold lakehouse/audit
	mkdir -p warehouse

install:
	pip install --upgrade pip
	pip install -r requirements.txt

install-dev:
	pip install --upgrade pip
	pip install -r requirements-dev.txt

up:
	docker compose up -d postgres spark-master spark-worker airflow-webserver airflow-scheduler dbt

init-airflow:
	docker compose up airflow-init

down:
	docker compose down

reset:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	pytest

lint:
	ruff check .
	black --check .

format:
	black .
	ruff check . --fix

dbt-debug:
	docker compose exec dbt dbt debug

dbt-compile:
	docker compose exec dbt dbt compile

dbt-test:
	docker compose exec dbt dbt test

spark-version:
	docker compose exec spark-master /opt/spark/bin/spark-submit --version

postgres-check:
	docker compose exec postgres psql -U climate_user -d climate_risk -c "CREATE EXTENSION IF NOT EXISTS postgis; SELECT PostGIS_Version();"

clean:
	rm -rf airflow/logs/*
	rm -rf .pytest_cache .ruff_cache
	rm -rf dbt/target dbt/logs dbt/dbt_packages
	rm -rf spark-warehouse metastore_db derby.log