.PHONY: help setup install install-dev up down reset ps logs test lint format dbt-compile dbt-debug dbt-test postgres-check clean

help:
	@echo "Available commands:"
	@echo "  make setup           Create local directories"
	@echo "  make install         Install runtime dependencies"
	@echo "  make install-dev     Install development dependencies"
	@echo "  make up              Start local services"
	@echo "  make down            Stop local services"
	@echo "  make reset           Stop services and remove volumes"
	@echo "  make ps              Show container status"
	@echo "  make logs            Tail Docker logs"
	@echo "  make test            Run unit tests"
	@echo "  make lint            Run ruff and black checks"
	@echo "  make format          Format code"
	@echo "  make dbt-debug       Check dbt connection"
	@echo "  make dbt-compile     Compile dbt project"
	@echo "  make dbt-test        Run dbt tests"
	@echo "  make postgres-check  Check PostGIS connection"
	@echo "  make clean           Remove local runtime artifacts"

setup:
	mkdir -p configs src spark_jobs tests
	mkdir -p dbt/models dbt/macros dbt/tests dbt/profiles
	mkdir -p lakehouse/bronze lakehouse/silver lakehouse/gold lakehouse/audit
	mkdir -p dashboard/gis/data

install:
	pip install --upgrade pip
	pip install -r requirements.txt

install-dev:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

up:
	docker compose up -d postgres dbt

down:
	docker compose down

reset:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

test:
	pytest tests/unit -q

lint:
	ruff check src tests
	black --check src tests

format:
	black src tests
	ruff check src tests --fix

dbt-debug:
	docker compose exec dbt dbt debug

dbt-compile:
	docker compose exec dbt dbt compile

dbt-test:
	docker compose exec dbt dbt test

postgres-check:
	docker compose exec postgres psql -U climate_user -d climate_risk -c "CREATE EXTENSION IF NOT EXISTS postgis; SELECT PostGIS_Version();"

clean:
	rm -rf .pytest_cache .ruff_cache
	rm -rf dbt/target dbt/logs dbt/dbt_packages
	rm -rf spark-warehouse metastore_db derby.log