# Local Development Setup

This document describes the local development environment for the Canadian Climate Risk Platform.

## Purpose

The project remains local-first for development and testing while Azure is the target cloud environment.

Core Python ingestion, geospatial processing, Gold feature engineering, risk scoring, and validation pipelines can run directly against the local Parquet lakehouse.

---

## Local Architecture

```text
Python
pandas / GeoPandas / Shapely
        |
        v
Local Parquet Lakehouse
Bronze / Silver / Gold / Audit
        |
        +--------------------+
        |                    |
        v                    v
Optional PostGIS           dbt
spatial QA                 Snowflake target
```

- Airflow and the local Spark cluster are not part of the current local runtime.
- Cloud orchestration will use Azure Data Factory.
- Selected distributed workloads will later use Azure Databricks / PySpark.

---

## Setup

Create the environment file:

```powershell
Copy-Item .env.example .env
```

Install runtime and development dependencies:

```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Create local lakehouse directories:

```powershell
make setup
```

---

## Local Storage

The default backend is:

```env
STORAGE_BACKEND=local
LOCAL_LAKEHOUSE_ROOT=lakehouse
```

Local data is stored under:

```
lakehouse/
├── bronze/
├── silver/
├── gold/
└── audit/
```

---

## Docker Services

Docker Compose is only required for optional local services:

```powershell
docker compose up -d postgres dbt
```

| Service | Purpose |
|---|---|
| PostgreSQL / PostGIS | Optional spatial QA and experimentation |
| dbt container | Snowflake/dbt development |

Neither service is required to run the core Python lakehouse pipeline.

---

## Tests

Run unit tests:

```powershell
python -m pytest tests/unit -q
```

Run linting:

```powershell
python -m ruff check src tests
python -m black --check src tests
```

---

## Azure Development

Azure Data Lake Storage Gen2 is the cloud storage target.

Authenticate locally with:

```powershell
az login
```

Then configure:

```env
STORAGE_BACKEND=azure
AZURE_STORAGE_ACCOUNT_NAME=<storage-account>
```

The storage backend uses Azure Identity credentials rather than embedding storage keys in application code. Full Azure provisioning is managed separately through Terraform.