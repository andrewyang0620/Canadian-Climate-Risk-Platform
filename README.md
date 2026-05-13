# Canadian Climate Risk Data Platform

![Status](https://img.shields.io/badge/status-in%20development-yellow)
![AWS](https://img.shields.io/badge/cloud-AWS%20S3-orange)
![Snowflake](https://img.shields.io/badge/warehouse-Snowflake-29B5E8)
![Spark](https://img.shields.io/badge/processing-Apache%20Spark-E25A1C)
![Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE)
![dbt](https://img.shields.io/badge/transformation-dbt-FF694B)
![Power BI](https://img.shields.io/badge/dashboard-Power%20BI-F2C811)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

> **Status:** In development  
> This repository is being built as a production-style data engineering portfolio project. The `develop` branch contains active work. The first stable release will be merged into `main` after the full pipeline, Snowflake marts, dashboard evidence, public front-end, and documentation are complete.

---

## Project At A Glance

- **One-sentence pitch:** Build a production-style **AWS S3 + Snowflake ELT geospatial data platform** that ingests, validates, profiles, transforms, models, and serves Canadian climate, hydrometric, wildfire, building-permit, disaster-event, floodplain, and municipal property datasets into trusted exposure marts for British Columbia and Alberta.
- **Architecture:** AWS S3 + PySpark/Sedona + Airflow + Snowflake + dbt Core + Power BI + GitHub Pages public front-end.
- **Scope:** British Columbia + Alberta province-wide 10km grid, Vancouver/Calgary 1km city grids, Vancouver parcel exposure screening, and Calgary property flood exposure screening.
- **Data quality:** Source freshness, schema contracts, row-count validation, schema profiling, CRS validation, geometry validation, spatial join audit, dbt tests, and pipeline status JSON.
- **Validation:** Canadian Disaster Database backtesting, lift/top-K capture, score sensitivity, and rank stability checks.
- **Front-end demo:** Static public project page with architecture, pipeline-status cards, dashboard screenshots/Power BI embed, validation summary, limitations, and documentation links.
- **Limitation:** This is a public-data-based exposure screening and prioritization platform. It is not an insurance-grade, legal, engineering-grade, or property appraisal risk model.

---

## Public Demo Plan

The project will expose a lightweight public front-end under `public_site/`.

```text
public_site/
|
v
index.html
|
v
pipeline_status.json
|
v
assets/
  architecture.png
  dashboard_overview.png
  grid_hazard_page.png
  vancouver_parcel_page.png
  calgary_property_page.png
  validation_page.png
  data_reliability_page.png
```

The public page is designed to show the project quickly to recruiters and reviewers:

| Section | Purpose |
|---|---|
| Hero / pitch | Explain the DE project in one screen |
| Architecture | Show AWS S3 + Snowflake ELT flow |
| Pipeline status | Show latest run health from `pipeline_status.json` |
| Data sources | Summarize source coverage and ingestion status |
| Data quality | Show freshness, row count, schema, CRS, geometry, and dbt quality |
| Dashboard preview | Embed Power BI if available; otherwise show screenshots |
| Validation | Show CDD lift/top-K/sensitivity summary |
| Limitations | Explain exposure-screening limitations honestly |
| Links | GitHub repo, docs, dashboard, screenshots, demo video |

---

## Business Problem

Western Canada faces overlapping climate, flood, wildfire, hydrometric, infrastructure, and development exposure. Public data exists across federal, provincial, and municipal portals, but it is fragmented across different formats, spatial grains, coordinate systems, update frequencies, and quality levels.

The purpose of this project is to build a reliable data engineering platform that turns fragmented public datasets into curated, validated, and BI-ready geospatial data products.

The intended fictional stakeholder is a municipal or provincial climate resilience analytics team that needs repeatable data products for monitoring areas that may deserve deeper planning review.

---

## High-Level Architecture

```text
External Public Sources
|
v
Airflow ingestion DAGs
|
v
Python ingestion layer
|
v
AWS S3 Bronze
|
v
PySpark + Apache Sedona standardization
|
v
AWS S3 Silver
|
v
Snowflake warehouse
|
v
dbt Core transformations and tests
|
v
Gold / Mart tables
|
v
Power BI dashboard + public project page
|
v
pipeline_status.json + screenshots + documentation
```

---

## Data Platform Layers

### Bronze — Raw Source Preservation

Bronze stores immutable source snapshots.

Local development path:

```text
lakehouse/bronze/
```

Cloud target:

```text
s3://<data-lake-bucket>/bronze/
```

Bronze records:

- raw source file
- `metadata.json`
- `bronze_runs.jsonl`
- checksum
- row count when available
- source URL and run ID
- source-specific extra metadata

### Silver — Standardized Processing Layer

Silver normalizes raw sources into reusable analytical inputs:

- standardized dates and keys
- standardized CRS and geometry
- generated 10km BC/AB grid
- generated 1km Vancouver/Calgary grid
- station-grid maps
- flood/property overlays
- coverage-confidence features

Target format:

```text
Parquet / GeoParquet on AWS S3
```

### Gold — Snowflake + dbt Marts

Snowflake is the primary analytical warehouse.

Planned schemas:

```text
BRONZE
SILVER
GOLD
AUDIT
```

dbt owns:

```text
staging
|
v
intermediate
|
v
marts
```

---

## Core Data Products

### Grid-Level Marts

- `mart_grid_month_hazard_exposure`
- `mart_grid_month_priority`
- `mart_municipality_month_priority`

These marts support BC/Alberta grid-level monitoring, monthly prioritization, hazard component analysis, and municipality-level aggregation.

### Property-Context Marts

- `mart_vancouver_parcel_exposure`
- `mart_calgary_property_flood_exposure`

These marts support city-level property-context screening using public parcel, property assessment, floodplain/flood hazard, and permit data.

### Reliability and Validation Marts

- `mart_data_reliability`
- `mart_score_validation`
- `mart_sensitivity_analysis`

These marts make data quality, source freshness, spatial join success, dbt test results, and score validation visible as first-class outputs.

---

## Current Implementation Status

### Completed / In Progress

- Project scaffold and local development setup
- Source registry and source configuration contracts
- National Bronze ingestion for Canadian Disaster Database
- Municipal Bronze ingestion for Vancouver and Calgary open data sources
- OpenDataSoft downloader
- Socrata downloader
- Socrata pagination with row-count reconciliation
- Bronze writer with raw file, metadata, checksum, and manifest records
- Municipal source availability report
- Source config contract cleanup
- Bronze manifest reader
- Bronze extract audit foundation
- AWS S3 + Snowflake architecture realignment

### Next Work

```text
source profiling
|
v
S3 storage backend
|
v
Silver standardization
|
v
Snowflake load
|
v
dbt marts
|
v
Airflow DAG orchestration
|
v
Power BI dashboard
|
v
public front-end page
```

---

## Data Quality Strategy

Quality is enforced across the full pipeline.

### Source-Level Quality

- source availability checks
- row counts
- file size checks
- checksums
- schema hash / schema drift detection
- Socrata row-count reconciliation
- extract metadata and manifest logging

### Source Profiling

Before Silver implementation, raw files are profiled to detect:

- actual columns
- sample rows
- candidate IDs
- candidate join keys
- coordinate fields
- measurement fields
- contract mismatches

### Geospatial Quality

- coordinate range validation
- CRS standardization
- geometry validity checks
- geometry repair logging
- spatial join success rate
- unmatched row audit
- coverage confidence score

### dbt / Warehouse Quality

- `not_null`
- `unique`
- `relationships`
- `accepted_values`
- custom score range tests
- no missing lineage tests
- no invalid priority tier tests

---

## Main Data Sources

### National / Provincial

- ECCC Historical Climate Data
- ECCC Hydrometric Real-Time Data
- HYDAT historical hydrometric archive
- CWFIS / CNFDB wildfire history
- Statistics Canada building permits
- Census / CSD / province boundaries
- Canadian Disaster Database

### Vancouver

- Property parcel polygons
- Property tax report
- Issued building permits
- Designated floodplain

### Calgary

- Property assessment
- Regulatory flood hazard map
- Building permits
- Development permits

---

## Technology Stack

### Cloud and Storage

- AWS S3 for Bronze and Silver data lake zones
- Snowflake for analytical warehouse

### Processing

- Python ingestion layer
- PySpark for distributed transformations
- Apache Sedona for geospatial processing
- GeoPandas as local fallback

### Orchestration

- Apache Airflow

### Transformation

- dbt Core with Snowflake adapter

### Visualization and Public Evidence

- Power BI dashboard
- GitHub Pages static front-end
- screenshots and demo video fallback
- `pipeline_status.json`

### DevOps

- Docker Compose for local services
- GitHub Actions for CI
- Terraform placeholders for AWS and Snowflake

---

## Local Development

Install dependencies:

```powershell
pip install -r requirements.txt
```

Run unit tests:

```powershell
pytest tests/unit -q
```

List municipal ingestion plans:

```powershell
python -m src.ingestion.run_bronze_ingestion --list-municipal-plans
```

Run municipal availability validation:

```powershell
python -m src.ingestion.validate_municipal_sources --download
```

Run Bronze extract audit:

```powershell
python -m src.audit.extract_audit --source-group municipal
```

---

## Repository Structure

```text
configs/
src/
  ingestion/
  audit/
  profiling/
  validation/
  geospatial/
  scoring/
  utils/
spark_jobs/
airflow/
dbt/
  models/
  profiles/
infra/
  terraform/
    aws/
    snowflake/
dashboard/
  powerbi/
  screenshots/
public_site/
  index.html
  pipeline_status.json
  assets/
docs/
tests/
```

---

## Why AWS S3 + Snowflake

This design mirrors a common modern data platform pattern:

```text
object storage data lake
|
v
distributed processing
|
v
cloud data warehouse
|
v
dbt marts
|
v
BI + public demo
```

It separates raw data storage from compute and keeps warehouse modeling focused on curated, analytics-ready tables.

---

## Limitations

This project is an exposure screening and prioritization platform.

It is not:

- an insurance underwriting model;
- a legal property risk assessment;
- a property appraisal model;
- an engineering-grade flood-depth model;
- a real-time emergency alerting system.

The platform uses public data and reports coverage confidence, quality flags, and limitations alongside every major output.

---

## License

MIT
