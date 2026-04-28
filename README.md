# Canadian Climate & Property Risk Data Platform

![Status](https://img.shields.io/badge/status-in%20development-yellow)
![Azure](https://img.shields.io/badge/cloud-Azure-blue)
![Spark](https://img.shields.io/badge/processing-Apache%20Spark-orange)
![Delta Lake](https://img.shields.io/badge/lakehouse-Delta%20Lake-0B7285)
![Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE)
![dbt](https://img.shields.io/badge/transformation-dbt-FF694B)
![PostGIS](https://img.shields.io/badge/serving-PostGIS-336791)
![Power BI](https://img.shields.io/badge/dashboard-Power%20BI-F2C811)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC)

> **Status:** In development  
> This repository is being built as a production-style data engineering portfolio project. The `develop` branch contains active work. The first stable release will be merged into `main` after the full pipeline, marts, dashboard evidence, and documentation are complete.

---

## Project At A Glance

- **One-sentence pitch:** Build a production-style Azure + Spark geospatial lakehouse that ingests, validates, transforms, models, and serves Canadian climate, hydrometric, wildfire, building-permit, disaster-event, floodplain, and municipal property datasets into trusted exposure marts for British Columbia and Alberta.
- **Architecture:** Azure VM + ADLS Gen2 + Delta Lake + PySpark/Sedona + Airflow + dbt Core + PostgreSQL/PostGIS + Power BI.
- **Scope:** British Columbia + Alberta province-wide 10km grid, Vancouver/Calgary 1km city grids, Vancouver parcel exposure screening, and Calgary property flood exposure screening.
- **Data quality:** Source freshness, schema hash, row-count anomaly checks, CRS validation, geometry validation, spatial join audit, dbt tests, and pipeline status JSON.
- **Validation:** Canadian Disaster Database backtesting, lift/top-K capture, score sensitivity, and rank stability checks.
- **Dashboard:** Public Power BI dashboard planned; screenshots and demo video will be provided as fallback if public embedding is unavailable.
- **Limitation:** This is a public-data-based exposure screening and prioritization platform. It is not an insurance-grade, legal, engineering-grade, or property appraisal risk model.

---

## Business Problem

Western Canada faces overlapping climate, flood, wildfire, hydrometric, infrastructure, and development exposure. Public data exists across federal, provincial, and municipal portals, but it is fragmented across different formats, spatial grains, coordinate systems, update frequencies, and quality levels.

The purpose of this project is to build a reliable data engineering platform that turns fragmented public datasets into curated, validated, and BI-ready geospatial data products.

The intended fictional stakeholder is a municipal or provincial climate resilience analytics team that needs repeatable data products for monitoring areas that may deserve deeper planning review.

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

## High-Level Architecture

```text
External Public Sources
|
v
Airflow ingestion DAGs
|
v
ADLS Gen2 / Delta Bronze
|
v
PySpark + Apache Sedona standardization
|
v
Delta Silver geospatial and feature tables
|
v
dbt Core transformations and tests
|
v
PostgreSQL / PostGIS Gold serving marts
|
v
Power BI dashboard + public project page
|
v
pipeline_status.json + screenshots + documentation