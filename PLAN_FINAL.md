# Canadian Climate Risk Data Platform — PLAN_FINAL

## 0. Final Project Identity

### Project Name

**Canadian Climate Risk Data Platform**

### Final One-Sentence Definition

Build a production-style **AWS S3 + Snowflake ELT data engineering platform** that ingests, validates, profiles, transforms, models, and serves Canadian climate, hydrometric, wildfire, disaster-event, census/boundary, building-permit, floodplain, and municipal property datasets into trusted **grid-level and city property-context geospatial exposure marts** for British Columbia and Alberta, with dbt-modeled Snowflake marts, auditability, data-quality evidence, Power BI reporting, and a lightweight public front-end demo.

### Core Positioning

This is a **Data Engineering project first**.

The project is not mainly a dashboard project, not a WebGIS/front-end project, and not a climate-science model. The core value is that heterogeneous public data flows reliably through a modern ELT platform into curated analytical marts with data quality, lineage, validation, observability, and reproducible cloud-oriented design.

### What the Project Claims

The project claims to build:

- a reproducible AWS + Snowflake data platform;
- a local-first development workflow that can target cloud storage;
- a Bronze/Silver/Gold ELT data flow;
- raw-source preservation with metadata, manifests, and checksums;
- config-driven source contracts;
- schema profiling before Silver standardization;
- geospatial feature-engineering pipelines;
- grid-level climate and hazard exposure marts;
- Vancouver parcel-level exposure screening marts;
- Calgary property flood exposure screening marts;
- municipality-level aggregation and validation marts;
- Snowflake/dbt-modeled analytical tables;
- data-quality, lineage, audit, and pipeline-status outputs;
- a Power BI dashboard and static public project page as downstream evidence.

### What the Project Does Not Claim

The project does **not** claim to build:

- an insurance underwriting model;
- a legal property risk assessment;
- an engineering-grade flood-depth model;
- a property-value prediction model;
- an address-level consumer risk product;
- a full-stack WebGIS application;
- a real-time streaming risk system;
- a full-Canada property-level platform.

Correct wording:

> This project provides public-data-based climate, hazard, and property-context exposure screening. It is not an insurance-grade, engineering-grade, legal, or property appraisal risk model.

---

## 1. Business Storyline

### 1.1 Business Context

Western Canada faces overlapping climate and infrastructure risks:

- British Columbia has wildfire, flood, coastal, mountainous, and development-pressure exposure.
- Alberta has flood, hydrometric, wildfire, urban expansion, and property-level flood planning exposure.
- Public datasets exist, but they are fragmented across federal, provincial, and municipal portals.
- These datasets arrive in different formats, spatial grains, frequencies, coordinate systems, and quality levels.

The business problem is not that one more map is needed. The real problem is that planners, analysts, and decision makers need **trusted, refreshable, comparable, validated geospatial exposure data products**.

### 1.2 Stakeholder Framing

The intended fictional stakeholder is:

> A municipal / provincial climate resilience analytics team that wants a repeatable data platform for identifying which areas deserve monitoring, planning attention, and deeper review based on public hazard, climate, disaster, development, and property-context signals.

Potential users:

- climate resilience analysts;
- municipal planning analysts;
- infrastructure risk analysts;
- emergency management analysts;
- public-sector data teams;
- BI teams needing trusted geospatial marts.

### 1.3 Main Business Question

> Which areas in British Columbia and Alberta should be prioritized for climate and hazard exposure monitoring when public climate, hydrometric, wildfire, disaster, development, and property-context data are integrated into a reliable geospatial data platform?

### 1.4 Supporting Questions

1. Which 10km grid cells across BC and Alberta show higher combined climate, wildfire, flood/hydrometric, historical disaster, and development exposure?
2. Which urban areas in Vancouver and Calgary show finer-grained exposure patterns at 1km resolution?
3. Which Vancouver parcels intersect designated floodplain areas and also show property/development exposure signals?
4. Which Calgary properties fall into regulatory flood-related zones and show high assessed-value or development-activity exposure?
5. Do high-priority grid or municipality areas capture more historical Canadian Disaster Database events than random or equal-weight baselines?
6. Which outputs are reliable, and which are limited by weak station coverage, failed joins, stale sources, schema drift, or geometry issues?

### 1.5 Portfolio Narrative

```text
Public climate and hazard data is valuable but fragmented.
|
v
I built a modern ELT pipeline that ingests and preserves raw data.
|
v
I validated source availability, pagination, row counts, checksums, and metadata.
|
v
I profiled real raw files before committing Silver transformations.
|
v
I standardized schemas, dates, coordinates, geometries, and spatial units.
|
v
I generated 10km BC/AB grids and 1km city grids as stable analytical contracts.
|
v
I engineered grid-level hazard, weather, wildfire, hydrometric, development, and disaster features.
|
v
I added Vancouver and Calgary property-context deep dives because those cities have useful municipal open data.
|
v
I modeled Snowflake/dbt marts for downstream BI and analysis.
|
v
I tracked data quality, freshness, schema drift, CRS transforms, and spatial join success.
|
v
I validated prioritization scores against Canadian Disaster Database events.
|
v
I surfaced both results and reliability in Power BI and a public static project page.
```

### 1.6 Recruiter-Friendly Summary

> Built an AWS S3 + Snowflake ELT geospatial data platform integrating Canadian climate, hydrometric, wildfire, permit, disaster, and municipal property data into trusted grid-level and property-context exposure marts, with Airflow orchestration, PySpark/Sedona processing, dbt modeling, source profiling, data-quality auditing, and Power BI/public-site reporting.

---

## 2. Final Scope

### 2.1 Geographic Scope

Final geographic scope:

```text
British Columbia + Alberta
```

Rationale:

| Area | Reason |
|---|---|
| British Columbia | Wildfire, flood, coastal/mountain climate exposure, and Vancouver parcel-data story |
| Alberta | Flood, hydrometric, wildfire, urban expansion, and Calgary property-flood story |
| BC + Alberta together | Large enough to prove scale, narrow enough to finish, strong Western Canada climate-risk narrative |

Excluded:

- Full Canada grid coverage
- Ontario / Quebec / Atlantic Canada
- National property-level modeling
- Full parcel coverage outside Vancouver and Calgary
- Address-search product
- Private property/claims/insurance data

### 2.2 Time Scope

| Data Type | Time Window | Purpose |
|---|---|---|
| Climate / hydrometric / wildfire / CDD | 2016-2025 | Historical baseline, percentiles, anomalies, validation |
| Current hazard signals | 2026 incremental refresh | Current monitoring layer |
| Vancouver permits | 2017-present where available | Parcel development exposure |
| Vancouver property tax | Current + available historical extracts | Parcel exposure proxy |
| Calgary assessment / permits / flood data | Current + available historical extracts | Property flood exposure screening |

### 2.3 Final Spatial Scale

| Layer | Spatial Unit | Area | Purpose | Status |
|---|---|---|---|---|
| Province-wide grid | 10km x 10km metric grid | Full BC + Alberta | Main exposure surface | Core |
| City grid | 1km x 1km metric grid | Vancouver + Calgary city boundaries | Higher-resolution urban comparison | Core |
| Vancouver parcel | Property parcel polygon | City of Vancouver | Parcel-level exposure screening | Core |
| Calgary property / parcel | Property geometry or centroid depending on available fields | City of Calgary | Property flood exposure screening | Core |
| Municipality / CSD | Census subdivision / municipality polygon | BC + Alberta | Aggregation, comparison, validation, BI slicers | Core support |

### 2.4 Final Scale Choice

```text
BC + Alberta province-wide 10km grid
+
Vancouver / Calgary 1km city grid
+
Vancouver parcel deep dive
+
Calgary property flood deep dive
+
municipality aggregation layer
```

This is the best balance because:

- 10km grid proves province-scale data engineering.
- 1km city grids prove finer spatial feature engineering.
- Vancouver parcel data supports a high-value parcel overlay story.
- Calgary property/flood data supports a flood-centric property screening story.
- Municipality aggregation keeps the output understandable for BI and validation.
- The project remains Data Engineering focused instead of becoming a full WebGIS product.

---

## 3. Final Technical Stack

| Layer | Technology | Final Decision | Why It Exists |
|---|---|---|---|
| Cloud | AWS | Primary target cloud platform | Common DE stack and strong JD relevance |
| Object Storage | AWS S3 | Bronze/Silver/Audit data lake storage | Durable storage, cloud-native ELT pattern |
| Processing | PySpark | Core transformation engine | Historical backfill, feature engineering, aggregation |
| Geospatial Processing | Apache Sedona + GeoPandas fallback | Distributed spatial joins and local fallback | High-value geospatial DE signal |
| Orchestration | Apache Airflow | DAGs, retry, backfill, audit | Production-style pipeline control |
| Warehouse | Snowflake | Primary analytical warehouse | Modern cloud data warehouse and dbt target |
| Transformation | dbt Core + dbt-snowflake | Staging, intermediate, marts, tests | Analytics engineering, lineage, data tests |
| BI / Demo | Power BI | Stable dashboard layer | Recruiter-friendly output |
| Public Front-End | GitHub Pages static site | Landing page, status cards, screenshots, docs links | Lightweight public evidence |
| IaC | Terraform | AWS + Snowflake placeholders | Reproducible cloud setup |
| Containerization | Docker Compose | Local Airflow + Spark + dbt + Postgres metadata | Reproducible development |
| CI/CD | GitHub Actions | lint, unit tests, dbt parse/compile, Docker build | Engineering workflow proof |
| Quality | Source audit + schema hash + row count + geometry + CRS + dbt tests | Full-chain quality | Main DE differentiator |

### 3.1 Local vs Cloud Runtime

Local development remains local-first:

```text
lakehouse/bronze
lakehouse/silver
lakehouse/audit
Docker Compose
Airflow local metadata Postgres
Optional local PostGIS validation
```

Cloud target:

```text
AWS S3 Bronze
AWS S3 Silver
AWS S3 Audit/Profile outputs
Snowflake BRONZE/SILVER/GOLD/AUDIT schemas
dbt Snowflake models
Power BI dashboard
GitHub Pages public front-end
```

Postgres is **not** the primary analytical warehouse anymore. It remains useful only for:

- Airflow metadata database;
- optional local PostGIS/geospatial validation;
- optional comparison layer if needed.

### 3.2 Display-Layer Decision

Final dashboard tool:

```text
Power BI
```

Public front-end:

```text
GitHub Pages static site
```

The public front-end is not the data product. It is a presentation wrapper that shows:

- project pitch;
- architecture diagram;
- pipeline status cards;
- source coverage summary;
- data-quality summary;
- Power BI embed or screenshots;
- demo video fallback;
- links to docs and repo sections.

Removed from final scope:

- Next.js production app
- React-heavy front-end
- MapLibre/deck.gl custom dashboard
- FastAPI tile server
- vector tile serving
- real-time interactive WebGIS

### 3.3 Source of Truth

The source of truth is:

```text
S3 raw/processed objects
+
Snowflake curated marts
+
audit/status outputs
+
dbt tests
```

Not:

```text
Dashboard visuals
```

Power BI and the public site are consumers of the data product.

---

## 4. Data Sources

### 4.1 National / Provincial Sources

| Source | Role | Ingestion Approach | Frequency | Final Use |
|---|---|---|---|---|
| ECCC Historical Climate | Temperature, precipitation, historical weather baseline | Bulk CSV / Datamart-style extraction | Historical + periodic refresh | Weather extreme score |
| ECCC Hydrometric Real-Time | Recent water level / discharge | REST / real-time extract | Daily or near-real-time | Current flood / water signal |
| HYDAT | Historical water level / discharge baseline | SQLite bulk download | Quarterly / periodic | Seasonal hydrometric percentile baseline |
| CWFIS / CNFDB Wildfire | Wildfire point history and proximity | GeoJSON / CSV / bulk download | Seasonal / periodic | Wildfire exposure score |
| StatCan Building Permits | Development exposure proxy | Bulk CSV first; API later if needed | Monthly | Municipality/grid development exposure |
| Census / CSD / Province Boundaries | Administrative mapping | Boundary file download | Periodic | Municipality aggregation and validation |
| Canadian Disaster Database | Historical disaster validation labels | Open Government spreadsheet download | Periodic | Backtesting, lift, validation |

### 4.2 Vancouver Municipal Sources

| Source | Role | Processing Details | Final Mart Use |
|---|---|---|---|
| Vancouver property parcel polygons | Parcel geometry | Validate geometry, normalize parcel key, standardize CRS | `mart_vancouver_parcel_exposure` |
| Vancouver property tax report | Land/improvement/total value proxy and property attributes | Join to parcel where possible; flag unmatched records | Exposure proxy |
| Vancouver issued building permits | Building/development activity | Normalize permit type, date, value, address/parcel linkage | Development activity feature |
| Vancouver designated floodplain | Floodplain polygon | Parcel/floodplain overlay; compute overlap percentage | Floodplain exposure feature |
| Vancouver city boundary | Scope boundary | Clip 1km grid and parcel scope | City grid and parcel layer |

### 4.3 Calgary Municipal Sources

| Source | Role | Processing Details | Final Mart Use |
|---|---|---|---|
| Calgary property assessment | Property assessment and property context | Normalize assessment year, value fields, property type, geometry/centroid | `mart_calgary_property_flood_exposure` |
| Calgary regulatory flood map / flood hazard layers | Floodway, flood fringe, overland flood zones | Overlay with property geometry or centroid; derive flood category flags | Flood exposure score |
| Calgary development permits | Development activity | Normalize permit type, status, date, spatial fields | Development feature |
| Calgary building permits | Building activity | Normalize date/type/status and spatial fields | Building activity feature |
| Calgary city / community boundaries | Scope and rollup boundaries | Validate geometry and CRS | City grid and aggregation |

### 4.4 Data Inclusion Rules

A dataset enters the project only if it satisfies at least four of the following six criteria:

1. Publicly accessible or reproducibly downloadable.
2. Relevant to climate, flood, wildfire, development exposure, property context, or validation.
3. Contains time, geography, stable join keys, or candidate contract fields.
4. Can be ingested into Bronze without manual cleaning.
5. Can be validated by schema, row count, coordinate, geometry, or domain checks.
6. Can contribute to a downstream mart, score, validation, or data-quality metric.

---

## 5. Config Contracts and Source Profiling

### 5.1 Source Config Role

`configs/source_config.yml` defines:

- source provider;
- source URL;
- access method;
- file format;
- expected frequency;
- target Bronze table;
- target Silver table;
- required raw fields;
- downstream contracts;
- validation checks.

### 5.2 Contract Pattern

Raw fields should not be guessed blindly. The project uses contracts such as:

- `identity_contract`
- `join_contract`
- `coordinate_contract`
- `measurement_contract`
- `climate_measurement_contract`
- `location_mapping_contract`
- `municipality_mapping_contract`
- `boundary_contract`

Rules:

```text
required_fields
|
v
Bronze/raw fields that should be directly checkable

contract fields
|
v
downstream requirements that may need Silver standardization

post_silver_validation_checks
|
v
checks that only make sense after standardization
```

### 5.3 Source Profiling

Before Silver standardization, run raw source profiling:

```text
latest Bronze raw file
|
v
detect file type
|
v
extract columns / property keys
|
v
sample rows
|
v
detect candidate IDs / join keys / coordinates / measurement fields
|
v
compare against source contracts
|
v
write profile JSON and markdown summary
```

Output targets:

```text
lakehouse/profiles/source_schema_profiles.json
docs/source_schema_profile_summary.md
```

This prevents writing Silver logic based on guessed field names.

---

## 6. Spatial Engineering Design

### 6.1 CRS Strategy

| Stage | CRS | Rule |
|---|---|---|
| Raw ingestion | Preserve source CRS | Record original CRS metadata |
| Standard processing | EPSG:3347 Canada Lambert | Use for area and distance calculations |
| City processing | EPSG:3347 by default | Use local projected CRS only if source requires it |
| Warehouse / BI | EPSG:4326 centroids + WKT/geometry where useful | BI compatibility |
| Audit | Store source CRS, processed CRS, transform status | Prevent silent CRS errors |

Hard rules:

- Never compute area or distance directly on latitude/longitude degrees.
- All spatial joins must write an audit record.
- All geometry repair operations must be counted and logged.
- Store centroids for BI and geometry/WKT where useful.

### 6.2 Grid Generation

#### 10km BC/AB Grid

```text
Load BC + Alberta boundaries
|
v
Transform boundaries to EPSG:3347
|
v
Generate 10km x 10km metric cells covering bounding box
|
v
Clip grid cells to BC/AB boundary
|
v
Assign stable grid_id
|
v
Compute centroid lat/lon in EPSG:4326
|
v
Write silver_grid_10km
```

#### 1km Vancouver/Calgary City Grid

```text
Load Vancouver and Calgary boundaries
|
v
Transform to EPSG:3347
|
v
Generate 1km x 1km metric cells inside city boundaries
|
v
Assign city_grid_id and city_name
|
v
Compute centroid lat/lon
|
v
Write silver_grid_1km_city
```

### 6.3 Spatial Join Types

| Join Type | Example | DE Value |
|---|---|---|
| Point-to-grid | weather station to 10km grid | station coverage and nearest station logic |
| Point-to-polygon | wildfire points to grid/city/municipality | proximity and count features |
| Polygon-to-grid | floodplain or boundary to grid | area overlap and coverage features |
| Polygon-to-polygon | Vancouver parcel to floodplain | high-value geospatial engineering |
| Property-to-flood-zone | Calgary property to regulatory flood zone | property flood exposure feature |
| Grid-to-municipality | aggregate grid results to CSD | validation and BI summary |

### 6.4 Spatial Join Audit

Every spatial join writes to:

```text
audit_spatial_join
```

Required fields:

```text
run_id
join_name
left_table
right_table
left_count
matched_count
unmatched_count
match_rate
median_distance_km
p95_distance_km
geometry_invalid_count
geometry_repaired_count
crs_source
crs_target
created_at
severity
```

---

## 7. Data Layer Design

### 7.1 Bronze Layer

Purpose:

- preserve raw source data;
- support reproducible reprocessing;
- detect source drift;
- provide auditability.

Storage:

```text
local: lakehouse/bronze/
cloud target: s3://<bucket>/bronze/
```

Bronze outputs:

```text
raw file
metadata.json
bronze_runs.jsonl manifest
```

Bronze metadata fields:

```text
run_id
source_name
source_url
extract_timestamp
raw_file_path
file_checksum
checksum_algorithm
schema_hash
ingestion_method
source_period_start
source_period_end
row_count
load_status
extra_metadata
```

### 7.2 Silver Layer

Purpose:

- standardize schemas;
- normalize dates and keys;
- validate coordinates and geometries;
- standardize CRS;
- generate grids;
- build reusable feature inputs.

Storage:

```text
local: lakehouse/silver/
cloud target: s3://<bucket>/silver/
format target: Parquet / GeoParquet
```

Silver source-level tables:

```text
silver_climate_daily
silver_hydro_daily
silver_hydro_baseline
silver_wildfire_event
silver_permit_monthly
silver_disaster_event_month
silver_boundary_province
silver_boundary_municipality
silver_grid_10km
silver_grid_1km_city
silver_station_grid_map
silver_grid_hazard_features
silver_grid_development_features
silver_vancouver_parcel
silver_vancouver_property_tax
silver_vancouver_building_permits
silver_vancouver_floodplain
silver_calgary_property
silver_calgary_flood_hazard
silver_calgary_building_permits
silver_calgary_development_permits
silver_spatial_coverage_confidence
```

Intermediate / overlay products:

```text
int_vancouver_parcel_flood_overlay
int_vancouver_permit_property_map
int_calgary_property_flood_overlay
int_calgary_permit_property_map
int_municipality_grid_rollup
int_cdd_event_month_labels
```

### 7.3 Gold / Warehouse Layer

Primary warehouse:

```text
Snowflake
```

Schemas:

```text
BRONZE
SILVER
GOLD
AUDIT
```

dbt builds:

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

## 8. Final Data Products / Marts

### 8.1 Core Dimensions

| Model | Grain | Purpose |
|---|---|---|
| `dim_date` | one row per date | Shared date dimension |
| `dim_month` | one row per month | Monthly aggregation |
| `dim_source` | one row per source | Source lineage and freshness |
| `dim_spatial_unit` | one row per spatial unit | Unified grid/parcel/property/municipality dimension |
| `dim_municipality` | one row per municipality/CSD | Administrative rollup |
| `dim_station` | one row per climate/hydro station | Station metadata and coverage |

### 8.2 Grid-Level Marts

#### `mart_grid_month_hazard_exposure`

Grain:

```text
one row per grid cell per month
```

Main columns:

```text
grid_id
resolution_m
province
municipality_key
centroid_lat
centroid_lon
geometry_wkt
month_key
precip_percentile
heat_anomaly_score
hydro_station_count_75km
nearest_hydro_station_km
water_level_percentile
wildfire_count_25km
wildfire_count_50km
nearest_wildfire_km
historical_disaster_count_50km
permit_value_monthly
permit_unit_count_monthly
development_exposure_score
coverage_confidence_score
data_quality_flag
```

#### `mart_grid_month_priority`

Grain:

```text
one row per grid cell per month per score method
```

Main columns:

```text
grid_id
month_key
score_method
flood_signal_score
wildfire_exposure_score
weather_extreme_score
development_exposure_score
grid_priority_score
priority_tier
coverage_confidence_score
score_version
```

### 8.3 Vancouver Parcel Mart

#### `mart_vancouver_parcel_exposure`

Grain:

```text
one row per Vancouver parcel per assessment/reporting year
```

### 8.4 Calgary Property Flood Mart

#### `mart_calgary_property_flood_exposure`

Grain:

```text
one row per Calgary property / parcel per assessment year
```

### 8.5 Municipality Aggregation Mart

#### `mart_municipality_month_priority`

Grain:

```text
one row per municipality per month
```

### 8.6 Reliability and Validation Marts

```text
mart_data_reliability
mart_score_validation
mart_sensitivity_analysis
```

These show source freshness, row count anomalies, schema drift, geometry validity, spatial join success, dbt test results, and score validation.

---

## 9. Score and Validation Design

### 9.1 Score Naming

Do not use one generic “risk score.” Use named scores by grain and purpose.

| Score | Grain | Purpose |
|---|---|---|
| Grid-Level Climate-Hazard Exposure Priority Score | grid-month | Prioritize grid cells for monitoring |
| Vancouver Parcel Exposure Screening Score | parcel-year | Screen parcel exposure based on floodplain, property-value proxy, and permit activity |
| Calgary Property Flood Exposure Screening Score | property-year | Screen property flood exposure based on regulatory flood layers and property context |
| Municipality Climate-Exposure Priority Tier | municipality-month | Aggregated BI and validation summary |

### 9.2 Coverage Confidence

Coverage confidence is reported beside the score instead of hidden inside it.

```text
priority_score
+
coverage_confidence_score
+
data_quality_flag
```

### 9.3 CDD Validation

CDD is used to validate grid-level and municipality-level prioritization.

Workflow:

```text
Build CDD event-month labels
|
v
Map CDD events to municipality/grid where possible
|
v
Calculate candidate grid/month scores
|
v
Aggregate grid score to municipality-month
|
v
Run out-of-time backtests
|
v
Calculate lift, event capture, PR-AUC, rank stability
|
v
Publish validation marts
|
v
Show validation page in Power BI and public site
```

Honest gate:

> If the calibrated score does not achieve meaningful lift over random and equal-weight baselines, report it as a weak exploratory prioritization heuristic instead of overselling it.

---

## 10. Data Quality and Observability

Quality is a first-class product output.

### 10.1 Source / Ingestion Quality

Tables / outputs:

```text
audit_extract_run
audit_source_freshness
audit_row_count_anomaly
audit_schema_hash
bronze_runs.jsonl
extract_audit.json
```

Checks:

- source freshness by expected frequency;
- row count anomaly vs rolling median;
- schema hash drift;
- new/missing columns;
- file checksum;
- extract status;
- retry count;
- failure reason;
- Socrata row count reconciliation where available.

### 10.2 Geospatial Quality

Tables / outputs:

```text
audit_coordinate_validation
audit_geometry_validation
audit_crs_transform
audit_spatial_join
mart_spatial_coverage_confidence
```

### 10.3 dbt / Warehouse Quality

Generic dbt tests:

- `not_null`
- `unique`
- `relationships`
- `accepted_values`

Custom dbt tests:

- score between 0 and 100;
- priority tier in allowed values;
- valid month key;
- valid spatial unit key;
- no unmapped high-priority grid cells;
- no missing coverage confidence;
- no negative permit values;
- no negative assessment values;
- no property exposure row without source lineage.

### 10.4 Pipeline Status JSON

Write:

```text
public_site/pipeline_status.json
```

---

## 11. Architecture

```text
External Sources
  ├── ECCC Historical Climate
  ├── ECCC Hydrometric Real-Time
  ├── HYDAT SQLite
  ├── CWFIS / CNFDB Wildfire
  ├── StatCan Building Permits
  ├── Census / CSD / Province Boundaries
  ├── Canadian Disaster Database
  ├── Vancouver Property / Permit / Floodplain Data
  └── Calgary Property / Permit / Flood Data

        ↓ Airflow ingestion DAGs
        ↓ source snapshot metadata
        ↓ schema hash / row-count checks

AWS S3 Bronze
  ├── raw source snapshots
  ├── source-preserving formats
  ├── file checksums
  └── extract audit metadata

        ↓ PySpark + Apache Sedona
        ↓ CRS standardization
        ↓ geometry validation
        ↓ grid generation
        ↓ spatial joins
        ↓ source profiling and silver quality checks

AWS S3 Silver
  ├── standardized tabular sources
  ├── standardized geospatial sources
  ├── generated grids
  ├── station-grid maps
  ├── hazard-grid features
  ├── property-flood overlays
  └── spatial coverage confidence

        ↓ Snowflake load / external staging
        ↓ dbt Core
        ↓ dbt tests
        ↓ score calibration and validation jobs

Snowflake Gold / Audit
  ├── dimensions
  ├── grid marts
  ├── Vancouver parcel mart
  ├── Calgary property flood mart
  ├── municipality aggregation mart
  ├── validation marts
  ├── sensitivity marts
  └── data reliability mart

        ↓ Power BI import / curated extracts
        ↓ public static front-end

Public Portfolio Evidence
  ├── Power BI dashboard
  ├── GitHub Pages project page
  ├── pipeline_status.json
  ├── architecture diagram
  ├── data dictionary
  ├── validation metrics
  ├── screenshots
  └── demo video fallback
```

---

## 12. Airflow DAGs

The target DAG set:

```text
historical_backfill_pipeline
daily_hazard_pipeline
monthly_exposure_pipeline
municipal_property_deep_dive_pipeline
score_validation_pipeline
data_quality_monitoring
```

Each DAG should produce audit outputs and update `pipeline_status.json` when relevant.

---

## 13. dbt Modeling Design

### 13.1 dbt Model Layers

```text
staging
|
v
intermediate
|
v
marts
```

### 13.2 Example Models

Staging:

```text
stg_climate_daily
stg_hydro_daily
stg_wildfire_event
stg_vancouver_parcel
stg_calgary_property
stg_cdd_events
```

Intermediate:

```text
int_grid_climate_monthly
int_grid_hydro_monthly
int_grid_wildfire_monthly
int_vancouver_parcel_overlay
int_calgary_property_flood_overlay
int_cdd_event_month_labels
```

Marts:

```text
mart_grid_month_hazard_exposure
mart_grid_month_priority
mart_vancouver_parcel_exposure
mart_calgary_property_flood_exposure
mart_municipality_month_priority
mart_score_validation
mart_sensitivity_analysis
mart_data_reliability
```

---

## 14. Public Front-End and Power BI Display

### 14.1 Public Front-End Role

The front-end is a static project evidence layer, not the main engineering product.

Target:

```text
GitHub Pages / static public_site
```

### 14.2 Front-End Sections

| Section | Purpose |
|---|---|
| Hero / Project Pitch | Explain the project in one screen |
| Architecture | Show AWS S3 + Snowflake ELT architecture |
| Pipeline Status | Read `pipeline_status.json` and show freshness/quality cards |
| Data Sources | Summarize sources and ingestion status |
| Data Quality | Show row count, schema, CRS, spatial join quality |
| Dashboard Preview | Embed Power BI if available; otherwise show screenshots |
| Validation | Show CDD lift/top-K/sensitivity summary |
| Limitations | Explain exposure-screening limits honestly |
| Links | GitHub repo, docs, PBIX/demo video, screenshots |

### 14.3 Public Site Files

```text
public_site/
  index.html
  pipeline_status.json
  assets/
    architecture.png
    dashboard_overview.png
    grid_hazard_page.png
    vancouver_parcel_page.png
    calgary_property_page.png
    validation_page.png
    data_reliability_page.png
```

### 14.4 Power BI Dashboard Pages

| Page | Purpose |
|---|---|
| Executive Overview | BC + Alberta grid priority map, top areas, score distribution |
| Grid Hazard Explorer | 10km / 1km grid hazard components and coverage confidence |
| Flood & Hydrometric Monitoring | Water level percentiles, station coverage, hydro anomalies |
| Wildfire Exposure | Wildfire proximity, counts, seasonal patterns |
| Development Exposure | Building-permit trends and development exposure proxy |
| Vancouver Parcel Deep Dive | Parcel exposure summary, floodplain overlap, permit activity |
| Calgary Flood-Property Deep Dive | Property flood zone exposure, assessment value proxy, permit activity |
| Score Validation | CDD backtesting, lift, top-K capture, sensitivity analysis |
| Data Reliability | Freshness, schema drift, row count anomalies, dbt tests, spatial join success |

### 14.5 Front-End Acceptance Rule

The public front-end must make the DE value visible quickly:

```text
title
|
v
one-sentence pitch
|
v
architecture
|
v
pipeline status
|
v
dashboard screenshots/embed
|
v
data-quality evidence
|
v
limitations
```

---

## 15. Deployment and Cost Control

| Component | Choice | Reason |
|---|---|---|
| Storage | AWS S3 | Low-cost data lake storage |
| Warehouse | Snowflake | Modern analytical warehouse |
| Compute | Local Docker / optional EC2 batch compute | Cheaper than always-on managed compute |
| Processing | PySpark / Apache Sedona | Scalable processing story |
| BI | Power BI Desktop + Service / Publish to Web | Stable public demo |
| Public Site | GitHub Pages | Free/low-cost project wrapper |
| CI/CD | GitHub Actions | Public repo automation |
| IaC | Terraform | Reproducible AWS/Snowflake setup |

Cost-control decisions:

- use local-first development;
- use small Snowflake warehouse;
- avoid always-on managed clusters;
- use S3 lifecycle rules for temp files;
- use curated marts and dashboard extracts;
- provide screenshots/video fallback.

---

## 16. Repository Structure

```text
canadian-climate-risk-platform/
│
├── README.md
├── PLAN_FINAL.md
├── docker-compose.yml
├── Makefile
├── .env.example
│
├── configs/
│   ├── project_scope.yml
│   ├── source_config.yml
│   ├── spatial_config.yml
│   ├── risk_score_config.yml
│   ├── dq_thresholds.yml
│   └── platform_config.yml
│
├── airflow/
├── spark_jobs/
├── src/
│   ├── ingestion/
│   ├── audit/
│   ├── profiling/
│   ├── validation/
│   ├── geospatial/
│   ├── scoring/
│   └── utils/
│
├── dbt/
│   ├── models/
│   ├── profiles/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
├── infra/
│   └── terraform/
│       ├── aws/
│       └── snowflake/
│
├── dashboard/
├── public_site/
├── docs/
├── tests/
└── .github/workflows/
```

---

## 17. Implementation Sequence and Branch Plan

### Completed / Current Foundation

```text
feature/00-project-realignment
|
v
feature/01-local-dev-env
|
v
feature/02-config-source-registry
|
v
feature/03-bronze-national-ingestion
|
v
feature/04-bronze-municipal-ingestion
|
v
feature/05-audit-framework
|
v
feature/06-aws-snowflake-realignment
```

### Next Branches

#### `feature/07-source-profiling`

```text
Read latest Bronze manifest
|
v
Profile CSV / XLSX / JSON / GeoJSON files
|
v
Detect raw columns and sample values
|
v
Check config contracts against real raw schema
|
v
Write profile JSON and markdown summary
|
v
Use results to finalize source_config.yml
```

#### `feature/08-s3-storage-backend`

```text
Introduce storage backend abstraction
|
v
LocalStorageBackend
|
v
S3StorageBackend
|
v
Write Bronze metadata and manifests to local or S3
|
v
Keep local mode fully runnable
```

#### `feature/09-silver-core-sources`

```text
Standardize climate, hydro, wildfire, permit, CDD, and boundary sources
```

#### `feature/10-silver-geospatial-grids`

```text
Generate 10km BC/AB grid and 1km Vancouver/Calgary grids
```

#### `feature/11-grid-feature-engineering`

```text
Build grid climate, hydro, wildfire, development, disaster, and confidence features
```

#### `feature/12-property-overlays`

```text
Build Vancouver parcel-floodplain overlay and Calgary property-flood overlay
```

#### `feature/13-snowflake-load-dbt-marts`

```text
Load curated outputs to Snowflake and build dbt marts
```

#### `feature/14-validation-scoring`

```text
Backtest scores against CDD events and publish validation marts
```

#### `feature/15-airflow-dags`

```text
Build production-style DAG orchestration
```

#### `feature/16-dashboard-public-site`

```text
Build Power BI report and static public project page
```

#### `feature/17-docs-polish`

```text
Finalize docs, limitations, screenshots, and interview story
```

---

## 18. Testing Strategy

### 18.1 Unit Tests

Test:

- config loading;
- source registry validation;
- source contract validation;
- source profiling;
- schema hash generation;
- row count anomaly logic;
- CRS conversion helper;
- grid ID generation;
- scoring functions;
- validation metrics.

### 18.2 Integration Tests

Test:

- Bronze write/read;
- local vs S3 storage backend behavior;
- Silver transformation on fixtures;
- geometry validation on sample polygons;
- spatial join on small known geometries;
- Snowflake load on sample data or mocked interface;
- dbt model build on sample profile;
- pipeline status JSON generation.

### 18.3 CI/CD Checks

```text
Python lint
|
v
Unit tests
|
v
dbt parse / compile
|
v
Docker build check
|
v
Docs link / markdown check
```

---

## 19. Final Deliverables

The project is complete only when all items below exist.

1. GitHub repository with final structure.
2. `PLAN_FINAL.md` in repo root.
3. Docker Compose local environment.
4. Terraform AWS and Snowflake placeholders.
5. Airflow DAGs for main pipelines.
6. AWS S3 Bronze/Silver/Audit target design.
7. PySpark/Sedona jobs for grid, hazard, and property overlays.
8. Source registry and config-driven ingestion.
9. Source profiling outputs.
10. Source audit outputs.
11. Geospatial audit outputs.
12. Snowflake schemas and dbt models.
13. dbt staging, intermediate, and mart models.
14. dbt tests and custom tests.
15. Data reliability mart.
16. Score validation and sensitivity marts.
17. Power BI dashboard with reliability and validation pages.
18. GitHub Pages static public site.
19. `pipeline_status.json`.
20. Architecture diagram.
21. Data dictionary.
22. Data source documentation.
23. CRS strategy document.
24. Data quality document.
25. Score validation document.
26. Limitations document.
27. Demo video fallback.
28. Final README.
29. Resume bullets and interview story.

---

## 20. Acceptance Criteria

### 20.1 DE Acceptance Criteria

The DE platform is acceptable only if:

- raw data is preserved in Bronze;
- Silver tables are reproducible from Bronze;
- grids are generated programmatically;
- CRS transforms are documented and audited;
- spatial joins write audit records;
- schema drift is detectable;
- row count anomalies are detectable;
- dbt tests run successfully;
- Snowflake marts are buildable;
- Power BI uses curated marts or curated extracts;
- `pipeline_status.json` reflects latest run health.

### 20.2 Geospatial Acceptance Criteria

The geospatial layer is acceptable only if:

- 10km BC/AB grid exists;
- 1km Vancouver/Calgary grids exist;
- geometry validity is checked;
- invalid geometry handling is logged;
- all distance/area calculations use projected CRS;
- Vancouver parcel-floodplain overlay exists;
- Calgary property-flood overlay exists;
- spatial join success rates are reported.

### 20.3 Public Demo Acceptance Criteria

The demo is acceptable only if it contains:

- Power BI dashboard or screenshots/video fallback;
- static public front-end landing page;
- architecture section;
- source status section;
- data-quality section;
- validation section;
- limitations section;
- repo/docs links.

---

## 21. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Municipal schema changes | Pipeline failure | schema hash, source configs, Bronze preservation, source profiling |
| Source fields are unknown | Wrong Silver logic | profile real raw files before Silver implementation |
| Property data joins are incomplete | Weak parcel/property mart | join quality flags, unmatched audit, honest limitations |
| Power BI public embed unavailable | No live public dashboard | screenshots + demo video fallback + PBIX in repo |
| Geospatial joins are slow | Long runtime | Sedona for large joins, GeoPandas only for small city layers |
| Station coverage is sparse in northern regions | Weak signal | coverage confidence score surfaced in dashboard |
| Score has weak validation lift | Methodology questioned | honest gate: report as exploratory heuristic |
| Project scope becomes too front-end heavy | DE story diluted | static front-end only, no full WebGIS app |
| Cloud cost grows | Budget issue | local-first dev, S3 low-cost storage, small Snowflake warehouse |

---

## 22. README First Screen Requirements

Required first screen:

```text
Project title
|
v
One-sentence DE pitch
|
v
Current status
|
v
Architecture badges: AWS S3 + Snowflake + PySpark + Airflow + dbt + Power BI
|
v
Scope: BC + Alberta, 10km grid, Vancouver/Calgary property deep dives
|
v
Data quality: freshness, schema hash, CRS validation, spatial join audit, dbt tests
|
v
Validation: CDD backtesting, lift/top-K/sensitivity
|
v
Demo: Power BI / screenshots / public front-end
|
v
Limitations: exposure screening, not insurance/legal/engineering risk model
```

---

## 23. Resume Bullets

### Bullet 1 — Core DE Platform

> Built an AWS S3 + Snowflake ELT data platform integrating Canadian climate, hydrometric, wildfire, building-permit, disaster-event, and municipal property data into Bronze/Silver/Gold layers with Airflow-orchestrated ingestion and dbt-modeled analytical marts.

### Bullet 2 — Geospatial DE + Quality

> Engineered PySpark/Sedona geospatial transformations to generate 10km BC/Alberta risk grids, 1km city grids, and Vancouver/Calgary property-flood overlays, with CRS standardization, geometry validation, schema-drift detection, row-count checks, source profiling, and spatial join audit outputs.

### Bullet 3 — Serving + Validation

> Modeled trusted Snowflake/dbt marts for grid-level hazard exposure, property-level flood screening, data reliability, and score validation; backtested prioritization scores against Canadian Disaster Database events and surfaced results through Power BI and a static public project page.

---

## 24. Interview Positioning

### 24.1 One-Sentence Pitch

> I built a DE-focused AWS S3 + Snowflake geospatial ELT platform that ingests public Canadian climate, hydrometric, wildfire, permit, disaster, and municipal property datasets, validates them across Bronze/Silver/Gold layers, and serves trusted grid-level and property-context exposure marts to Power BI and a public project page.

### 24.2 Why This Is Data Engineering

This is DE because the project focuses on:

- reliable ingestion from heterogeneous sources;
- raw data preservation;
- source profiling;
- schema drift detection;
- row-count anomaly detection;
- PySpark/Sedona spatial transformations;
- CRS standardization;
- geometry validation;
- repeatable grid generation;
- spatial join auditability;
- dbt modeling and testing;
- Snowflake warehouse modeling;
- Airflow orchestration;
- pipeline observability;
- public data freshness reporting.

### 24.3 Answer: “Why Not Just Power BI?”

> Power BI is only the presentation layer. The hard part is making heterogeneous public data reliable enough to feed the dashboard: ingestion, schema drift detection, source profiling, CRS standardization, spatial joins, coverage confidence, dbt tests, and validation against external disaster events. The dashboard proves the data product is consumable; it is not the core engineering work.

### 24.4 Answer: “Is This a Risk Model?”

> No. I call it an exposure screening and prioritization heuristic. I do not have claims data, insured asset values, engineering flood-depth models, or legal parcel assessment authority. The platform prioritizes areas based on public hazard, exposure, and validation signals, and it clearly reports coverage confidence and limitations.

### 24.5 Answer: “Why Vancouver and Calgary?”

> Vancouver and Calgary are not random add-ons. Vancouver supports a parcel-centric deep dive with parcel, tax, permit, and floodplain data. Calgary supports a flood-centric property deep dive with property assessment, flood hazard, and permit data. They let the platform show city-level property-context engineering without pretending to build full-Canada property risk modeling.

---

## 25. Explicit Non-Goals

The final project does not build:

- Next.js / React production front end;
- MapLibre dashboard;
- deck.gl visualization app;
- FastAPI geospatial API;
- vector tile server;
- PMTiles delivery pipeline;
- insurance risk model;
- property value prediction model;
- flood-depth simulation model;
- address search UI;
- real-time streaming architecture;
- full-Canada property-level platform.

These are excluded because they either dilute the DE story, increase completion risk, or require data that is not available publicly.

---

## 26. Final Definition

This final plan defines the project as:

> A DE-focused AWS S3 + Snowflake geospatial ELT platform for British Columbia and Alberta that produces reliable grid-level and city property-context exposure marts, validates prioritization scores against historical disaster events, tracks data quality across the full pipeline, and delivers stable Power BI and public-site evidence backed by Snowflake/dbt analytical marts.

The project’s value is not that it has the flashiest map.

The value is that the data behind the map is:

```text
ingested
|
v
preserved
|
v
profiled
|
v
validated
|
v
standardized
|
v
spatially engineered
|
v
modeled
|
v
served
|
v
monitored
|
v
documented
```

That is the Data Engineering story.
