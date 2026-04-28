# PLAN_FINAL.md
# Canadian Climate & Property Risk Data Platform — Final DE Project Plan

## 0. Final Project Identity

### Project Name

**Canadian Climate & Property Risk Data Platform**

### Final One-Sentence Definition

Build a production-style **Azure + Spark data engineering platform** that ingests, validates, transforms, models, and serves Canadian climate, hydrometric, wildfire, building-permit, disaster-event, floodplain, and municipal property datasets into trusted **grid-level and property-context geospatial exposure marts** for British Columbia and Alberta, with a stable Power BI dashboard and public project page as downstream evidence.

### Core Positioning

This is a **Data Engineering project first**.

The project is not mainly a dashboard project, not a WebGIS front-end project, and not a climate-science model. The core value is that heterogeneous public data flows reliably through a lakehouse pipeline into curated analytical marts with data quality, lineage, validation, and monitoring.

### What the Project Claims

The project claims to build:

- a reproducible cloud data platform;
- a lakehouse Bronze/Silver/Gold data flow;
- a geospatial feature-engineering pipeline;
- grid-level climate and hazard exposure marts;
- Vancouver parcel-level exposure screening marts;
- Calgary property flood exposure screening marts;
- municipality-level aggregation and validation marts;
- public BI and data-quality evidence;
- a documented, honest exposure-prioritization methodology.

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

The correct wording is:

> This project provides public-data-based climate, hazard, and property-context exposure screening. It is not an insurance-grade, engineering-grade, legal, or property appraisal risk model.

---

## 1. Business Storyline

### 1.1 Business Context

Western Canada faces overlapping climate and infrastructure risks:

- British Columbia has wildfire, flood, coastal, mountainous, and development-pressure exposure.
- Alberta has flood, hydrometric, wildfire, urban expansion, and property-level flood planning exposure.
- Public datasets exist, but they are fragmented across federal, provincial, and municipal portals.
- These datasets arrive in different formats, spatial grains, frequencies, coordinate systems, and quality levels.

The business problem is not that one more map is needed. The real problem is that planners, analysts, and decision makers need **trusted, refreshable, comparable, and validated geospatial exposure data products**.

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
6. Which parts of the output are reliable, and which parts have weak station coverage, failed joins, stale sources, or geometry issues?

### 1.5 Business Narrative for Portfolio

The project story should be told in this order:

```text
Public climate and hazard data is valuable but fragmented.
|
v
I built a cloud lakehouse pipeline that ingests and preserves raw data.
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
I modeled curated dbt/PostgreSQL/PostGIS marts for downstream BI and analysis.
|
v
I tracked data quality, freshness, schema drift, CRS transforms, and spatial join success.
|
v
I validated prioritization scores against Canadian Disaster Database events.
|
v
I surfaced both results and reliability in a public Power BI dashboard and GitHub Pages project page.
```

### 1.6 Recruiter-Friendly Summary

> Built a DE-focused Azure/Spark geospatial lakehouse that integrates Canadian climate, hydrometric, wildfire, permit, disaster, and municipal property data into trusted grid-level and property-level exposure marts, with Airflow orchestration, Delta Lake storage, dbt/PostGIS serving, data-quality monitoring, and Power BI public reporting.

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
| Climate / hydrometric / wildfire / CDD | 2016-2025 | Historical baseline, percentiles, anomalies, backtesting |
| Current hazard signals | 2026 incremental refresh | Current monitoring layer |
| Vancouver permits | 2017-present where available | Parcel development exposure |
| Vancouver property tax | Current + available historical extracts | Parcel exposure proxy |
| Calgary assessment / permits / flood data | Current + available historical extracts | Property flood exposure screening |

The 2016-2025 baseline is long enough to support seasonal baselines, percentile ranking, and out-of-time validation without turning the project into climate science research.

### 2.3 Final Spatial Scale

This project uses **multi-scale spatial units** because public data arrives at different spatial resolutions.

| Layer | Spatial Unit | Area | Purpose | Status |
|---|---|---|---|---|
| Province-wide grid | 10km x 10km metric grid | Full BC + Alberta | Main exposure surface | Core |
| City grid | 1km x 1km metric grid | Vancouver + Calgary city boundaries | Higher-resolution urban comparison | Core |
| Vancouver parcel | Property parcel polygon | City of Vancouver | Parcel-level exposure screening | Core |
| Calgary property / parcel | Property geometry or centroid depending on available fields | City of Calgary | Property flood exposure screening | Core |
| Municipality / CSD | Census subdivision / municipality polygon | BC + Alberta | Aggregation, comparison, validation, BI slicers | Core support |

### 2.4 Why This Scale Is the Final Choice

The final scale is:

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
- 1km city grids prove finer spatial feature engineering without full property overload.
- Vancouver parcel layer uses real parcel polygons where city data supports it.
- Calgary property flood layer uses strong flood/property municipal data.
- Municipality layer keeps the output understandable for BI, validation, and interview storytelling.
- The scope remains Data Engineering focused rather than becoming a full-stack WebGIS product.

---

## 3. Final Technical Stack

| Layer | Technology | Final Decision | Why It Exists |
|---|---|---|---|
| Cloud | Azure | Primary cloud platform | Enterprise cloud relevance and Canadian-market familiarity |
| Object Storage | ADLS Gen2 + HNS | Lakehouse storage | Durable raw/processed/gold storage |
| Lake Format | Delta Lake | Bronze/Silver/Gold tables | ACID, schema evolution, reproducible backfills |
| Processing | PySpark | Core transformation engine | Historical backfill, feature engineering, aggregation |
| Geospatial Processing | Apache Sedona + GeoPandas fallback | Distributed spatial joins and local fallback | High-value geospatial DE signal |
| Orchestration | Apache Airflow | DAGs, retry, backfill, audit | Production-style pipeline control |
| Transformation | dbt Core | Staging, intermediate, marts, tests | Analytics engineering and lineage |
| Serving | Azure PostgreSQL Flexible Server + PostGIS | BI and geospatial marts | Geometry columns, spatial indexes, downstream serving |
| BI / Demo | Power BI Desktop + Power BI Service / Publish to Web | Stable public dashboard | Reliable portfolio demonstration |
| Public Site | GitHub Pages or Azure Static Web Apps | Landing page, status JSON, documentation | Public proof of work |
| IaC | Terraform | Azure infrastructure | Reproducible cloud setup |
| Containerization | Docker Compose | Local/dev Airflow + Spark + dbt + Postgres | Reproducible development |
| CI/CD | GitHub Actions | lint, unit tests, dbt compile/test, Docker build | Engineering workflow proof |
| Quality | Source audit + schema hash + row count + geometry + CRS + dbt tests | Full-chain quality | Main DE differentiator |

### 3.1 Display-Layer Decision

Final dashboard tool:

```text
Power BI
```

Reason:

- The project is a DE portfolio project, not a front-end portfolio project.
- Power BI gives a stable public demo.
- Recruiters can click and understand the result quickly.
- Dashboard failure risk is lower than a custom WebGIS application.
- The dashboard is downstream evidence, not the source of truth.

Removed from final scope:

- Next.js
- React front-end
- MapLibre GL JS application
- deck.gl layer development
- FastAPI tile server
- pg_tileserv deployment
- PMTiles as required delivery
- Azure Container Apps for custom dashboard hosting

### 3.2 Source of Truth

The source of truth is:

```text
Curated marts + audit tables + data-quality outputs
```

Not:

```text
Dashboard visuals
```

Power BI is only a consumer of the data product.

---

## 4. Data Sources

### 4.1 National / Provincial Sources

| Source | Role | Ingestion Approach | Frequency | Final Use |
|---|---|---|---|---|
| ECCC Historical Climate | Temperature, precipitation, historical weather baseline | Bulk CSV / Datamart-style extraction | Historical + periodic refresh | Weather extreme score |
| ECCC Hydrometric Real-Time | Recent water level / discharge | REST / real-time extract | Daily or near-real-time | Current flood / water signal |
| HYDAT | Historical water level / discharge baseline | SQLite bulk download | Quarterly or periodic | Seasonal hydrometric percentile baseline |
| CWFIS / CNFDB Wildfire | Wildfire point history and proximity | GeoJSON / CSV / bulk download, points first | Seasonal / periodic | Wildfire exposure score |
| StatCan Building Permits | Development exposure proxy | Bulk CSV first; API later only if needed | Monthly | Municipality/grid development exposure |
| Census / CSD Boundaries | Administrative mapping | Boundary file download | Periodic | Municipality aggregation and validation |
| Province Boundaries | BC/AB clipping | Boundary file download | Static / periodic | Grid generation and clipping |
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
| Calgary current year property assessments parcel | Current property assessment and property context | Normalize assessment year, value fields, property type, geometry/centroid | `mart_calgary_property_flood_exposure` |
| Calgary historical property assessments parcel | Assessment baseline and schema drift testing | Normalize years and field changes | Historical/context quality |
| Calgary regulatory flood map / flood hazard layers | Floodway, flood fringe, overland flood zones | Overlay with property geometry or centroid; derive flood category flags | Flood exposure score |
| Calgary development permits | Development activity | Normalize permit type, status, date, spatial fields | Development feature |
| Calgary building permits | Building activity | Normalize date/type/status and spatial fields | Building activity feature |
| Calgary city / community boundaries | Scope and rollup boundaries | Validate geometry and CRS | City grid and aggregation |

### 4.4 Data Inclusion Rules

A dataset enters the project only if it satisfies at least four of the following six criteria:

1. Publicly accessible or reproducibly downloadable.
2. Relevant to climate, flood, wildfire, development exposure, property context, or validation.
3. Contains time, geography, or stable join keys.
4. Can be ingested into Bronze without manual cleaning.
5. Can be validated by schema, row count, coordinate, geometry, or domain checks.
6. Can contribute to a downstream mart, score, validation, or data-quality metric.

A dataset is excluded if:

1. It cannot be legally or publicly redistributed.
2. It contains private or sensitive personal information.
3. It requires manual point-and-click collection for every run.
4. It creates false precision that cannot be explained.
5. It cannot be mapped to a stable spatial unit.
6. It adds visual appeal but no pipeline or data-model value.

---

## 5. Spatial Engineering Design

### 5.1 CRS Strategy

| Stage | CRS | Rule |
|---|---|---|
| Raw ingestion | Preserve source CRS | Record original CRS metadata |
| Standard processing | EPSG:3347 Canada Lambert | Use for area and distance calculations |
| City processing | EPSG:3347 by default | Use local projected CRS only if source requires it |
| Serving / BI | EPSG:4326 centroids + WKT/geometry | BI compatibility |
| Audit | Store source CRS, processed CRS, transform status | Prevent silent CRS errors |

Hard rules:

- Never compute area or distance directly on latitude/longitude degrees.
- All spatial joins must write an audit record.
- All geometry repair operations must be counted and logged.
- Store centroids for BI and geometry for PostGIS where useful.

### 5.2 Grid Generation

#### 10km BC/AB Grid

Purpose:

- province-level exposure surface;
- main grid risk mart;
- Power BI map layer;
- CDD validation aggregation;
- stable spatial contract for multiple sources.

Generation logic:

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

Purpose:

- city-level comparison;
- fine-grained urban hazard/development features;
- bridge between province grid and property deep dives.

Generation logic:

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

### 5.3 Spatial Join Types

| Join Type | Example | DE Value |
|---|---|---|
| Point-to-grid | weather station to 10km grid | station coverage and nearest station logic |
| Point-to-polygon | wildfire points to grid/city/municipality | proximity and count features |
| Polygon-to-grid | floodplain or boundary to grid | area overlap and coverage features |
| Polygon-to-polygon | Vancouver parcel to floodplain | high-value geospatial engineering |
| Property-to-flood-zone | Calgary property to regulatory flood zone | property flood exposure feature |
| Grid-to-municipality | aggregate grid results to CSD | validation and BI summary |

### 5.4 Spatial Join Audit

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

## 6. Lakehouse Layer Design

### 6.1 Bronze Layer

Purpose:

- preserve raw source data;
- support reproducible reprocessing;
- detect source drift;
- provide auditability.

Bronze tables:

```text
bronze_climate
bronze_hydrometric_realtime
bronze_hydat
bronze_wildfire
bronze_statcan_building_permits
bronze_boundaries
bronze_disaster_events
bronze_vancouver_property_parcels
bronze_vancouver_property_tax
bronze_vancouver_building_permits
bronze_vancouver_floodplain
bronze_calgary_property_assessment
bronze_calgary_flood_hazard
bronze_calgary_building_permits
bronze_calgary_development_permits
```

Bronze metadata fields:

```text
run_id
source_name
source_url
extract_timestamp
raw_file_path
file_checksum
schema_hash
ingestion_method
source_period_start
source_period_end
row_count
load_status
```

### 6.2 Silver Layer

Purpose:

- standardize schemas;
- normalize dates and keys;
- validate coordinates and geometries;
- standardize CRS;
- generate grids;
- build reusable feature inputs.

Silver tables:

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
silver_vancouver_parcel_flood_overlay
silver_vancouver_permit_property_map
silver_calgary_property
silver_calgary_flood_hazard
silver_calgary_property_flood_overlay
silver_calgary_permit_property_map
silver_spatial_coverage_confidence
```

### 6.3 Gold / Serving Layer

Purpose:

- provide stable downstream contracts;
- support Power BI and SQL consumers;
- preserve geometry where useful;
- serve validation and reliability outputs.

Technology:

```text
Azure PostgreSQL Flexible Server + PostGIS
```

Gold tables are built through dbt and exported/loaded into PostgreSQL/PostGIS.

---

## 7. Final Data Products / Marts

### 7.1 Core Dimensions

| Model | Grain | Purpose |
|---|---|---|
| `dim_date` | one row per date | Shared date dimension |
| `dim_month` | one row per month | Monthly aggregation |
| `dim_source` | one row per source | Source lineage and freshness |
| `dim_spatial_unit` | one row per spatial unit | Unified grid/parcel/property/municipality dimension |
| `dim_municipality` | one row per municipality/CSD | Administrative rollup |
| `dim_station` | one row per climate/hydro station | Station metadata and coverage |

### 7.2 Grid-Level Marts

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

### 7.3 Vancouver Parcel Mart

#### `mart_vancouver_parcel_exposure`

Grain:

```text
one row per Vancouver parcel per assessment/reporting year
```

Main columns:

```text
parcel_id
property_tax_year
centroid_lat
centroid_lon
geometry_wkt
land_value
improvement_value
total_value
permit_count_since_2017
permit_count_last_3y
latest_permit_date
floodplain_flag
floodplain_overlap_pct
building_or_improvement_proxy
vancouver_parcel_exposure_score
exposure_tier
join_quality_flag
geometry_valid_flag
source_freshness_flag
```

### 7.4 Calgary Property Flood Mart

#### `mart_calgary_property_flood_exposure`

Grain:

```text
one row per Calgary property / parcel per assessment year
```

Main columns:

```text
property_id
assessment_year
centroid_lat
centroid_lon
geometry_wkt_or_null
property_type
assessed_value
land_value
improvement_value
regulatory_flood_zone_flag
floodway_flag
flood_fringe_flag
overland_flood_flag
flood_zone_category
building_permit_count_last_3y
development_permit_count_last_3y
calgary_flood_exposure_score
exposure_tier
join_quality_flag
geometry_valid_flag
source_freshness_flag
```

### 7.5 Municipality Aggregation Mart

#### `mart_municipality_month_priority`

Grain:

```text
one row per municipality per month
```

Purpose:

- recruiter-friendly summary;
- CDD validation layer;
- Power BI slicers and top-N views;
- bridge between grid/property outputs and administrative boundaries.

Main columns:

```text
municipality_key
province
month_key
grid_cell_count
avg_grid_priority_score
p90_grid_priority_score
high_priority_grid_count
high_priority_grid_share
permit_value_monthly
CDD_event_count
coverage_confidence_score
municipality_priority_tier
```

### 7.6 Data Reliability Mart

#### `mart_data_reliability`

Grain:

```text
one row per source per pipeline run
```

Main columns:

```text
run_id
source_name
expected_frequency
last_extract_at
freshness_status
row_count
row_count_change_pct
schema_changed_flag
geometry_valid_rate
spatial_join_success_rate
dbt_tests_passed
dbt_tests_failed
severity
```

---

## 8. Score and Validation Design

### 8.1 Score Naming

Do not use one generic “risk score.” Use named scores by grain and purpose.

| Score | Grain | Purpose |
|---|---|---|
| Grid-Level Climate-Hazard Exposure Priority Score | grid-month | Prioritize grid cells for monitoring |
| Vancouver Parcel Exposure Screening Score | parcel-year | Screen parcel exposure based on floodplain, property-value proxy, and permit activity |
| Calgary Property Flood Exposure Screening Score | property-year | Screen property flood exposure based on regulatory flood layers and property context |
| Municipality Climate-Exposure Priority Tier | municipality-month | Aggregated BI and validation summary |

### 8.2 Grid-Level Score

Sub-scores:

1. Flood / hydrometric signal score
2. Wildfire exposure score
3. Extreme weather score
4. Development exposure score
5. Coverage confidence, reported separately

Candidate methods:

| Method | Purpose |
|---|---|
| Equal-weight baseline | Honest baseline |
| Hazard-only baseline | Tests whether development exposure adds signal |
| Validation-calibrated score | Uses CDD labels to calibrate or select weights |

Important rule:

```text
Coverage confidence is not hidden inside the risk score.
It is reported beside the score so users can judge reliability.
```

### 8.3 Vancouver Parcel Exposure Score

Conceptual formula:

```text
vancouver_parcel_exposure_score =
  floodplain_overlap_component
+ property_value_exposure_component
+ permit_activity_component
+ data_quality_adjustment
```

Interpretation:

- Higher overlap with floodplain increases exposure.
- Higher property-value proxy increases asset exposure context.
- Recent permit activity increases development/activity exposure context.
- Data quality issues lower confidence or flag the row.

### 8.4 Calgary Property Flood Exposure Score

Conceptual formula:

```text
calgary_flood_exposure_score =
  regulatory_flood_zone_component
+ assessed_value_exposure_component
+ building_or_development_activity_component
+ data_quality_adjustment
```

Interpretation:

- Flood zone presence is the dominant factor.
- Assessed value is used as public asset-context proxy, not market valuation.
- Building/development activity indicates recent property activity.
- Data quality flags are mandatory.

### 8.5 CDD Validation

CDD is used to validate grid-level and municipality-level prioritization.

It is not used as property-level truth.

Validation workflow:

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
Show validation page in Power BI
```

Validation metrics:

| Metric | Purpose |
|---|---|
| Event Capture @ Top 10% | Whether high-priority areas capture disaster events |
| Lift @ Top 10% | Improvement over random ranking |
| Precision @ Top K | Share of selected areas with event labels |
| Recall @ Top K | Share of all events captured |
| PR-AUC | Rare-event ranking quality |
| Spearman rank stability | Stability of rankings over time |
| Jaccard Top-K stability | Stability of high-priority shortlist |
| Weight sensitivity | Robustness to weighting assumptions |

Honest gate:

> If the calibrated score does not achieve meaningful lift over random and equal-weight baselines, report it as a weak exploratory prioritization heuristic instead of overselling it.

---

## 9. Data Quality and Observability

Quality is a first-class product output, not background code.

### 9.1 Source / Ingestion Quality

Tables:

```text
audit_extract_run
audit_source_freshness
audit_row_count_anomaly
audit_schema_hash
```

Checks:

- source freshness by expected frequency;
- row count anomaly vs rolling median;
- schema hash drift;
- new/missing columns;
- file checksum;
- extract status;
- retry count;
- failure reason.

### 9.2 Geospatial Quality

Tables:

```text
audit_coordinate_validation
audit_geometry_validation
audit_crs_transform
audit_spatial_join
mart_spatial_coverage_confidence
```

Checks:

- valid latitude/longitude range;
- coordinates within expected province/city bounding box;
- geometry validity;
- geometry repair count;
- CRS detected vs expected;
- CRS transform success rate;
- spatial join success rate;
- failed join count by source and join type;
- median/p95 station distance by grid or municipality;
- parcel-to-floodplain overlay success rate.

### 9.3 Silver Transformation Quality

Checks:

- natural key uniqueness;
- date range validity;
- non-negative precipitation;
- non-negative permit value;
- non-negative assessed value;
- numeric hydrometric values;
- valid wildfire geometry;
- valid floodplain geometry;
- valid parcel geometry;
- partition completeness;
- duplicate rate threshold.

### 9.4 Gold / dbt Quality

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

### 9.5 Pipeline Status JSON

After each successful pipeline run, write:

```text
public_site/pipeline_status.json
```

Example schema:

```json
{
  "last_successful_run": "2026-05-10T08:00:00Z",
  "environment": "azure-vm-prod-lite",
  "sources": {
    "climate": {
      "status": "fresh",
      "rows_loaded": 123456,
      "last_extract_time": "2026-05-10T08:00:00Z"
    },
    "hydrometric_realtime": {
      "status": "fresh",
      "rows_loaded": 8432,
      "last_extract_time": "2026-05-10T08:02:00Z"
    }
  },
  "quality": {
    "dbt_tests_passed": 57,
    "dbt_tests_failed": 0,
    "spatial_join_success_rate": 0.964,
    "geometry_valid_rate": 0.991
  }
}
```

---

## 10. Architecture

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

ADLS Gen2 Bronze Delta
  ├── raw source snapshots
  ├── raw schemas
  ├── file checksums
  └── extract audit metadata

        ↓ PySpark + Apache Sedona
        ↓ CRS standardization
        ↓ geometry validation
        ↓ grid generation
        ↓ spatial joins
        ↓ silver quality checks

ADLS Gen2 Silver Delta
  ├── standardized tabular sources
  ├── standardized geospatial sources
  ├── generated grids
  ├── station-grid maps
  ├── hazard-grid features
  ├── property-flood overlays
  └── spatial coverage confidence

        ↓ dbt Core
        ↓ dbt tests
        ↓ score calibration and validation jobs

Azure PostgreSQL + PostGIS Gold / Serving Layer
  ├── dimensions
  ├── grid marts
  ├── Vancouver parcel mart
  ├── Calgary property flood mart
  ├── municipality aggregation mart
  ├── validation marts
  ├── sensitivity marts
  └── data reliability mart

        ↓ Power BI import / scheduled refresh / curated extracts

Public Portfolio Evidence
  ├── Power BI dashboard
  ├── GitHub Pages project page
  ├── pipeline_status.json
  ├── architecture diagram
  ├── data dictionary
  ├── validation metrics
  └── demo video fallback
```

---

## 11. Airflow DAGs

### DAG 1: `historical_backfill_pipeline`

Purpose:

Build full historical baseline.

```text
historical_backfill_pipeline
  ├── extract_eccc_climate_history
  ├── extract_hydat_sqlite
  ├── extract_wildfire_history
  ├── extract_statcan_building_permits_history
  ├── extract_cdd_disaster_events
  ├── extract_boundary_files
  ├── write_bronze_delta
  ├── run_schema_hash_checks
  ├── run_row_count_checks
  ├── spark_standardize_crs
  ├── spark_build_silver_core_tables
  ├── spark_generate_10km_grid
  ├── spark_build_station_grid_map
  ├── spark_build_grid_hazard_features
  ├── dbt_run_core_models
  ├── dbt_test
  └── write_pipeline_audit
```

### DAG 2: `daily_hazard_pipeline`

Purpose:

Load recent hazard signals.

```text
daily_hazard_pipeline
  ├── extract_latest_hydrometric_realtime
  ├── extract_latest_climate_if_available
  ├── extract_latest_wildfire_if_available
  ├── source_freshness_check
  ├── spark_daily_hazard_features
  ├── update_grid_hazard_mart
  ├── dbt_test_selected
  └── update_public_status_json
```

### DAG 3: `monthly_exposure_pipeline`

Purpose:

Update development and exposure proxies.

```text
monthly_exposure_pipeline
  ├── extract_statcan_building_permits
  ├── extract_vancouver_permits
  ├── extract_calgary_permits
  ├── row_count_anomaly_check
  ├── spark_permit_normalization
  ├── spark_grid_development_features
  ├── update_exposure_marts
  ├── dbt_test_selected
  └── update_public_status_json
```

### DAG 4: `municipal_property_deep_dive_pipeline`

Purpose:

Build Vancouver and Calgary property-context exposure marts.

```text
municipal_property_deep_dive_pipeline
  ├── extract_vancouver_property_parcels
  ├── extract_vancouver_property_tax
  ├── extract_vancouver_floodplain
  ├── extract_calgary_property_assessment
  ├── extract_calgary_flood_hazard
  ├── validate_property_source_schemas
  ├── validate_property_geometries
  ├── standardize_property_crs
  ├── build_vancouver_parcel_flood_overlay
  ├── build_vancouver_permit_property_map
  ├── build_calgary_property_flood_overlay
  ├── build_calgary_permit_property_map
  ├── update_property_exposure_marts
  ├── dbt_test_property_models
  └── write_property_join_audit
```

### DAG 5: `score_validation_pipeline`

Purpose:

Validate scoring method.

```text
score_validation_pipeline
  ├── build_cdd_event_month_labels
  ├── calculate_candidate_grid_scores
  ├── aggregate_grid_to_municipality
  ├── run_out_of_time_backtests
  ├── calculate_lift_at_k
  ├── calculate_event_capture_at_k
  ├── run_weight_sensitivity
  ├── run_radius_sensitivity
  ├── publish_validation_marts
  └── update_validation_page_assets
```

### DAG 6: `data_quality_monitoring`

Purpose:

Monitor pipeline health.

```text
data_quality_monitoring
  ├── source_freshness
  ├── schema_hash_check
  ├── row_count_anomaly
  ├── coordinate_validity
  ├── geometry_validity
  ├── crs_transform_audit
  ├── station_mapping_success
  ├── parcel_overlay_success
  ├── failed_spatial_join_count
  ├── coverage_confidence_summary
  ├── dbt_tests
  ├── publish_dq_results
  └── update_pipeline_status_json
```

---

## 12. dbt Modeling Design

### 12.1 dbt Model Layers

```text
staging
|
v
intermediate
|
v
marts
```

### 12.2 Staging Models

Purpose:

- rename fields;
- cast types;
- standardize dates;
- expose clean source tables to downstream models.

Examples:

```text
stg_climate_daily
stg_hydro_daily
stg_hydat_baseline
stg_wildfire_event
stg_statcan_permits
stg_vancouver_parcel
stg_vancouver_property_tax
stg_vancouver_permits
stg_calgary_property
stg_calgary_flood_hazard
stg_calgary_permits
stg_cdd_events
```

### 12.3 Intermediate Models

Purpose:

- join standardized inputs;
- create reusable feature tables;
- aggregate by grid/city/property/month.

Examples:

```text
int_grid_climate_monthly
int_grid_hydro_monthly
int_grid_wildfire_monthly
int_grid_development_monthly
int_vancouver_parcel_overlay
int_calgary_property_flood_overlay
int_municipality_grid_rollup
int_cdd_event_month_labels
```

### 12.4 Mart Models

Purpose:

- stable final output tables.

Examples:

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

## 13. Power BI Dashboard Plan

### 13.1 Dashboard Role

The dashboard proves the data product is consumable.

It does not define the project.

### 13.2 Final Pages

| Page | Purpose |
|---|---|
| 1. Executive Overview | BC + Alberta grid priority map, top areas, score distribution |
| 2. Grid Hazard Explorer | 10km / 1km grid hazard components and coverage confidence |
| 3. Flood & Hydrometric Monitoring | Water level percentiles, station coverage, hydro anomalies |
| 4. Wildfire Exposure | Wildfire proximity, counts, seasonal patterns |
| 5. Development Exposure | Building-permit trends and development exposure proxy |
| 6. Vancouver Parcel Deep Dive | Parcel exposure summary, floodplain overlap, permit activity |
| 7. Calgary Flood-Property Deep Dive | Property flood zone exposure, assessment value proxy, permit activity |
| 8. Score Validation | CDD backtesting, lift, top-K capture, sensitivity analysis |
| 9. Data Reliability | Freshness, schema drift, row count anomalies, dbt tests, spatial join success |

### 13.3 Map Strategy

Power BI maps should not try to become a heavy WebGIS product.

Rules:

- Use centroid lat/lon for grid display.
- Use aggregated or filtered property records for Vancouver/Calgary pages.
- Keep full geometry in PostGIS, not in visual-heavy Power BI layers.
- Use Data Reliability and Score Validation pages as proof of DE quality.
- Use screenshots and demo video as fallback if public embedding fails.

### 13.4 Required Dashboard Filters

- Province
- Municipality
- Month / year
- Spatial unit type
- Score method
- Priority tier
- Hazard type
- Coverage confidence tier
- Source freshness status

---

## 14. Deployment and Cost Control

### 14.1 Deployment Components

| Component | Choice | Reason |
|---|---|---|
| Storage | ADLS Gen2 | Low-cost lakehouse storage |
| Compute | Azure VM running Dockerized Airflow + Spark | Cheaper than always-on Databricks |
| Serving | Azure PostgreSQL Flexible Server + PostGIS | BI marts and spatial serving |
| BI | Power BI Desktop + Service / Publish to Web | Stable public demo |
| Public Site | GitHub Pages or Azure Static Web Apps | Free/low-cost project wrapper |
| CI/CD | GitHub Actions | Public repo automation |
| IaC | Terraform | Reproducible cloud setup |

### 14.2 Estimated Monthly Cost

| Component | Estimated Cost |
|---|---:|
| ADLS Gen2 | $1-5/month |
| Azure PostgreSQL Flexible Server B1ms + storage | $16-25/month |
| Azure VM for Airflow + Spark | $70-90/month |
| GitHub Pages | $0 |
| GitHub Actions | $0 for normal public repo usage |
| Power BI Desktop | $0 |
| Other | $0-10/month |
| **Total** | **~$90-130/month** |

### 14.3 Cost Decisions

Avoid:

- always-on Databricks clusters;
- Microsoft Fabric capacity;
- real-time streaming infrastructure;
- custom tile servers;
- Azure Container Apps for dashboard hosting;
- heavy DirectQuery production setup.

Use:

- scheduled batch pipelines;
- curated marts;
- Power BI import or refreshable extracts;
- public status JSON;
- screenshots and video fallback.

---

## 15. Repository Structure

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
│   └── dq_thresholds.yml
│
├── airflow/
│   └── dags/
│       ├── historical_backfill_pipeline.py
│       ├── daily_hazard_pipeline.py
│       ├── monthly_exposure_pipeline.py
│       ├── municipal_property_deep_dive_pipeline.py
│       ├── score_validation_pipeline.py
│       └── data_quality_monitoring.py
│
├── spark_jobs/
│   ├── climate_to_silver.py
│   ├── hydrometric_to_silver.py
│   ├── hydat_to_baseline.py
│   ├── wildfire_to_silver.py
│   ├── statcan_permits_to_silver.py
│   ├── disaster_events_to_labels.py
│   ├── boundaries_to_silver.py
│   ├── generate_metric_grids.py
│   ├── build_station_grid_map.py
│   ├── build_grid_hazard_features.py
│   ├── build_grid_development_features.py
│   ├── vancouver_property_to_silver.py
│   ├── vancouver_parcel_flood_overlay.py
│   ├── calgary_property_to_silver.py
│   ├── calgary_property_flood_overlay.py
│   ├── build_spatial_coverage_confidence.py
│   ├── calibrate_priority_score.py
│   └── export_gold_to_postgres.py
│
├── src/
│   ├── ingestion/
│   │   ├── downloaders/
│   │   ├── api_clients/
│   │   └── source_registry.py
│   ├── validation/
│   │   ├── schema_checks.py
│   │   ├── row_count_checks.py
│   │   ├── geometry_checks.py
│   │   └── dq_writer.py
│   ├── geospatial/
│   │   ├── crs.py
│   │   ├── grid.py
│   │   ├── joins.py
│   │   ├── overlays.py
│   │   └── spatial_audit.py
│   ├── scoring/
│   │   ├── grid_score.py
│   │   ├── property_score.py
│   │   ├── validation.py
│   │   └── sensitivity.py
│   ├── audit/
│   │   ├── freshness.py
│   │   ├── schema_hash.py
│   │   ├── row_count.py
│   │   └── pipeline_status.py
│   └── utils/
│       ├── logging.py
│       ├── config.py
│       └── io.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── core/
│   │       ├── grid/
│   │       ├── property/
│   │       ├── validation/
│   │       └── reliability/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── modules/
│
├── dashboard/
│   ├── powerbi/
│   │   ├── README.md
│   │   ├── measures.md
│   │   └── screenshots/
│   └── data_exports/
│
├── public_site/
│   ├── index.html
│   ├── pipeline_status.json
│   └── assets/
│
├── docs/
│   ├── architecture.md
│   ├── data_sources.md
│   ├── geospatial_design.md
│   ├── crs_strategy.md
│   ├── risk_score_validation.md
│   ├── sensitivity_analysis.md
│   ├── data_quality.md
│   ├── dbt_lineage.md
│   ├── deployment.md
│   ├── cost_control.md
│   ├── limitations.md
│   └── interview_story.md
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
└── .github/
    └── workflows/
        ├── ci.yml
        ├── dbt.yml
        ├── docker.yml
        └── deploy_public_status.yml
```

---

## 16. Implementation Sequence and Branch Plan

This is the final execution path. These are not optional extensions.

### `feature/00-project-scaffold`

```text
Create repository
|
v
Create folder structure
|
v
Add README placeholder
|
v
Add PLAN_FINAL.md
|
v
Add .env.example
|
v
Add Makefile
|
v
Add docker-compose.yml placeholder
|
v
PR to develop
```

### `feature/01-config-source-registry`

```text
Create project_scope.yml
|
v
Create source_config.yml
|
v
Create spatial_config.yml
|
v
Create risk_score_config.yml
|
v
Create dq_thresholds.yml
|
v
Implement src/ingestion/source_registry.py
|
v
Add source metadata validation tests
|
v
PR to develop
```

### `feature/02-local-runtime-and-infra`

```text
Build Docker Compose environment
|
v
Add Airflow service
|
v
Add Spark service
|
v
Add PostgreSQL/PostGIS local service
|
v
Add dbt service/config
|
v
Add Terraform skeleton
|
v
Add GitHub Actions CI skeleton
|
v
PR to develop
```

### `feature/03-bronze-national-ingestion`

```text
Implement climate downloader
|
v
Implement hydrometric realtime client
|
v
Implement HYDAT bulk ingestion
|
v
Implement wildfire ingestion
|
v
Implement StatCan permit ingestion
|
v
Implement CDD ingestion
|
v
Implement boundary ingestion
|
v
Write Bronze Delta outputs
|
v
PR to develop
```

### `feature/04-bronze-municipal-ingestion`

```text
Implement Vancouver parcel ingestion
|
v
Implement Vancouver property tax ingestion
|
v
Implement Vancouver permit ingestion
|
v
Implement Vancouver floodplain ingestion
|
v
Implement Calgary property assessment ingestion
|
v
Implement Calgary flood hazard ingestion
|
v
Implement Calgary permit ingestion
|
v
Write Bronze Delta outputs
|
v
PR to develop
```

### `feature/05-audit-framework`

```text
Implement extract run audit
|
v
Implement source freshness checks
|
v
Implement schema hash checks
|
v
Implement row count anomaly checks
|
v
Write audit tables
|
v
Write unit tests
|
v
PR to develop
```

### `feature/06-silver-core-sources`

```text
Standardize climate source
|
v
Standardize hydrometric source
|
v
Build HYDAT baseline table
|
v
Standardize wildfire source
|
v
Standardize building permits
|
v
Standardize CDD event-month table
|
v
Standardize boundary tables
|
v
PR to develop
```

### `feature/07-silver-geospatial-grids`

```text
Implement CRS helper module
|
v
Implement geometry validation module
|
v
Generate 10km BC/AB grid
|
v
Generate 1km Vancouver/Calgary grid
|
v
Build station-to-grid map
|
v
Build grid-to-municipality map
|
v
Write spatial join audit
|
v
PR to develop
```

### `feature/08-grid-feature-engineering`

```text
Build grid climate monthly features
|
v
Build grid hydro monthly features
|
v
Build grid wildfire proximity/count features
|
v
Build grid development features
|
v
Build historical disaster features
|
v
Build coverage confidence table
|
v
PR to develop
```

### `feature/09-property-overlays`

```text
Standardize Vancouver parcel/property tables
|
v
Build Vancouver parcel-floodplain overlay
|
v
Build Vancouver permit-property mapping
|
v
Standardize Calgary property/flood tables
|
v
Build Calgary property-flood overlay
|
v
Build Calgary permit-property mapping
|
v
Write property join audit
|
v
PR to develop
```

### `feature/10-dbt-marts`

```text
Create dbt staging models
|
v
Create dbt intermediate models
|
v
Create grid marts
|
v
Create Vancouver parcel mart
|
v
Create Calgary property mart
|
v
Create municipality aggregation mart
|
v
Create data reliability mart
|
v
Add dbt tests
|
v
PR to develop
```

### `feature/11-validation-scoring`

```text
Implement grid score methods
|
v
Implement Vancouver parcel score
|
v
Implement Calgary flood exposure score
|
v
Map CDD labels to municipality/grid
|
v
Run out-of-time backtesting
|
v
Calculate lift/top-K/sensitivity metrics
|
v
Publish validation marts
|
v
PR to develop
```

### `feature/12-serving-postgis`

```text
Enable PostGIS extension
|
v
Export/load marts to PostgreSQL/PostGIS
|
v
Create geometry columns where useful
|
v
Create GIST indexes
|
v
Validate row counts between Delta and Postgres
|
v
PR to develop
```

### `feature/13-dashboard-public-site`

```text
Build Power BI report
|
v
Create dashboard screenshots
|
v
Create demo video fallback
|
v
Create public_site/index.html
|
v
Publish pipeline_status.json
|
v
Add architecture diagram
|
v
Add public README links
|
v
PR to develop
```

### `feature/14-docs-polish`

```text
Finalize README
|
v
Finalize docs/data_sources.md
|
v
Finalize docs/architecture.md
|
v
Finalize docs/geospatial_design.md
|
v
Finalize docs/data_quality.md
|
v
Finalize docs/risk_score_validation.md
|
v
Finalize docs/limitations.md
|
v
Finalize docs/interview_story.md
|
v
PR to main
```

---

## 17. Testing Strategy

### 17.1 Unit Tests

Test:

- config loading;
- source registry validation;
- schema hash generation;
- row count anomaly logic;
- CRS conversion helper;
- grid ID generation;
- scoring functions;
- validation metrics.

### 17.2 Integration Tests

Test:

- Bronze write/read;
- Silver transformation on fixtures;
- geometry validation on sample polygons;
- spatial join on small known geometries;
- dbt model build on sample data;
- Postgres/PostGIS export;
- pipeline status JSON generation.

### 17.3 dbt Tests

Test:

- primary keys;
- foreign keys;
- accepted values;
- score ranges;
- non-negative values;
- no missing lineage;
- no high-priority row without coverage confidence.

### 17.4 CI/CD Checks

GitHub Actions should run:

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
dbt tests on sample profile
|
v
Docker build check
|
v
Docs link / markdown check
```

---

## 18. Final Deliverables

The project is complete only when all items below exist.

1. GitHub repository with final structure.
2. `PLAN_FINAL.md` in repo root.
3. Docker Compose local environment.
4. Terraform infrastructure code.
5. Airflow DAGs for all six pipelines.
6. ADLS Bronze/Silver/Gold Delta layout.
7. PySpark/Sedona jobs for grid, hazard, and property overlays.
8. Source registry and config-driven ingestion.
9. Source audit tables.
10. Geospatial audit tables.
11. dbt staging, intermediate, and mart models.
12. dbt tests and custom tests.
13. PostgreSQL/PostGIS serving marts.
14. Data reliability mart.
15. Score validation and sensitivity marts.
16. Power BI dashboard with reliability and validation pages.
17. GitHub Pages / Azure Static Web Apps public site.
18. `pipeline_status.json`.
19. Architecture diagram.
20. Data dictionary.
21. Data source documentation.
22. CRS strategy document.
23. Data quality document.
24. Score validation document.
25. Limitations document.
26. Demo video fallback.
27. Final README.
28. Resume bullets and interview story.

---

## 19. Acceptance Criteria

### 19.1 DE Acceptance Criteria

The DE platform is acceptable only if:

- raw data is preserved in Bronze;
- Silver tables are reproducible from Bronze;
- grids are generated programmatically;
- CRS transforms are documented and audited;
- spatial joins write audit records;
- schema drift is detectable;
- row count anomalies are detectable;
- dbt tests run successfully;
- PostgreSQL/PostGIS marts are loadable;
- Power BI uses curated marts or curated extracts;
- `pipeline_status.json` reflects latest run health.

### 19.2 Geospatial Acceptance Criteria

The geospatial layer is acceptable only if:

- 10km BC/AB grid exists;
- 1km Vancouver/Calgary grids exist;
- geometry validity is checked;
- invalid geometry handling is logged;
- all distance/area calculations use projected CRS;
- Vancouver parcel-floodplain overlay exists;
- Calgary property-flood overlay exists;
- spatial join success rates are reported.

### 19.3 Dashboard Acceptance Criteria

The dashboard is acceptable only if it contains:

- executive overview;
- grid hazard explorer;
- flood/hydrometric page;
- wildfire page;
- development exposure page;
- Vancouver parcel page;
- Calgary property flood page;
- score validation page;
- data reliability page.

### 19.4 Portfolio Acceptance Criteria

The project is portfolio-ready only if:

- README shows the live demo or demo video immediately;
- architecture is visible in the first screen or early README;
- data quality is obvious;
- validation is documented;
- limitations are honest;
- screenshots prove pipeline execution;
- resume bullets are concise and DE-focused.

---

## 20. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Municipal schema changes | Pipeline failure | schema hash, source configs, bronze preservation |
| Property data joins are incomplete | Weak parcel/property mart | join quality flags, unmatched audit, honest limitations |
| Power BI public embed unavailable | No live public dashboard | screenshots + demo video fallback + PBIX in repo |
| Geospatial joins are slow | Long pipeline runtime | Sedona for large joins, GeoPandas only for small city layers, spatial indexes in PostGIS |
| Station coverage is sparse in northern regions | Weak signal | coverage confidence score surfaced in dashboard |
| Score has weak validation lift | Methodology questioned | honest gate: report as exploratory heuristic |
| Project scope becomes too front-end heavy | DE story diluted | no custom WebGIS app in final scope |
| Cloud cost grows | Budget issue | VM-based batch compute, no always-on tile server, no Databricks always-on |

---

## 21. README First Screen Requirements

The README must show the DE value immediately.

Required first screen:

```text
Project title
|
v
One-sentence DE pitch
|
v
Live demo / demo video / screenshots
|
v
Architecture badge: Azure + Spark + Delta + Airflow + dbt + PostGIS + Power BI
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
Limitations: exposure screening, not insurance/legal/engineering risk model
```

---

## 22. Resume Bullets

Use 2-3 bullets only.

### Bullet 1 — Core DE Platform

> Built an Azure/Spark lakehouse platform integrating 10 years of Canadian climate, hydrometric, wildfire, building-permit, disaster-event, and municipal property data into ADLS Gen2 Delta Bronze/Silver/Gold layers with Airflow-orchestrated backfill and incremental pipelines.

### Bullet 2 — Geospatial DE + Quality

> Engineered PySpark/Sedona geospatial transformations to generate 10km BC/Alberta risk grids, 1km city grids, and Vancouver/Calgary property-flood overlays, with CRS standardization, geometry validation, schema-drift detection, row-count anomaly checks, and spatial join audit marts.

### Bullet 3 — Serving + Validation

> Modeled trusted dbt/PostgreSQL/PostGIS marts for grid-level hazard exposure, property-level flood screening, data reliability, and score validation; backtested prioritization scores against Canadian Disaster Database events and surfaced results in a public Power BI dashboard.

---

## 23. Interview Positioning

### 23.1 One-Sentence Pitch

> I built a DE-focused Azure/Spark geospatial lakehouse that ingests public Canadian climate, hydrometric, wildfire, permit, disaster, and municipal property datasets, validates them across Bronze/Silver/Gold layers, and serves trusted grid-level and property-context exposure marts to Power BI and PostgreSQL/PostGIS.

### 23.2 Why This Is Data Engineering

This is DE because the project focuses on:

- reliable ingestion from heterogeneous sources;
- raw data preservation;
- schema drift detection;
- row-count anomaly detection;
- PySpark/Sedona spatial transformations;
- CRS standardization;
- geometry validation;
- repeatable grid generation;
- spatial join auditability;
- dbt modeling and testing;
- PostgreSQL/PostGIS serving;
- Airflow orchestration;
- pipeline observability;
- public data freshness reporting.

### 23.3 Answer: “Why Not Just Power BI?”

> Power BI is only the presentation layer. The hard part is making heterogeneous public data reliable enough to feed the dashboard: ingestion, schema drift detection, CRS standardization, spatial joins, coverage confidence, dbt tests, and validation against external disaster events. The dashboard proves the data product is consumable; it is not the core engineering work.

### 23.4 Answer: “Is This a Risk Model?”

> No. I call it an exposure screening and prioritization heuristic. I do not have claims data, insured asset values, engineering flood-depth models, or legal parcel assessment authority. The platform prioritizes areas based on public hazard, exposure, and validation signals, and it clearly reports coverage confidence and limitations.

### 23.5 Answer: “Why Vancouver and Calgary?”

> Vancouver and Calgary are not random add-ons. Vancouver supports a parcel-centric deep dive with parcel, tax, permit, and floodplain data. Calgary supports a flood-centric property deep dive with property assessment, flood hazard, and permit data. They let the platform show city-level property-context engineering without pretending to build full-Canada property risk modeling.

---

## 24. Explicit Non-Goals

The final project does not build:

- Next.js / React front end;
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

## 25. Source Reference Notes

These should be documented in `docs/data_sources.md` during implementation.

- ECCC Historical Climate Data: https://climate.weather.gc.ca/
- Environment and Climate Change Canada climate data: https://www.canada.ca/en/environment-climate-change/services/climate-change/canadian-centre-climate-services/display-download/climate-data.html
- Water Survey of Canada data products and services: https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services.html
- HYDAT archive: https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html
- Canadian Disaster Database: https://www.publicsafety.gc.ca/cnt/rsrcs/cndn-dsstr-dtbs/index-en.aspx
- Vancouver Property Parcel Polygons: https://opendata.vancouver.ca/explore/dataset/property-parcel-polygons/
- Vancouver Property Tax Report: https://vancouver.opendatasoft.com/explore/dataset/property-tax-report/
- Vancouver Issued Building Permits: https://vancouver.aws-ec2-ca-central-1.opendatasoft.com/explore/dataset/issued-building-permits/
- Calgary Open Data Portal: https://data.calgary.ca/
- Calgary Regulatory Flood Hazard Map / Steplines: https://data.calgary.ca/Environment/Regulatory-Flood-Hazard-Map-Steplines/3q69-wm6a/about

---

## 26. Final Definition

This final plan defines the project as:

> A DE-focused Azure/Spark geospatial lakehouse platform for British Columbia and Alberta that produces reliable grid-level and city property-context exposure marts, validates prioritization scores against historical disaster events, tracks data quality across the full pipeline, and delivers a stable Power BI public dashboard backed by PostgreSQL/PostGIS serving marts.

The project’s value is not that it has the flashiest map.

The value is that the data behind the map is:

```text
ingested
|
v
preserved
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
