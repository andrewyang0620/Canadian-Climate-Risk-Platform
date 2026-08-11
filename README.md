# Canadian Climate Risk Platform

![Status](https://img.shields.io/badge/status-In%20progress-yellow)
![Python](https://img.shields.io/badge/Python-3_11-3776AB?logo=python&logoColor=white)
![Parquet](https://img.shields.io/badge/storage-Parquet-50ABF1)
![GeoPandas](https://img.shields.io/badge/geospatial-GeoPandas%20%2B%20Shapely-139C5A)
![Azure Target](https://img.shields.io/badge/cloud%20target-Microsoft%20Azure-0078D4?logo=microsoftazure&logoColor=white)
![Snowflake](https://img.shields.io/badge/warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/transformation-dbt%20Core-FF694B?logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/BI-Power%20BI-F2C811?logo=powerbi&logoColor=black)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform&logoColor=white)

> **Status:** Active development.  
> The national data-product and risk-scoring core is complete. The project is currently in **Phase C: Azure foundation and GIS productization**. `develop` is the active integration branch.

An Azure-oriented geospatial data engineering platform that standardizes public Canadian climate, hydrometric, wildfire, boundary, disaster, and municipal datasets into validated spatial data products for Alberta and British Columbia.

The current national product operates at a **10 km grid × month** grain and includes a retrospective multi-hazard prioritization score built from Climate, Hydro, and Wildfire signals.

---

## Project at a Glance

- **Geographic scope:** Alberta and British Columbia at 10 km national grid resolution, with Vancouver and Calgary city datasets standardized for later city-level extensions.
- **Time coverage:** January 2016 through December 2025 for the national risk feature and score skeleton.
- **National analytical grain:** `grid_cell_key × reference_month`.
- **Core Gold mart:** 16,508 grid cells × 120 months = **1,980,960 grid-month rows**.
- **Hazard domains:** Climate, Hydro, and Wildfire.
- **Risk product:** Retrospective multi-hazard prioritization score with domain scores, confidence, provincial monthly percentile ranking, and priority tiers.
- **Validation:** Historical spatial validation against Canadian Disaster Database events, including domain diagnostics, label-quality sensitivity, and weight robustness.
- **Current engineering work:** Replacing the legacy AWS/S3 cloud direction with **Azure Data Lake Storage Gen2**, Azure-oriented Terraform, and baseline CI.
- **Next product work:** National interactive GIS using **MapLibre GL + deck.gl**.
- **Later serving layer:** Snowflake + dbt analytics marts + Power BI.
- **Later cloud automation:** Azure Data Factory orchestration and a selective Azure Databricks / PySpark scale-out pilot.

This is a **public-data climate-risk screening and prioritization platform**. It is not a disaster-probability, expected-loss, insurance underwriting, legal property-risk, or engineering-grade hazard model.

---

## Why This Project Exists

Public climate-risk data in Canada is available across federal, provincial, and municipal sources, but the data is fragmented across:

- different APIs and file formats;
- incompatible temporal grains;
- inconsistent coordinate reference systems;
- station observations, polygons, administrative areas, and property records;
- incomplete spatial coverage;
- different meanings of zero and missing values;
- changing source schemas and update patterns.

The platform turns those sources into reproducible, validated spatial data products that can support questions such as:

- Which 10 km grid cells currently rank highest within a province for observed multi-hazard conditions?
- How much of the ranking is driven by Climate, Hydro, or Wildfire?
- How reliable is the score when observational coverage is incomplete?
- Do historically affected disaster areas tend to receive higher rankings than comparable grids?
- How sensitive are the rankings to alternative domain weights?
- How does spatial label quality affect measured backtest performance?
- How can national risk context later be connected to city and parcel-level exposure products without claiming false spatial precision?

---

## Current Architecture

The **implemented analytical core** is local-first and Parquet-based:

```text
External Public Data Sources
            |
            v
    Python Ingestion
            |
            v
     Bronze Snapshots
            |
            v
  Silver Standardization
            |
            v
Python Geospatial Processing
pandas / GeoPandas / Shapely
            |
            v
          Gold
   Spatial + Hazard Domains
            |
            v
gold_grid_month_risk_feature_mart
            |
            v
      Risk Score v1
            |
            +----------------------+
            |                      |
            v                      v
 Historical Validation        GIS / BI Products
```

The **target cloud architecture** is being implemented from Phase C onward:

```text
External Public Sources
            |
            v
Azure Data Factory                  [Phase G]
            |
            v
Azure Data Lake Storage Gen2       [Phase C]
Bronze / Silver / Gold / Audit
            |
            +-------------------------------+
            |                               |
            v                               v
Python Geospatial Pipelines       Azure Databricks / PySpark
pandas / GeoPandas / Shapely      selective scale-out only [Phase G]
            |
            v
         Snowflake                 [Phase F]
            |
            v
         dbt Core                  [Phase F]
            |
       +----+-------------------+
       |                        |
       v                        v
   Power BI               GIS Data Products
   [Phase F]                    |
                                v
                       MapLibre GL + deck.gl
                              [Phase C]
                                |
                                v
                       Azure Static Web Apps
```

The project is **not** being built as a multi-cloud platform. Legacy S3/AWS scaffolding is being removed as Azure becomes the single cloud target.

---

## Platform Layers

### Bronze — Raw Source Preservation

Bronze keeps source snapshots and run metadata close to the original source representation.

Typical local layout:

```text
lakehouse/bronze/
  <source_name>/
    extract_date=<date>/
      run_id=<run_id>/
        raw/
        metadata.json
```

Bronze ingestion includes source-specific metadata such as:

- run ID and extract date;
- source URL;
- checksum;
- file size;
- row count when available;
- manifest records;
- schema and source-specific audit metadata.

Local development remains the default fast iteration path. The cloud storage target is **Azure Data Lake Storage Gen2**.

### Silver — Standardized Source Products

Silver converts source-specific raw data into reusable, typed analytical inputs.

Implemented Silver pipelines cover national/provincial and municipal sources including:

- ECCC daily climate observations;
- ECCC real-time hydrometric observations;
- HYDAT historical hydrometric data;
- hydro basin polygons;
- wildfire history and perimeter polygons;
- Canadian Disaster Database events;
- Census / administrative boundaries;
- Statistics Canada building permits;
- Vancouver and Calgary property, flood, building-permit, and development-permit datasets.

Silver processing includes:

- type normalization;
- date normalization;
- source-key normalization;
- CRS standardization;
- geometry validation;
- source-specific semantic cleanup;
- validation runners and audit checks.

City-level datasets are standardized in Silver, but city-level Gold products are intentionally a later phase.

### Gold — Validated Spatial and Analytical Products

Gold contains stable, validated data products consumed by scoring, backtesting, GIS, and later warehouse marts.

The national Gold layer is the most mature part of the project.

#### Spatial Foundation

Core spatial outputs include:

- `gold_grid_cell`
- `gold_grid_municipality_bridge`

The national risk product uses:

```text
ab_10km
bc_10km
```

City grid systems are maintained separately and are not mixed into the national risk mart.

#### Climate Gold

Primary output:

```text
gold_grid_month_climate_feature
```

Climate features are assigned to the grid using:

- direct station-in-cell mapping;
- direct multi-station averaging;
- inverse distance weighting (IDW);
- explicit no-coverage state when no suitable station is available.

The feature product retains station-distance diagnostics, mapping method, IDW confidence, completeness metrics, quality flags, and monthly climate indicators such as extreme heat, extreme cold, heavy precipitation, freeze-thaw activity, and precipitation totals.

A grid-month with no climate station/interpolation coverage is **missing**, not zero risk.

#### Hydro Gold

Primary output:

```text
gold_grid_month_hydro_feature
```

Hydrometric observations are assigned spatially through:

- basin-polygon intersection as the primary method;
- station point-in-cell fallback;
- explicit `no_hydro_coverage` state.

The Hydro product contains flow and level features including station counts, observation counts, summary statistics, p95 values, zero-flow diagnostics, measurement completeness, basin coverage, and quality flags.

Missing hydrometric coverage is kept distinct from observed zero values.

#### Wildfire Gold

Primary output:

```text
gold_grid_month_wildfire_perimeter_feature
```

Wildfire uses observed fire perimeter intersection with the 10 km grid.

Unlike Climate and Hydro, Wildfire uses **known-zero semantics**:

> no observed perimeter overlap = confirmed zero overlap for that grid-month.

The product includes perimeter count, intersected area, grid-area overlap ratio, fire-size fields, fire-cause counts, overlap flags, and temporal assignment method.

This null/zero distinction is preserved through the downstream mart and scoring pipeline.

---

## National Risk Feature Mart

Primary output:

```text
gold_grid_month_risk_feature_mart
```

This is the national feature contract consumed by Risk Score v1.

It joins only validated Gold-layer inputs:

```text
gold_grid_cell
gold_grid_municipality_bridge
gold_grid_month_climate_feature
gold_grid_month_hydro_feature
gold_grid_month_wildfire_perimeter_feature
```

Current validated shape:

| Metric | Value |
|---|---:|
| Grid systems | `ab_10km`, `bc_10km` |
| Grid cells | 16,508 |
| Months | 120 |
| Date range | 2016-01 to 2025-12 |
| Rows | 1,980,960 |
| Columns | 107 |
| Grain | `grid_cell_key × reference_month` |

The mart deliberately preserves different domain semantics:

| Domain | No signal / coverage semantics |
|---|---|
| Climate | No observational/interpolation coverage → null feature values |
| Hydro | No hydrometric coverage → null feature values |
| Wildfire | Confirmed no perimeter overlap → zero |

This distinction is a core modeling rule: **missing data is not silently converted into low risk**.

Detailed contract:

```text
docs/contracts/gold_grid_month_risk_feature_mart_contract.md
```

---

## Risk Score v1

Primary output:

```text
gold_grid_month_risk_score
```

The score is a **retrospective monthly multi-hazard prioritization index**. It is not a future-event probability model.

### Domain Scores

The score contains:

```text
climate_sub_score
hydro_sub_score
wildfire_sub_score
composite_risk_score
score_confidence
priority_percentile
priority_tier
```

All domain and composite scores use a `[0, 1]` scale.

Baseline domain weights:

| Domain | Weight |
|---|---:|
| Climate | 0.35 |
| Hydro | 0.35 |
| Wildfire | 0.30 |

### Missing-Data Rules

- Missing domain values are **not** filled with zero.
- Available domain weights are renormalized when calculating the composite score.
- At least two domains are required for composite-score eligibility.
- Score confidence is calculated separately and does not renormalize missing domain weight away.

This means a high score and a high confidence value communicate different information.

### Ranking

Eligible grids are ranked within:

```text
province_key × reference_month
```

Ranking requires composite-score eligibility and at least 1% provincial-boundary coverage.

Priority tiers:

```text
very_high         >= 0.90
high              >= 0.75
elevated          >= 0.50
moderate          >= 0.25
low               <  0.25
insufficient_data not ranking eligible
```

Detailed design and contract:

```text
docs/architecture/risk_scoring_design.md
docs/contracts/gold_grid_month_risk_score_contract.md
configs/risk_score_config.yml
```

---

## Disaster Labels and Historical Validation

Disaster-event labels are built separately from scoring inputs to avoid label leakage.

Supporting Gold products include event-reference, administrative-scope, grid-scope, and grid-month label tables under:

```text
src/gold/disaster/
```

Primary validation label:

```text
gold_grid_month_disaster_event_label
```

The backtest asks:

> Within the same province and month, are grids associated with recorded disaster events ranked higher than comparable grids without a recorded grid-eligible event?

The validation is **contemporaneous and retrospective**. It does not claim forecasting skill.

### Validation Sample

- 22 underlying disaster events
- 36 event-month observations
- 36,681 event-grid assignments
- 11 Alberta events
- 11 British Columbia events
- 7 wildfire events
- 6 flood events
- 9 severe-storm / climate events

### Overall Results

| Metric | Mean | Median |
|---|---:|---:|
| Event capture @ top 10% | 19.3% | 6.9% |
| Capture lift @ top 10% | 1.93× | 0.69× |
| Event AUC | 0.540 | 0.507 |

The result is best interpreted as a **modest positive prioritization signal overall with substantial event-level heterogeneity**.

### Provincial Results

| Province | Events | Mean capture @10 | Mean lift @10 | Mean AUC |
|---|---:|---:|---:|---:|
| Alberta | 11 | 2.7% | 0.27× | 0.374 |
| British Columbia | 11 | 35.8% | 3.58× | 0.706 |

British Columbia shows substantially stronger historical spatial alignment.

Alberta's validation labels are materially coarser at grid level: many events are represented through parent Census Division or broad regional administrative footprints. The validation therefore treats spatial-label precision as an important confounding factor rather than claiming that measured provincial differences are purely model-driven.

### Domain Diagnostics

| Disaster domain | Mean AUC | Median AUC |
|---|---:|---:|
| Flood / Hydro | 0.671 | 0.714 |
| Severe storm / Climate | 0.654 | 0.813 |
| Wildfire | 0.523 | 0.514 |

Hydro and Climate show materially stronger event-level discrimination than Wildfire.

The Wildfire sub-score measures contemporaneous observed burn-perimeter overlap, while many disaster labels represent broader administrative affected areas. The current result is therefore not presented as predictive wildfire skill.

### Label-Quality Sensitivity

| Scenario | Events | Mean capture @10 | Mean lift @10 | Mean AUC |
|---|---:|---:|---:|---:|
| Baseline | 22 | 19.3% | 1.93× | 0.540 |
| Exclude CSD approximation | 14 | 23.1% | 2.31× | 0.602 |

Because the event sample changes, this is not interpreted causally. It does support spatial-label quality as an important source of validation uncertainty.

### Weight Robustness

Reasonable alternative Climate/Hydro/Wildfire weight scenarios leave the underlying ranking structure largely stable.

Equal weights produce approximately:

```text
mean province-month Spearman vs baseline: 0.999
mean top-10 Jaccard vs baseline:           0.974
```

Sensitivity analysis is used as a robustness check, not as a label-driven weight tuning procedure.

Full report:

```text
docs/analysis/risk_score_validation.md
```

---

## Data Quality and Validation

Validation is treated as a first-class data product rather than an end-of-pipeline check.

### Ingestion and Bronze

- source availability;
- checksums;
- file size;
- source row counts when available;
- extract metadata;
- run manifests;
- source-specific audit logging;
- pagination reconciliation for API sources.

### Silver

- schema and type validation;
- key and grain validation;
- CRS validation;
- geometry validity;
- coordinate checks;
- source-specific accepted values;
- missingness and semantic consistency.

### Gold Spatial / Hazard Products

- deterministic grid skeleton;
- uniqueness and row-count conservation;
- spatial coverage checks;
- method-enum validation;
- geometry and overlap validation;
- coverage and quality flags;
- ratio bounds;
- explicit null-vs-zero semantics.

### Risk Scoring

- `[0, 1]` score bounds;
- domain-availability consistency;
- component-weight accounting;
- minimum-domain eligibility;
- confidence consistency;
- ranking eligibility;
- priority-tier consistency.

### Backtesting

- same-province / same-month comparison universes;
- exclusion of other recorded events from controls;
- event-month first aggregation;
- source-event second aggregation;
- domain diagnostics;
- label-quality sensitivity;
- domain-weight sensitivity;
- rank stability.

Contracts are maintained under `docs/contracts/` and unit coverage under `tests/unit/`.

---

## Main Data Sources

### National / Provincial

- Environment and Climate Change Canada historical climate observations
- Environment and Climate Change Canada hydrometric observations
- HYDAT historical hydrometric archive
- CWFIS / CNFDB wildfire history and perimeter data
- Statistics Canada building permits
- Census / Census Subdivision / Census Division boundaries
- Canadian Disaster Database

### Vancouver

- property parcels
- property tax / assessment context
- issued building permits
- designated floodplain / flood-hazard data

### Calgary

- property assessment
- property tax / assessment context
- building permits
- development permits
- flood-hazard data where suitable spatial coverage is available

Municipal datasets are standardized in Silver today. City-level Gold exposure products are a later phase and will not be presented as complete until their spatial validation is finished.

---

## Technology Stack and Delivery Status

The repository deliberately separates **implemented technology** from **target technology**.

| Layer | Technology | Status |
|---|---|---|
| Core processing | Python, pandas, NumPy | Implemented |
| Local lakehouse | Parquet / PyArrow | Implemented |
| Spatial processing | GeoPandas, Shapely, pyproj, Fiona | Implemented |
| Local spatial QA | PostgreSQL / PostGIS | Optional |
| Storage abstraction | Local filesystem backend | Implemented |
| Cloud storage | Azure Data Lake Storage Gen2 | Phase C migration |
| GIS | MapLibre GL + deck.gl | Phase C |
| Public GIS hosting | Azure Static Web Apps | Phase C |
| Analytical warehouse | Snowflake on Azure | Phase F |
| SQL transformation | dbt Core + dbt-snowflake | Scaffold exists; Phase F implementation |
| Business intelligence | Power BI | Phase F |
| Distributed processing | Azure Databricks / PySpark | Selective Phase G pilot |
| Distributed spatial | Apache Sedona | Optional if justified by a real workload |
| Orchestration | Azure Data Factory | Phase G |
| Infrastructure as Code | Terraform | Azure migration in progress; Snowflake foundation retained |
| CI/CD | GitHub Actions | Phase C baseline, expanded later |

### Explicit Non-Goals

The final architecture will **not** use:

- AWS S3 as the production data lake;
- Apache Airflow as the production orchestrator;
- a permanent local Spark cluster;
- Tableau as the primary BI tool;
- Kepler.gl or Folium as the final public GIS;
- multi-cloud deployment purely for technology breadth.

The project favors a smaller number of technologies with clear responsibilities over duplicated infrastructure.

---

## Current Project Status

### Phase A — National Gold Closure

**Status: Complete**

Completed:

- national spatial grid foundation;
- grid-to-municipality bridge;
- Climate v2 Gold;
- Hydro v2 Gold;
- Wildfire perimeter Gold;
- national grid-month risk feature mart;
- disaster-event label products.

Development exposure was intentionally excluded from the national score because the available permit data does not support defensible 10 km grid attribution without introducing false precision.

### Phase B — Risk Scoring and Historical Validation

**Status: Complete**

Completed:

- scoring design;
- Climate / Hydro / Wildfire sub-scores;
- composite multi-hazard score;
- confidence calculation;
- provincial monthly percentile ranking;
- priority tiers;
- risk-score Gold output;
- disaster-event historical validation;
- domain diagnostics;
- label-quality sensitivity;
- weight sensitivity;
- validation report.

### Phase C — Azure Foundation and National GIS

**Status: In progress**

Current work:

1. remove legacy AWS/S3 and Airflow infrastructure references;
2. add Azure Data Lake Storage Gen2 backend;
3. provision Azure storage foundation through Terraform;
4. add baseline GitHub Actions CI;
5. build GIS-ready national exports;
6. generate static project maps;
7. build the MapLibre GL + deck.gl National Risk Explorer;
8. deploy the GIS application to Azure Static Web Apps.

### Phase D — City Gold

Planned:

- Vancouver parcel flood exposure;
- Calgary property exposure only where a defensible spatial hazard layer exists;
- optional city development-intensity features.

### Phase E — City Spatial Context and GIS

Planned:

- connect city exposure products to national-grid context;
- retain explicit 10 km resolution metadata;
- integrate city / parcel layers into the same MapLibre/deck.gl application.

### Phase F — Snowflake, dbt, and Power BI

Planned:

- load curated Gold products into Snowflake;
- build dbt staging, intermediate, rollup, reliability, and BI marts;
- build Power BI pages for executive overview, hazards, city exposure, score validation, and data reliability.

Power BI will handle business analytics and cross-filtering. Detailed polygon exploration remains the responsibility of the dedicated GIS application.

### Phase G — Cloud Automation and Scale-Out

Planned:

- one real Azure Databricks / PySpark pipeline pilot;
- equivalence testing against the existing pandas implementation;
- Azure Data Factory orchestration;
- Terraform hardening for ADF, Databricks, identities, Key Vault, and deployment;
- expanded GitHub Actions CI/CD.

The existing validated pandas / geospatial pipelines will **not** be rewritten wholesale in Spark.

### Phase H — Final Documentation and Portfolio Delivery

Planned:

- final architecture diagram;
- public GIS link and screenshots;
- Power BI evidence;
- deployment and local-development runbooks;
- project limitations;
- interview walkthrough and architecture trade-off notes.

---

## Local Development

### Install

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Run Unit Tests

```powershell
python -m pytest tests/unit -q
```

### Lint and Format Checks

```powershell
python -m ruff check src tests
python -m black --check src tests
```

### Build the National Risk Feature Mart

```powershell
python -m src.gold.mart.run_risk_monthly_grid
```

Validate it:

```powershell
python -m src.gold.mart.validate_risk_monthly_grid
```

### Build Risk Score v1

```powershell
python -m src.scoring.run_risk_score
```

### Run Historical Validation

```powershell
python -m src.backtesting.run_risk_score_backtest
```

Generated lakehouse data and audit outputs are runtime artifacts and are not intended to be committed to Git.

---

## Repository Structure

The active repository is organized by data-product responsibility rather than by notebook or analysis task.

```text
configs/
  backtesting/
  platform_config.yml
  project_scope.yml
  risk_score_config.yml
  source_config.yml
  spatial_config.yml

src/
  ingestion/
  audit/
  profiling/
  silver/
  gold/
    climate/
    hydro/
    wildfire/
    disaster/
    spatial/
    mart/
  scoring/
  backtesting/
  storage/
  validation/
  utils/

dbt/
  models/
  macros/
  profiles/
  tests/

infra/
  terraform/
    snowflake/
    azure/          # Phase C Azure foundation

dashboard/
  gis/              # Phase C
  powerbi/          # Phase F
  screenshots/

spark_jobs/         # Phase G selective PySpark pilot

docs/
  analysis/
  architecture/
  contracts/
  data/
  operations/
  profiles/

tests/
  unit/
```

Legacy AWS/S3 and Airflow scaffolding is being removed as part of the Azure architecture migration.

---

## Key Design Decisions

### 1. Local-first development, Azure cloud target

The project keeps local Parquet execution because it makes pipeline development and validation fast and reproducible. Azure is the deployment target, not a replacement for the local developer workflow.

### 2. ADLS Gen2 for the lake, Snowflake for analytics

ADLS Gen2 will hold the Bronze / Silver / Gold / Audit lakehouse zones. Snowflake will hold curated analytical datasets and dbt marts rather than duplicating every raw source object.

### 3. Python remains the main geospatial transformation layer

IDW interpolation, basin intersection, fire-perimeter overlay, administrative spatial mapping, and validated geometry logic already exist in Python. They are not being rewritten in SQL simply to increase tool count.

### 4. dbt owns analytical modeling, not geometry

dbt will own staging models, relational transformations, dimensions, rollups, BI marts, warehouse tests, lineage, and documentation. Python retains spatial computations that are naturally expressed through GeoPandas / Shapely.

### 5. Power BI and GIS have different jobs

Power BI will answer KPI, time-series, comparison, distribution, and business-filtering questions.

MapLibre/deck.gl will handle 10 km polygon exploration, hazard layer switching, confidence overlays, grid interaction, and later city / parcel polygons.

### 6. PySpark is a scale-out demonstration, not a rewrite

One meaningful pipeline will be ported to Databricks/PySpark and compared with the existing pandas output. Spark is included only where it demonstrates a legitimate scale-out path.

### 7. Risk score is prioritization, not prediction

The score combines observed monthly hazard conditions. The backtest evaluates historical spatial alignment. The project does not relabel this as disaster prediction, probability, expected loss, or causal impact.

---

## Limitations

The platform is intended for analytical screening, prioritization, portfolio demonstration, and exploratory spatial analysis.

It is not:

- an insurance underwriting model;
- a catastrophe-loss model;
- a disaster-probability model;
- an engineering-grade flood-depth model;
- a legal property-risk assessment;
- a property appraisal model;
- a real-time emergency alerting system.

Important analytical limitations include:

- national risk products operate at 10 km resolution;
- observational Climate and Hydro coverage is incomplete in some grid-months;
- historical disaster labels are administrative-event footprints rather than authoritative physical damage perimeters;
- Alberta disaster labels are generally coarser than British Columbia labels in the current validation sample;
- Wildfire validation compares observed perimeter overlap with broader disaster-event administrative footprints;
- national risk context must not be presented as parcel-level precision in later city products.

The platform preserves quality flags, coverage indicators, score confidence, spatial-assignment methods, and validation limitations so downstream users can distinguish low risk from low information.

---

## Documentation

Useful starting points:

```text
docs/contracts/gold_grid_month_risk_feature_mart_contract.md
docs/contracts/gold_grid_month_risk_score_contract.md
docs/analysis/risk_score_validation.md
docs/architecture/risk_scoring_design.md
docs/architecture/storage_backend.md
configs/risk_score_config.yml
```

---

## License

MIT