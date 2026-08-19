# Canadian Climate Risk Platform

![Status](https://img.shields.io/badge/status-In%20progress-yellow)
[![Live GIS](https://img.shields.io/badge/Live%20GIS-gis.climate--risk.andrewjingtaoyang.dev-0A84FF)](https://gis.climate-risk.andrewjingtaoyang.dev)
![Python](https://img.shields.io/badge/Python-3_11-3776AB?logo=python&logoColor=white)
![Parquet](https://img.shields.io/badge/storage-Parquet-50ABF1)
![GeoPandas](https://img.shields.io/badge/geospatial-GeoPandas%20%2B%20Shapely-139C5A)
![Azure Target](https://img.shields.io/badge/cloud%20target-Microsoft%20Azure-0078D4?logo=microsoftazure&logoColor=white)
![Snowflake](https://img.shields.io/badge/warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/transformation-dbt%20Core-FF694B?logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/BI-Power%20BI-F2C811?logo=powerbi&logoColor=black)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform&logoColor=white)

> **Work in progress.** This README presents the intended final production architecture and project scope. The live GIS is already deployed while the warehouse, orchestration, BI, and final infrastructure layers are being completed.


## Summary

Canadian Climate Risk Platform is an end-to-end data engineering and geospatial analytics platform that turns fragmented public climate, hydrology, wildfire, disaster, property, flood, and development data into governed analytical products for national risk screening and city-level exposure analysis.

The project is designed around a production-oriented Azure data stack:

- **Multi-source ingestion and Medallion modeling** across Bronze, Silver, and Gold using Python, pandas, GeoPandas, Shapely, Parquet, and Azure Data Lake Storage Gen2
- **National analytical layer** covering 16,508 10 km grid cells across Alberta and British Columbia, 120 monthly periods, and 1,980,960 grid-month risk records
- **City analytical layer** covering 99,726 Vancouver parcels and 410,049 Calgary property locations with assessment, flood, building permit, and development permit context
- **Validated risk scoring** combining Climate, Hydro, and Wildfire domains with historical disaster backtesting
- **Snowflake analytical warehouse** for curated Gold products
- **dbt semantic layer** for dimensional modeling, BI marts, testing, lineage, and documentation
- **Azure Data Factory orchestration** for scheduled cloud workflows and operational monitoring
- **Selective PySpark scale-out** for a representative high-volume transformation with equivalence testing against the pandas implementation
- **Two presentation products** built for different analytical needs: Power BI for KPI and trend analysis, and MapLibre with deck.gl for detailed spatial investigation
- **Infrastructure and delivery** through Terraform, GitHub Actions, Azure Blob Storage, and Azure Static Web Apps

**Live GIS:** <https://gis.climate-risk.andrewjingtaoyang.dev>

## Architecture

```text
Public Data Sources
Climate | Hydrology | Wildfire | Disasters
Property | Assessment | Flood | Permits
                |
                v
        Python Ingestion Layer
                |
                v
      Azure Data Lake Storage Gen2
       Bronze -> Silver -> Gold
                |
        +-------+-------+
        |               |
        v               v
Python Geospatial     Curated Gold
Processing                |
GeoPandas                  v
Shapely                Snowflake
Risk Scoring               |
Backtesting                v
        |                dbt Core
        |          dimensions | facts | marts
        |               |
        +-------+-------+
                |
        +-------+-------+
        |               |
        v               v
   GIS Serving         Power BI
FlatGeobuf + JSON   KPI | Trends | BI
        |
        v
Azure Blob Storage
        |
        v
MapLibre + deck.gl + MapTiler
        |
        v
Azure Static Web Apps

Cloud orchestration:
Azure Data Factory -> ADLS -> Databricks/PySpark pilot
                   -> Gold -> Snowflake -> dbt
```

## Data Engineering Design

### 1. Source ingestion

The platform integrates public datasets with different schemas, spatial resolutions, update patterns, and reliability characteristics.

The ingestion layer is responsible for:

- source-specific extraction
- raw preservation
- schema normalization
- metadata and run tracking
- deterministic keys
- validation before promotion
- local and Azure storage backends

Local Parquet remains the fast development environment. Azure Data Lake Storage Gen2 is the cloud data platform.

### 2. Bronze, Silver, and Gold

```text
Bronze
raw source-aligned records
        |
        v
Silver
cleaned and normalized domain tables
        |
        v
Gold
validated analytical products at explicit business grains
```

Gold products are not flattened into one universal table. Each product preserves the grain required by the underlying business process.

Examples:

```text
National risk
grid_cell_key x reference_month

Vancouver property
property_parcel_key

Calgary property
source_parcel_id

Building permits
permit event grain

Development permits
development permit grain
```

### 3. Geospatial processing

Spatial transformations remain in Python because the core logic depends on GeoPandas, Shapely, coordinate reference systems, polygon intersection, spatial assignment, and interpolation.

Key transformations include:

- national 10 km grid construction and province clipping
- climate station interpolation
- hydrology spatial assignment
- wildfire perimeter overlap
- parcel and property flood overlay
- permit to property mapping
- city property to national grid assignment

dbt does not reimplement these spatial algorithms.

### 4. National risk products

The national analytical layer contains:

- **16,508** 10 km grid cells
- **120** monthly periods from January 2016 to December 2025
- **1,980,960** grid-month risk rows
- Climate, Hydro, and Wildfire domain scores
- composite risk score
- score confidence and coverage signals

Composite weights:

```text
Climate    0.35
Hydro      0.35
Wildfire   0.30
```

Historical validation uses disaster events and evaluates ranking quality with capture@10, lift@10, Spearman correlation, and AUC.

The score is a retrospective prioritization signal. It is not modeled disaster probability or expected financial loss.

### 5. City analytical products

#### Vancouver

- 99,726 parcels
- 1,552,486 source tax assessment records
- 50,610 building permits
- 2,602 parcels with mapped municipal flood exposure
- parcel assessment and zoning context
- building permit to parcel relationships
- national 10 km contextual assignment

#### Calgary

- 410,049 property locations
- 489,276 building permits
- 190,399 development permits
- 8,322 property locations with regulatory flood exposure
- 210,659 development permit to property relationships
- one-to-many development permit mapping
- national 10 km contextual assignment

National risk remains a 10 km contextual signal and is never presented as parcel-level hazard precision.

## Snowflake and dbt

Curated Gold products are loaded into Snowflake rather than copying the entire raw lake into the warehouse.

```text
ADLS Gold
   |
   v
Snowflake
   |
   v
dbt
staging
intermediate
marts
   |
   v
Power BI
```

dbt is responsible for:

- dimensions and fact models
- business-friendly joins
- province and municipality rollups
- BI aggregates
- data reliability marts
- validation marts
- `unique`, `not_null`, `relationships`, and business tests
- lineage and model documentation

Python remains responsible for spatial transformations, risk scoring, and backtesting.

## Orchestration and Scale-out

Azure Data Factory orchestrates the cloud workflow after the analytical pipeline is stable.

A representative workflow is:

```text
Source ingestion
      |
      v
ADLS Bronze
      |
      v
Databricks PySpark climate transformation
      |
      v
ADLS Silver and Gold
      |
      v
Snowflake load
      |
      v
dbt build
      |
      v
Power BI refresh
```

PySpark is used selectively rather than rewriting the entire platform.

The Spark implementation is validated against the pandas pipeline using:

- row count comparison
- schema comparison
- key uniqueness
- aggregate checksums

## Analytics Products

### Power BI

Power BI consumes Snowflake dbt marts and focuses on:

- executive KPIs
- province and municipality trends
- hazard comparisons
- priority distribution
- city exposure
- score validation
- data reliability

### Interactive GIS

The GIS focuses on detailed spatial investigation.

```text
National | Vancouver | Calgary
```

National:

- monthly 10 km Composite, Climate, Hydro, and Wildfire layers
- timeline and region filtering
- grid hover, selection, and detail

Vancouver:

- property assessment and zoning
- municipal flood exposure
- building permits
- permit to parcel interaction

Calgary:

- property assessment
- regulatory flood exposure
- building permits
- development permits
- one-to-many permit to property interaction

GIS presentation data is exported separately from analytical Gold.

```text
National
FlatGeobuf geometry + monthly JSON

City
FlatGeobuf layers + compact relationship JSON
```

Viewport-based FlatGeobuf loading uses HTTP Range requests so large city layers are not downloaded as full files.

## Infrastructure and Delivery

The final platform uses:

- Azure Data Lake Storage Gen2
- Azure Blob Storage
- Azure Static Web Apps
- Azure Data Factory
- Azure Databricks
- Snowflake on Azure
- Terraform
- GitHub Actions

CI and deployment validation cover:

- Python linting and unit tests
- data and schema smoke tests
- dbt compile and tests
- Terraform formatting and validation
- GIS production build
- Azure deployment validation

## Technology Stack

**Data Engineering**

`Python` `pandas` `PySpark` `Parquet` `ADLS Gen2` `Snowflake` `dbt Core` `Azure Data Factory`

**Geospatial**

`GeoPandas` `Shapely` `FlatGeobuf` `MapLibre GL` `deck.gl` `MapTiler`

**Analytics**

`Power BI`

**Cloud and DevOps**

`Azure` `Databricks` `Terraform` `GitHub Actions` `pytest`

## Repository Structure

```text
src/                    ingestion and Bronze, Silver, Gold logic
configs/                source, scoring, and validation configuration
tests/                  unit and validation tests
lakehouse/              local development lake
dashboard/gis/          GIS serving products and web application
dbt/                    Snowflake dbt project
infra/terraform/        Azure and Snowflake infrastructure
docs/                   architecture and operations documentation
```

## Key Engineering Decisions

- Preserve natural business grain instead of forcing properties, permits, and risk grids into one table.
- Keep spatial transformation logic in Python and use dbt for warehouse modeling.
- Load curated Gold into Snowflake instead of duplicating the full Bronze and Silver lake.
- Use Power BI and GIS as separate consumers because they solve different analytical problems.
- Keep national risk at its true 10 km resolution when enriching city properties.
- Do not force nearest-property matches when authoritative permit mapping is unavailable.
- Keep 3D buildings as physical map context rather than using them to encode risk.
- Use one selective PySpark migration to demonstrate scale-out instead of rewriting stable pandas pipelines.
- Use Azure Data Factory as the production orchestrator rather than maintaining parallel Airflow and ADF implementations.
- Maintain one production cloud architecture rather than a dual AWS and Azure deployment.

## Known Limitations

- National risk is based on observed historical signals and is not a forward-looking loss model.
- Climate interpolation preserves the existing 150 km station search radius and IDW semantics, including known circular influence artifacts.
- City flood products preserve source-specific hazard definitions rather than inventing one universal flood severity score.
- National risk assigned to city properties is contextual 10 km information, not parcel-level precision.

## Documentation

- `docs/architecture/national_gis_serving.md`
- `docs/architecture/city_level_silver_gold_summary.md`
- `docs/operations/deployment.md`

## License

See repository license and source-specific terms before redistributing source data.
