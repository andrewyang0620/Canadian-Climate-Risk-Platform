# Snowflake & dbt Lineage

## Overview

The analytical warehouse follows this path:

```text
ADLS Bronze -> Silver -> Gold
                         |
                         v
                 Snowflake CORE
                         |
                         v
                    dbt models
                         |
                         v
               Snowflake ANALYTICS
                         |
                         v
                      Power BI
```

Python remains responsible for ingestion, spatial processing, hazard features, risk scoring, and backtesting. Snowflake and dbt provide the warehouse and analytical serving layer.

## Snowflake

Database: `CLIMATE_RISK`

Schemas:

- `CORE` — 13 curated physical tables loaded from frozen ADLS Gold snapshots.
- `ANALYTICS` — dbt-managed analytical views and mart tables.
- `AUDIT` — warehouse load audit records such as `CORE_LOAD_RUN`.

ADLS Gold is exposed through `ADLS_GOLD_STAGE` using the Snowflake Azure storage integration. CORE loads are manifest-driven, validated before promotion, and snapshot-aware.

## dbt

dbt uses `CLIMATE_RISK.CORE` as its source layer and writes to `CLIMATE_RISK.ANALYTICS`.

```text
CORE sources
   |
   v
STAGING          13 views
   |
   v
INTERMEDIATE      1 view
   |
   v
MARTS            13 tables
```

### Staging

`stg_*` models are thin views over CORE. They preserve source grain and business logic while standardizing analytical types where needed, especially dates.

### Intermediate

`INT_GRID_MONTH_RISK_PANEL` is the reusable national grid-month backbone. It combines:

- grid-month physical risk features
- risk scores
- disaster observation labels

Its grain is one row per `grid_cell_key + reference_month`.

### Marts

Marts are materialized as Snowflake tables and grouped into three domains.

**National**

- `FCT_GRID_MONTH_RISK`
- `DIM_GRID`
- `DIM_MONTH`
- `DIM_MUNICIPALITY`
- `BRIDGE_GRID_MUNICIPALITY`

**Property**

- `FCT_VANCOUVER_PARCEL_RISK`
- `FCT_VANCOUVER_BUILDING_PERMIT`
- `FCT_CALGARY_PROPERTY_RISK`
- `FCT_CALGARY_BUILDING_PERMIT`
- `FCT_CALGARY_DEVELOPMENT_PERMIT`

**Reliability**

- `FCT_DISASTER_EVENT`
- `BRIDGE_DISASTER_EVENT_GRID`
- `FCT_GRID_MONTH_DISASTER_OBSERVATION`

## Main Relationships

```text
DIM_MONTH
    |
    v
FCT_GRID_MONTH_RISK <--- DIM_GRID ---> FCT_GRID_MONTH_DISASTER_OBSERVATION
                          |
                          v
               BRIDGE_GRID_MUNICIPALITY
                          |
                          v
                  DIM_MUNICIPALITY

FCT_DISASTER_EVENT
        |
        v
BRIDGE_DISASTER_EVENT_GRID
        |
        v
      DIM_GRID

Vancouver / Calgary property facts
        |
        v
      DIM_GRID
```

The dbt layer is tested with source, uniqueness, not-null, accepted-value, and relationship tests. The generated dbt catalog confirms staging/intermediate objects are views and marts are base tables in `CLIMATE_RISK.ANALYTICS`.
