# Gold Monthly Climate Feature Contract

## Purpose

This Gold layer converts daily ECCC historical climate observations into monthly climate features for downstream exposure, hazard, and risk-priority modeling.

## Input Tables

- `silver_climate_daily`
- `gold_grid_cell`

## Output Tables

### `gold_climate_station_month_feature`

**Grain:** one row per `station_id` × `reference_month`

**Purpose:** Aggregates daily climate records into station-month climate summaries.

**Important fields:**

- `climate_station_month_key`
- `station_id`
- `station_name`
- `province_key`
- `reference_month`
- `latitude`
- `longitude`
- `daily_record_count`
- `temperature_observation_count`
- `precipitation_observation_count`
- `mean_temp_c`
- `min_temp_c`
- `max_temp_c`
- `observed_min_temp_c`
- `observed_max_temp_c`
- `total_precip_mm`
- `total_rain_mm`
- `total_snow`
- `precipitation_days`
- `heavy_precipitation_days`
- `extreme_heat_days`
- `extreme_cold_days`
- `freeze_thaw_days`
- `temperature_completeness_ratio`
- `precipitation_completeness_ratio`

### `gold_grid_month_climate_feature`

**Grain:** one row per `grid_cell_key` × `reference_month`

**Purpose:** Maps station-month climate features to province-level 10 km Gold grid cells.

**Important fields:**

- `grid_month_climate_feature_key`
- `grid_cell_key`
- `grid_system`
- `grid_level`
- `grid_version`
- `province_key`
- `reference_month`
- `station_count`
- `nearest_station_distance_km`
- `mean_station_distance_km`
- `mean_temp_c`
- `min_temp_c`
- `max_temp_c`
- `observed_min_temp_c`
- `observed_max_temp_c`
- `total_precip_mm`
- `total_rain_mm`
- `total_snow`
- `precipitation_days`
- `heavy_precipitation_days`
- `extreme_heat_days`
- `extreme_cold_days`
- `freeze_thaw_days`
- `temperature_completeness_ratio`
- `precipitation_completeness_ratio`
- `climate_data_completeness_score`
- `climate_feature_quality_flag`

## Spatial Scope

This version maps climate stations to:

- `ab_10km`
- `bc_10km`

City-level grids are intentionally excluded from this v1 feature table:

- `calgary_1km`
- `vancouver_1km`

City-level climate features can be added later using either nearest-station mapping or interpolation.

## Temporal Scope

- Expected month range: `2016-01` through `2025-12`
- Expected month count: `120`

## Mapping Method

Climate stations are mapped to Gold grid cells using station coordinates projected from EPSG:4326 into EPSG:3347.

- If a station point falls inside a grid cell, the mapping method is `point_within_grid_cell`.
- If a station point falls outside all available grid cells, the nearest grid cell is selected.

## Null Handling

Monthly precipitation-like totals preserve all-null groups as null. This means:

- all daily values missing → monthly value is null
- some daily values present → monthly value is sum of present values

This avoids incorrectly converting missing rain or snow observations into zero.

## Quality Flags

`climate_feature_quality_flag` is derived from `climate_data_completeness_score`.

Allowed values:

- `high`
- `medium`
- `low`
- `very_low`

## Validation

The formal validation runner is:

```bash
python -m src.gold.climate.validate_monthly_features
```

Current validation coverage:

- [x] station-month row count
- [x] station-month key uniqueness
- [x] month range
- [x] province scope
- [x] coordinate validity
- [x] precipitation non-negativity
- [x] completeness ratio bounds
- [x] grid-month row count
- [x] grid-month key uniqueness
- [x] expected grid systems
- [x] known grid keys
- [x] station count validity
- [x] station distance validity
- [x] quality flag validity

## Source Selection and Mapping Guardrails

The Gold monthly climate feature pipeline intentionally reads only the latest available
`silver_climate_daily` run. The source reader selects the latest `extract_date`, then the
latest `run_id` within that extract date by file modification time. This prevents historical
Silver runs from being silently concatenated into the Gold feature build.

Climate station-to-grid mapping uses only provincial 10km grid systems:

- `ab_10km`
- `bc_10km`

The mapping step requires the input grid to use EPSG:3347 analysis geometry. The pipeline
fails fast if a non-3347 grid is supplied.

The pipeline also enforces station mapping guardrails:

- minimum station mapping coverage: `0.95`
- maximum reasonable station-to-grid distance: `50.0 km`

Current production output maps all climate stations successfully, with no unmapped stations.

## Grid-Month Aggregation Semantics

`gold_climate_station_month_feature` is built at the station-month grain:

    province_key × station_id × reference_month

The station-month key format is:

    province_key__station_id__reference_month

`gold_grid_month_climate_feature` is built at the grid-cell-month grain. When multiple stations
map to the same grid cell, climate values are station-averaged unless the field is explicitly a
count of records or stations.

For example:

- station-level `extreme_heat_days` means the number of extreme heat days at one station in one month
- grid-level `extreme_heat_days` means the average extreme heat days across stations mapped to that grid cell
- grid-level `total_precip_mm`, `total_rain_mm`, and `total_snow` are station-averaged monthly values, not summed totals across stations

This avoids inflating climate intensity simply because more stations are mapped to a grid cell.
