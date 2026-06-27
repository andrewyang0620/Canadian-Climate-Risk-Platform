# Gold Hydro Monthly Feature Contract
 
## Purpose
 
The Gold hydro monthly feature pipeline converts Silver hydrometric daily observations into
monthly station-level and grid-level hydro features for AB and BC.
 
The v1 pipeline is designed for provincial 10km grid analysis and intentionally does not map
hydro observations to Calgary/Vancouver 1km city grids.
 
## Source Tables
 
The v1 pipeline uses:
 
- `silver_hydro_station`
- `silver_hydro_daily`
- `gold_grid_cell`
 
The v1 pipeline does not use `silver_hydro_realtime_observation`.
 
Realtime hydro observations are high-frequency, short-window records and should be handled
later as a separate realtime/latest Gold feature.
 
## Source Window
 
Gold hydro monthly v1 uses the same analysis window as Gold climate monthly features:
 
{fence}text
2016-01 through 2025-12
{fence}
 
Although `silver_hydro_daily` contains records from 1901 through 2026, Gold hydro monthly v1
uses only 2016-2025 to stay aligned with the climate feature window and downstream grid-month
hazard modeling.
 
## Source Shape
 
`silver_hydro_daily` is stored in long format with the following fields:
 
- `station_id`
- `observation_date`
- `observation_year`
- `observation_month`
- `observation_day`
- `measurement_type`
- `measurement_value`
- `measurement_symbol`
- `province`
- `latitude`
- `longitude`
 
The expected measurement types for v1 are:
 
- `flow`
- `level`
 
## Gold Tables
 
The pipeline produces two Gold tables:
 
- `gold_hydro_station_month_feature`
- `gold_grid_month_hydro_feature`
 
### Table: `gold_hydro_station_month_feature`
 
**Grain:** `province_key x station_id x measurement_type x reference_month`
 
**Primary Key:** `province_key__station_id__measurement_type__reference_month`
 
**Required Fields:**
 
- `hydro_station_month_key`
- `province_key`
- `station_id`
- `station_name`
- `measurement_type`
- `reference_month`
- `latitude`
- `longitude`
- `drainage_area_gross`
- `drainage_area_effect`
- `rhbn`
- `real_time`
- `daily_record_count`
- `observation_day_count`
- `days_in_month`
- `measurement_completeness_ratio`
- `mean_measurement_value`
- `min_measurement_value`
- `max_measurement_value`
- `median_measurement_value`
- `p95_measurement_value`
- `measurement_symbol_count`
- `estimated_symbol_count`
- `approved_symbol_count`
- `flow_zero_day_count`
- `negative_value_count`
 
**Semantics:**
 
- `measurement_type = flow` represents daily streamflow/discharge values.
- `measurement_type = level` represents daily water level/stage values.
- `negative_value_count` is allowed for `level` because level may be relative to a gauge
  datum. For `flow`, negative values are invalid.
 
### Table: `gold_grid_month_hydro_feature`
 
**Grain:** `grid_cell_key x measurement_type x reference_month`
 
Only the following grid systems are used:
 
- `ab_10km`
- `bc_10km`
 
**Primary Key:** `grid_cell_key__measurement_type__reference_month`
 
**Required Fields:**
 
- `grid_month_hydro_feature_key`
- `grid_cell_key`
- `grid_system`
- `grid_level`
- `grid_version`
- `province_key`
- `measurement_type`
- `reference_month`
- `station_count`
- `daily_record_count`
- `observation_day_count`
- `mean_measurement_value`
- `min_measurement_value`
- `max_measurement_value`
- `median_measurement_value`
- `p95_measurement_value`
- `mean_measurement_completeness_ratio`
- `nearest_station_distance_km`
- `mean_station_distance_km`
- `hydro_feature_quality_flag`
 
## Grid Aggregation Semantics
 
Station-month hydro values are aggregated to grid-month features by station averaging unless
a field is explicitly a count.
 
For example:
 
- grid-level `mean_measurement_value` is the average of station-month means
- grid-level `p95_measurement_value` is the average of station-month p95 values
- grid-level `station_count` is the number of stations mapped to the grid cell
- grid-level `daily_record_count` is summed across mapped station-month rows
 
This avoids inflating hydro intensity simply because more stations are mapped to a grid cell.
 
## Mapping Rules
 
Hydro stations are mapped to provincial 10km grid cells using station longitude/latitude
projected from EPSG:4326 into EPSG:3347.
 
The mapping step requires the input grid to use EPSG:3347 analysis geometry.
 
Station mapping guardrails:
 
- minimum station mapping coverage: `0.95`
- maximum reasonable station-to-grid distance: `50.0 km`
 
If a station point intersects multiple grid cells, the pipeline uses a deterministic
tie-break by `grid_cell_key`.
 
## Validation Rules
 
### Station-month validation must check
 
- row count > 0
- primary key is non-null and unique
- reference month range is 2016-01 through 2025-12
- province values are AB/BC only
- measurement types are flow/level only
- station coordinates are non-null and within AB/BC expected bounds
- completeness ratio is between 0 and 1
- flow has no negative values
- station-month rows join to known hydro stations
 
### Grid-month validation must check
 
- row count > 0
- primary key is non-null and unique
- grid systems are only `ab_10km` and `bc_10km`
- reference month range is 2016-01 through 2025-12
- grid keys exist in `gold_grid_cell`
- `station_count >= 1`
- station-grid distances are non-negative and below the max distance threshold
- `hydro_feature_quality_flag` is one of `high`, `medium`, `low`, `very_low`
 
## Quality Flag
 
Grid-month hydro quality is based on mean station completeness:
 
- `high` >= 0.90
- `medium` >= 0.70
- `low` >= 0.40
- `very_low` < 0.40

## Current Validated Output

Latest validated run:

```text
gold_hydro_station_month_feature rows: 158,831
gold_grid_month_hydro_feature rows: 142,249
station_count: 981
mapped_station_count: 981
unmapped_station_count: 0
reference_month range: 2016-01 through 2025-12
month_count: 120
measurement_types: flow, level
feature_grid_cell_count: 841
valid_10km_grid_cell_count: 16,508
validation checks: 25/25 passed
```

Important interpretation:

`gold_grid_month_hydro_feature` is a sparse grid feature table. It only includes grid
cells with mapped hydrometric stations. It does not attempt to fabricate hydro values
for all 10km grid cells.

This is intentional because hydrometric station values are river-network dependent and
should not be treated as continuous surface measurements.