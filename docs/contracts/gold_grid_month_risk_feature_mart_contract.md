# Gold Grid Month Risk Feature Mart Contract
 
## Purpose
 
The Gold grid-month risk feature mart integrates spatial grid, municipality, climate,
and hydro monthly Gold features into one analysis-ready table.
 
This mart is designed as the downstream foundation for:
 
- dashboard map layers
- grid-month exposure analysis
- municipality-level aggregation
- modeling datasets
- future risk scoring
 
This mart does not calculate final risk scores. It only prepares clean, joined,
analysis-ready feature data.
 
## Output Table
 
**Table:** `gold_grid_month_risk_feature_mart`
 
**Grain:** `grid_cell_key x reference_month`
 
Each row represents one 10km grid cell in one calendar month.
 
**Primary Key:** `grid_cell_key__reference_month`
 
## Source Tables
 
The mart uses:
 
- `gold_grid_cell`
- `gold_grid_municipality_bridge`
- `gold_grid_month_climate_feature`
- `gold_grid_month_hydro_feature`
 
## Grid Scope
 
The v1 mart includes only provincial 10km grid systems:
 
- `ab_10km`
- `bc_10km`
 
The v1 mart does not include:
 
- `calgary_1km`
- `vancouver_1km`
 
Reason: climate and hydro monthly features are currently generated only for AB/BC 10km
grid systems.
 
## Time Scope
 
The v1 mart uses the same monthly window as Gold climate and Gold hydro:
 
{fence}text
2016-01 through 2025-12
{fence}
 
Expected month count: `120`
 
## Expected Row Count
 
The current 10km grid universe has `16,508` grid cells.
 
Expected mart row count:
 
{fence}text
16,508 x 120 = 1,980,960 rows
{fence}
 
## Join Strategy
 
The mart is built from a complete skeleton:
 
{fence}text
all 10km grid cells x all reference months
{fence}
 
Then it left joins climate and hydro features.
 
This means:
 
- grid-month rows without climate data remain in the mart
- grid-month rows without hydro data remain in the mart
- missing feature values are represented as nulls
- coverage flags indicate whether a feature exists
 
## Municipality Join Strategy
 
`gold_grid_municipality_bridge` can contain multiple municipality matches per grid cell.
 
The mart must not directly join the full bridge table because that would duplicate
grid-month rows.
 
Instead, the mart joins only:
 
{fence}text
is_primary_municipality = True
{fence}
 
Municipality fields included in the mart:
 
- `primary_municipality_key`
- `primary_municipality_name`
- `primary_municipality_type`
- `primary_municipality_grid_coverage_ratio`
- `municipality_match_count`
 
## Climate Join Strategy
 
`gold_grid_month_climate_feature` is already at this grain:
 
{fence}text
grid_cell_key x reference_month
{fence}
 
Climate fields should be prefixed with `climate_` when needed to avoid ambiguity.
 
**Required climate fields:**
 
- `climate_station_count`
- `climate_daily_record_count`
- `climate_temperature_observation_count`
- `climate_precipitation_observation_count`
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
- `climate_nearest_station_distance_km`
- `climate_mean_station_distance_km`
- `temperature_completeness_ratio`
- `precipitation_completeness_ratio`
- `climate_data_completeness_score`
- `climate_feature_quality_flag`
- `has_climate_feature`
 
## Hydro Join Strategy
 
`gold_grid_month_hydro_feature` is long format:
 
{fence}text
grid_cell_key x reference_month x measurement_type
{fence}
 
The mart must pivot hydro into wide format before joining.
 
Expected measurement types:
 
- `flow`
- `level`
 
**Hydro flow fields:**
 
- `flow_station_count`
- `flow_daily_record_count`
- `flow_observation_day_count`
- `flow_measurement_observation_count`
- `flow_mean_measurement_value`
- `flow_min_measurement_value`
- `flow_max_measurement_value`
- `flow_median_measurement_value`
- `flow_p95_measurement_value`
- `flow_mean_measurement_completeness_ratio`
- `flow_zero_day_count`
- `flow_negative_value_count`
- `flow_nearest_station_distance_km`
- `flow_mean_station_distance_km`
- `flow_feature_quality_flag`
- `has_hydro_flow_feature`
 
**Hydro level fields:**
 
- `level_station_count`
- `level_daily_record_count`
- `level_observation_day_count`
- `level_measurement_observation_count`
- `level_mean_measurement_value`
- `level_min_measurement_value`
- `level_max_measurement_value`
- `level_median_measurement_value`
- `level_p95_measurement_value`
- `level_mean_measurement_completeness_ratio`
- `level_zero_day_count`
- `level_negative_value_count`
- `level_nearest_station_distance_km`
- `level_mean_station_distance_km`
- `level_feature_quality_flag`
- `has_hydro_level_feature`
 
## Required Identity and Spatial Fields
 
- `grid_month_risk_feature_key`
- `grid_cell_key`
- `reference_month`
- `grid_system`
- `grid_level`
- `grid_version`
- `province_key`
- `province_code`
- `province_name`
- `boundary_year`
- `cell_size_m`
- `grid_x_index`
- `grid_y_index`
- `centroid_longitude`
- `centroid_latitude`
- `full_cell_area_sq_km`
- `analysis_area_sq_km`
- `boundary_coverage_ratio`
- `is_boundary_edge_cell`
 
## Coverage Flags
 
The mart must include:
 
- `has_climate_feature`
- `has_hydro_flow_feature`
- `has_hydro_level_feature`
 
These flags are important because climate and hydro feature tables are sparse.
 
## Validation Rules
 
The mart validation must check:
 
- row count equals expected skeleton row count
- primary key is non-null and unique
- grid systems are exactly `ab_10km` and `bc_10km`
- reference month range is 2016-01 through 2025-12
- month count is 120
- every `grid_cell_key` exists in `gold_grid_cell`
- every feature grid-month row preserves one row per `grid_cell_key x reference_month`
- municipality join does not duplicate rows
- climate feature flags match climate null/non-null state
- hydro flow feature flags match flow null/non-null state
- hydro level feature flags match level null/non-null state
- completeness ratios are between 0 and 1 when non-null
- quality flags are one of `high`, `medium`, `low`, `very_low` when non-null

## Current Validated Output
 
Latest validated run:
 
```text
gold_grid_month_risk_feature_mart rows: 1,980,960
grid_cell_count: 16,508
reference_month range: 2016-01 through 2025-12
month_count: 120
grid_systems: ab_10km, bc_10km
columns: 82
 
climate_grid_month_count: 48,360
hydro_flow_grid_month_count: 68,247
hydro_level_grid_month_count: 74,002
 
grid_cells_with_climate_feature: 513
grid_cells_with_hydro_flow_feature: 732
grid_cells_with_hydro_level_feature: 809
 
validation checks: 17/17 passed
```
 
Important interpretation:
 
The mart is a complete 10km grid-month skeleton. Climate and hydro features are sparse
and are joined with coverage flags:
 
```text
has_climate_feature
has_hydro_flow_feature
has_hydro_level_feature
```
 
Rows without climate or hydro features are intentionally preserved with null feature
values.