# Gold Contract: `gold_grid_month_risk_feature_mart`

## 1. Purpose

`gold_grid_month_risk_feature_mart` is the national monthly grid-level risk feature mart.

It combines the core Gold domain features required for downstream climate-risk scoring, backtesting, GIS visualization, and BI reporting.

This table is the main A1 output of the national Gold closure phase.

It joins:

- Gold spatial grid foundation
- Gold primary municipality assignment
- Gold monthly climate grid features
- Gold monthly hydro grid features
- Gold monthly wildfire perimeter features

The table grain is:

```text
one row per grid_cell_key × reference_month
```

---

## 2. Current Version

```text
Version: v2
Status: production-ready local Gold table
Primary output: lakehouse/gold/gold_grid_month_risk_feature_mart/
Validation status: passed
Validation checks: 14
```

The v2 mart replaces the old v1 mart.

The old v1 mart expected Hydro long-format rows with `measurement_type`.  
The v2 mart uses Hydro v2 wide-format features and includes wildfire perimeter features.

---

## 3. Output Table

```text
Table name:
gold_grid_month_risk_feature_mart

Storage format:
Parquet

Latest validated output:
lakehouse/gold/gold_grid_month_risk_feature_mart/extract_date=2026-07-04/run_id=3cd37b22-656c-4621-8c0d-90a375f85e45/gold_grid_month_risk_feature_mart.parquet
```

Generated lakehouse files are local artifacts and should not be committed.

---

## 4. Input Tables

The mart is built only from Gold-layer inputs.

```text
gold_grid_cell
gold_grid_municipality_bridge
gold_grid_month_climate_feature
gold_grid_month_hydro_feature
gold_grid_month_wildfire_perimeter_feature
```

Silver-layer inputs are not read directly by this mart.

---

## 5. Grain and Coverage

### Grain

```text
grid_cell_key × reference_month
```

### Expected coverage

```text
Grid systems:
ab_10km
bc_10km

Grid cells:
16,508

Months:
120

Month range:
2016-01 through 2025-12

Expected rows:
1,980,960
```

### Latest validated output

```text
row_count: 1,980,960
column_count: 107
grid_cell_count: 16,508
month_count: 120
minimum_month: 2016-01
maximum_month: 2025-12
unique_grid_month_risk_feature_key_count: 1,980,960
```

---

## 6. Grid Scope

Only provincial 10km grid systems are included:

```text
ab_10km
bc_10km
```

City-level 1km grid systems are excluded from this mart:

```text
vancouver_1km
calgary_1km
```

City-level marts should be built separately in the city Gold phase.

---

## 7. Primary Key

### Primary key column

```text
grid_month_risk_feature_key
```

### Key format

```text
{grid_cell_key}__{reference_month}
```

Example:

```text
ab_10km_x1_y1__2016-01
```

### Required constraints

```text
grid_month_risk_feature_key is non-null
grid_month_risk_feature_key is unique
(grid_cell_key, reference_month) is unique
```

---

## 8. Spatial and Municipality Columns

The mart carries core spatial context from `gold_grid_cell` and primary municipality context from `gold_grid_municipality_bridge`.

### Core grid columns

```text
grid_month_risk_feature_key
grid_cell_key
grid_system
grid_level
grid_version
province_key
province_code
province_name
boundary_year
cell_size_m
grid_x_index
grid_y_index
centroid_longitude
centroid_latitude
full_cell_area_sq_km
analysis_area_sq_km
boundary_coverage_ratio
is_boundary_edge_cell
reference_month
```

### Primary municipality columns

```text
primary_municipality_key
primary_municipality_name
primary_municipality_type
primary_municipality_grid_coverage_ratio
primary_municipality_coverage_ratio
municipality_match_count
```

### Municipality semantics

Each grid cell should have one primary municipality assignment.

`municipality_match_count` records how many municipalities intersect the grid cell.

The primary municipality is selected upstream in `gold_grid_municipality_bridge`.

---

## 9. Climate Feature Columns

Climate features come from:

```text
gold_grid_month_climate_feature
```

### Climate assignment methods

Valid values:

```text
direct_station_in_cell
direct_station_average_in_cell
idw_interpolated
no_station_within_radius
```

### Latest validated method counts

```text
idw_interpolated: 1,786,619
no_station_within_radius: 146,617
direct_station_in_cell: 40,941
direct_station_average_in_cell: 6,783
```

### Climate feature columns

```text
grid_month_climate_feature_key
climate_mapping_method
climate_station_count
climate_nearest_station_distance_km
climate_mean_station_distance_km
climate_max_station_distance_km
climate_idw_confidence_score
climate_daily_record_count
climate_temperature_observation_count
climate_precipitation_observation_count
climate_mean_temp_c
climate_min_temp_c
climate_max_temp_c
climate_observed_min_temp_c
climate_observed_max_temp_c
climate_total_precip_mm
climate_total_rain_mm
climate_total_snow
climate_precipitation_days
climate_heavy_precipitation_days
climate_extreme_heat_days
climate_extreme_cold_days
climate_freeze_thaw_days
climate_temperature_completeness_ratio
climate_precipitation_completeness_ratio
climate_data_completeness_score
climate_feature_quality_flag
```

### Climate quality flags

Valid non-null values:

```text
direct
high
medium
low
very_low
```

Latest validated quality counts:

```text
medium: 1,393,187
low: 193,200
high: 164,066
None: 146,617
direct: 47,724
very_low: 36,166
```

### Climate no-coverage semantics

For rows where:

```text
climate_mapping_method = no_station_within_radius
```

the following semantics must hold:

```text
has_climate_feature = false
climate_station_count = 0
climate_idw_confidence_score = 0
climate_feature_quality_flag = null
climate value columns = null
climate completeness score columns = null
```

Important distinction:

`climate_idw_confidence_score = 0` is valid for `no_station_within_radius`.  
It should not be treated as a null-leakage error.

### Latest validated climate coverage

```text
has_climate_feature true rows: 1,834,343
grid cells with climate feature: 15,819
no_station_within_radius rows: 146,617
no climate mean_temp non-null rows: 0
no climate quality non-null rows: 0
```

---

## 10. Hydro Feature Columns

Hydro features come from:

```text
gold_grid_month_hydro_feature
```

Hydro v2 is wide-format.  
It does not use `measurement_type` rows in this mart.

### Hydro spatial assignment methods

Valid values:

```text
basin_polygon_intersection
station_point_in_cell
no_hydro_coverage
```

### Latest validated method counts

```text
basin_polygon_intersection: 1,647,240
no_hydro_coverage: 333,480
station_point_in_cell: 240
```

### Hydro feature columns

```text
grid_month_hydro_feature_key
hydro_spatial_assignment_method
hydro_station_count
hydro_basin_station_count
hydro_point_station_count
hydro_basin_intersection_area_sq_km
hydro_basin_grid_coverage_ratio
flow_station_count
flow_daily_record_count
flow_observation_day_count
flow_measurement_observation_count
flow_mean_measurement_value
flow_min_measurement_value
flow_max_measurement_value
flow_median_measurement_value
flow_p95_measurement_value
flow_measurement_completeness_ratio
flow_zero_day_count
flow_negative_value_count
level_station_count
level_daily_record_count
level_observation_day_count
level_measurement_observation_count
level_mean_measurement_value
level_min_measurement_value
level_max_measurement_value
level_median_measurement_value
level_p95_measurement_value
level_measurement_completeness_ratio
level_negative_value_count
hydro_data_completeness_score
hydro_feature_quality_flag
```

### Hydro quality flags

Valid non-null values:

```text
high
medium
low
very_low
```

Latest validated quality counts:

```text
high: 1,392,073
None: 455,555
medium: 112,085
low: 15,529
very_low: 5,718
```

### Hydro no-coverage semantics

For rows where:

```text
hydro_spatial_assignment_method = no_hydro_coverage
```

the following semantics must hold:

```text
has_hydro_spatial_coverage = false
has_hydro_flow_feature = false
has_hydro_level_feature = false
has_hydro_feature = false
hydro_station_count = 0
hydro_basin_station_count = 0
hydro_point_station_count = 0
flow count columns = 0
level count columns = 0
flow value columns = null
level value columns = null
hydro basin area and ratio columns = null
hydro_data_completeness_score = null
hydro_feature_quality_flag = null
```

### Latest validated hydro coverage

```text
has_hydro_spatial_coverage true rows: 1,647,480
has_hydro_flow_feature true rows: 1,510,965
has_hydro_level_feature true rows: 1,517,240
has_hydro_feature true rows: 1,525,405
grid cells with hydro spatial coverage: 13,729
grid cells with hydro available feature: 13,729
no_hydro_coverage rows: 333,480
no hydro flow mean non-null rows: 0
no hydro level mean non-null rows: 0
no hydro quality non-null rows: 0
```

---

## 11. Wildfire Perimeter Feature Columns

Wildfire perimeter features come from:

```text
gold_grid_month_wildfire_perimeter_feature
```

Wildfire uses zero semantics.

A zero value means confirmed no observed wildfire perimeter overlap for that grid-month.

This is different from climate and hydro, where null can indicate no observational coverage.

### Wildfire temporal assignment methods

Valid values:

```text
polygon_fire_month
no_observed_perimeter_overlap
```

### Latest validated method counts

```text
no_observed_perimeter_overlap: 1,972,718
polygon_fire_month: 8,242
```

### Wildfire feature columns

```text
wildfire_grid_month_key
wildfire_crs_epsg
wildfire_grid_analysis_area_sq_km
wildfire_perimeter_count
wildfire_intersection_area_sq_km
wildfire_intersection_area_ha
wildfire_intersection_area_ratio_of_grid
wildfire_max_source_size_ha
wildfire_max_calculated_size_ha
wildfire_cause_n_polygon_count
wildfire_cause_h_polygon_count
wildfire_cause_u_polygon_count
wildfire_cause_prescribed_burn_polygon_count
wildfire_cause_other_polygon_count
wildfire_has_observed_perimeter_overlap
wildfire_temporal_assignment_method
```

### Wildfire no-overlap semantics

For rows where:

```text
wildfire_temporal_assignment_method = no_observed_perimeter_overlap
```

the following semantics must hold:

```text
wildfire_has_observed_perimeter_overlap = false
has_wildfire_observed_perimeter_overlap = false
wildfire_perimeter_count = 0
wildfire_intersection_area_sq_km = 0
wildfire_intersection_area_ha = 0
wildfire_intersection_area_ratio_of_grid = 0
wildfire cause count columns = 0
```

### Wildfire overlap semantics

For rows where:

```text
wildfire_temporal_assignment_method = polygon_fire_month
```

the following semantics must hold:

```text
wildfire_has_observed_perimeter_overlap = true
has_wildfire_observed_perimeter_overlap = true
wildfire_perimeter_count >= 1
wildfire_intersection_area_sq_km > 0
wildfire_intersection_area_ha > 0
wildfire_intersection_area_ratio_of_grid between 0 and 1
```

### Latest validated wildfire coverage

```text
wildfire joined rows: 1,980,960
has_wildfire_perimeter_feature true rows: 1,980,960
has_wildfire_observed_perimeter_overlap true rows: 8,242
grid cells with wildfire overlap: 5,439
no wildfire overlap rows: 1,972,718
no wildfire overlap perimeter count sum: 0
```

---

## 12. Feature Flag Columns

The mart provides boolean feature availability flags for downstream risk scoring and modeling.

```text
has_climate_feature
has_hydro_spatial_coverage
has_hydro_flow_feature
has_hydro_level_feature
has_hydro_feature
has_wildfire_perimeter_feature
has_wildfire_observed_perimeter_overlap
```

### Flag semantics

```text
has_climate_feature =
  climate_mapping_method != no_station_within_radius

has_hydro_spatial_coverage =
  hydro_spatial_assignment_method != no_hydro_coverage

has_hydro_flow_feature =
  flow_mean_measurement_value is not null

has_hydro_level_feature =
  level_mean_measurement_value is not null

has_hydro_feature =
  has_hydro_flow_feature OR has_hydro_level_feature

has_wildfire_perimeter_feature =
  wildfire_perimeter_count is not null

has_wildfire_observed_perimeter_overlap =
  wildfire_has_observed_perimeter_overlap is true
```

### Latest validated flag counts

```text
has_climate_feature: 1,834,343
has_hydro_spatial_coverage: 1,647,480
has_hydro_flow_feature: 1,510,965
has_hydro_level_feature: 1,517,240
has_hydro_feature: 1,525,405
has_wildfire_perimeter_feature: 1,980,960
has_wildfire_observed_perimeter_overlap: 8,242
```

---

## 13. Ratio Fields

The following ratio or score fields must be between 0 and 1 when non-null:

```text
boundary_coverage_ratio
primary_municipality_grid_coverage_ratio
primary_municipality_coverage_ratio
climate_idw_confidence_score
climate_temperature_completeness_ratio
climate_precipitation_completeness_ratio
climate_data_completeness_score
hydro_basin_grid_coverage_ratio
flow_measurement_completeness_ratio
level_measurement_completeness_ratio
hydro_data_completeness_score
wildfire_intersection_area_ratio_of_grid
```

Latest validation found zero out-of-range values for all ratio fields.

---

## 14. Validation Rules

The mart validation is implemented in:

```text
src/gold/mart/validation.py
```

The validation runner is:

```text
src/gold/mart/validate_risk_monthly_grid.py
```

Validation command:

```bash
python -m src.gold.mart.validate_risk_monthly_grid
```

Latest result:

```text
[OK] Gold grid-month risk mart validation passed | checks=14
```

### Validation checks

The validation currently checks:

```text
1. row count matches 10km grid-month skeleton
2. primary key is non-null and unique
3. one row per grid_cell_key × reference_month
4. grid systems are exactly ab_10km and bc_10km
5. month range is exactly 2016-01 through 2025-12
6. all expected 10km grid cells are present
7. primary municipality assignment is populated
8. source climate/hydro/wildfire grains are one row per grid-month
9. climate method counts and no-coverage semantics are valid
10. hydro method counts and no-coverage semantics are valid
11. wildfire method counts and zero semantics are valid
12. feature flags match underlying source values
13. quality flags are valid
14. ratio fields are within [0, 1]
```

---

## 15. Build Command

The mart is built with:

```bash
python -m src.gold.mart.run_risk_monthly_grid
```

Latest successful build output:

```text
[OK] wrote Gold grid-month risk feature mart | rows=1980960 grid_cells=16508 months=120 run_id=3cd37b22-656c-4621-8c0d-90a375f85e45
```

---

## 16. Unit Tests

Relevant unit test files:

```text
tests/unit/test_gold_risk_monthly_grid_mart.py
tests/unit/test_gold_risk_monthly_grid_validation.py
```

Current unit test status:

```text
all tests passed
```

Recommended test command:

```bash
pytest tests/unit -q
```

---

## 17. Downstream Usage

This mart is the correct feature source for:

```text
Risk Scoring v1
Backtesting
GIS visualization
Power BI national dashboard
Interview/demo analytics
```

Downstream models should use the feature flags to distinguish real availability from no-coverage or no-overlap semantics.

Important examples:

```text
Climate:
no_station_within_radius means no usable climate station or interpolation coverage.

Hydro:
no_hydro_coverage means the grid-month has no hydro spatial coverage.

Wildfire:
no_observed_perimeter_overlap means confirmed zero observed wildfire perimeter overlap.
```

---

## 18. Known Non-Goals

This mart does not include:

```text
development exposure features
building permit features
property assessment features
disaster event labels
risk scores
backtesting labels
city-level 1km mart outputs
Power BI semantic model
```

Those are separate downstream phases.

---

## 19. Design Notes

### Why use a full skeleton?

The mart uses a full grid-month skeleton to make downstream scoring deterministic.

Every eligible AB/BC 10km grid cell has one row for every month from 2016-01 through 2025-12.

This makes missingness explicit and prevents scoring outputs from silently dropping grid-months.

### Why keep climate/hydro null semantics but wildfire zero semantics?

Climate and hydro values depend on observational coverage.

For climate and hydro, missing coverage is represented by method columns plus null values.

Wildfire perimeter overlap is a spatial event observation.

For wildfire, no overlap is a meaningful zero, not missing data.

### Why include feature flags?

Feature flags make downstream risk scoring safer.

They prevent models or scoring rules from confusing:

```text
missing observational coverage
confirmed zero event overlap
available measured feature
spatial coverage without observed value
```

---

## 20. Final Acceptance Criteria

This table is accepted when all of the following are true:

```text
row_count = 1,980,960
grid_cell_count = 16,508
month_count = 120
minimum_month = 2016-01
maximum_month = 2025-12
grid systems = ab_10km, bc_10km
primary key is unique
one row per grid_cell_key × reference_month
climate method counts match source
hydro method counts match source
wildfire method counts match source
climate no-coverage null semantics pass
hydro no-coverage null semantics pass
wildfire zero-overlap semantics pass
feature flag consistency checks pass
quality flag checks pass
ratio field checks pass
unit tests pass
```

Current status:

```text
accepted
```