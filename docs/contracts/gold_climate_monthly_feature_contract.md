# Gold Hydro Monthly Feature Contract

## 1. Purpose

This document defines the contract for the Gold hydro monthly feature outputs.

The Hydro v2 design converts hydrometric station observations and basin polygon coverage into monthly grid-level hydro features for the Canadian climate risk platform.

The main goal is to produce a full grid-month skeleton for AB and BC 10km grid cells, while preserving clear data-quality semantics:

- basin polygon intersection is the primary spatial assignment method
- station point-in-cell is used only as a limited fallback for observed stations without basin polygons
- no hydro coverage must remain null, not zero-filled risk
- hydro features are monthly and grid-aligned for later feature mart and risk scoring work

## 2. Output Tables

### 2.1 gold_hydro_station_month_feature

Grain:

- province_key
- station_id
- measurement_type
- reference_month

Current validated output:

- row count: 158,831
- station count: 981
- month count: 120
- month range: 2016-01 to 2025-12
- measurement types:
  - flow
  - level
- provinces:
  - AB
  - BC

This table aggregates daily hydrometric observations into station-month features.

### 2.2 gold_grid_month_hydro_feature

Grain:

- grid_cell_key
- reference_month

Current validated output:

- row count: 1,980,960
- grid cell count: 16,508
- month count: 120
- month range: 2016-01 to 2025-12
- grid systems:
  - ab_10km
  - bc_10km

The row count is expected to equal:

    16,508 grid cells × 120 months = 1,980,960 rows

This table is a full grid-month skeleton. Every AB/BC 10km grid cell has one row per month, even when no hydro station or basin coverage exists.

## 3. Spatial Assignment Rules

Hydro v2 uses the following spatial assignment priority.

### Priority 1: basin_polygon_intersection

Method name:

    basin_polygon_intersection

Definition:

A hydrometric station has a basin polygon, and that basin polygon intersects a 10km grid cell.

This is the primary and preferred assignment method because basin polygons represent hydrological contributing areas better than station point locations.

Current validated coverage:

- basin matched observed stations: 948
- basin unmatched observed stations: 33
- basin match rate: 96.64%
- basin intersection rows: 123,649
- basin covered grids: 13,727
- basin grid-month rows: 1,647,240

Rules:

- basin intersection area must be positive
- basin grid coverage ratio must be between 0 and 1
- basin fields must be populated:
  - hydro_basin_intersection_area_sq_km
  - hydro_basin_grid_coverage_ratio
- hydro_point_station_count must be 0
- hydro_station_count must equal hydro_basin_station_count

### Priority 2: station_point_in_cell

Method name:

    station_point_in_cell

Definition:

A station does not have a matched basin polygon, but its projected station point physically falls inside a 10km grid cell.

This is a strict fallback. It is not a radius search and not a nearest-neighbor assignment.

Current validated coverage:

- point-in-cell grids: 2
- point-in-cell grid-month rows: 240

Rules:

- only used for observed stations without basin polygons
- only point-in-cell containment is allowed
- no nearest-grid fallback is allowed
- no distance-radius fallback is allowed
- basin fields must be null:
  - hydro_basin_intersection_area_sq_km
  - hydro_basin_grid_coverage_ratio
- hydro_basin_station_count must be 0
- hydro_station_count must equal hydro_point_station_count
- quality flag must not be high
- quality flag may be null when the station has no monthly observation values

### Priority 3: no_hydro_coverage

Method name:

    no_hydro_coverage

Definition:

A grid cell has no basin polygon intersection and no valid station point-in-cell fallback.

Current validated coverage:

- no-coverage grids: 2,779
- no-coverage grid-month rows: 333,480

Rules:

- hydro station counts must be 0
- flow count fields must be 0
- level count fields must be 0
- flow value fields must be null
- level value fields must be null
- hydro_data_completeness_score must be null
- hydro_feature_quality_flag must be null
- basin area and basin ratio fields must be null

Important interpretation:

No hydro coverage is not the same as low risk. It means the hydro feature is unavailable for that grid-month.

## 4. gold_grid_month_hydro_feature Columns

### 4.1 Identity Columns

- grid_month_hydro_feature_key
- grid_cell_key
- grid_system
- grid_level
- grid_version
- province_key
- reference_month

Key rule:

    grid_month_hydro_feature_key = grid_cell_key + "__" + reference_month

The key must be unique and non-null.

### 4.2 Spatial Assignment Columns

- hydro_spatial_assignment_method
- hydro_station_count
- hydro_basin_station_count
- hydro_point_station_count
- hydro_basin_intersection_area_sq_km
- hydro_basin_grid_coverage_ratio

Allowed spatial assignment methods:

- basin_polygon_intersection
- station_point_in_cell
- no_hydro_coverage

### 4.3 Flow Feature Columns

- flow_station_count
- flow_daily_record_count
- flow_observation_day_count
- flow_measurement_observation_count
- flow_mean_measurement_value
- flow_min_measurement_value
- flow_max_measurement_value
- flow_median_measurement_value
- flow_p95_measurement_value
- flow_measurement_completeness_ratio
- flow_zero_day_count
- flow_negative_value_count

Flow rules:

- flow values must be null when flow_station_count = 0
- flow count fields must be 0 when flow_station_count = 0
- flow values must be populated when flow_station_count > 0
- flow_min_measurement_value must not be negative
- flow_negative_value_count must be 0
- flow_measurement_completeness_ratio must be between 0 and 1 when populated

### 4.4 Level Feature Columns

- level_station_count
- level_daily_record_count
- level_observation_day_count
- level_measurement_observation_count
- level_mean_measurement_value
- level_min_measurement_value
- level_max_measurement_value
- level_median_measurement_value
- level_p95_measurement_value
- level_measurement_completeness_ratio
- level_negative_value_count

Level rules:

- level values must be null when level_station_count = 0
- level count fields must be 0 when level_station_count = 0
- level values must be populated when level_station_count > 0
- negative level values are allowed
- level_measurement_completeness_ratio must be between 0 and 1 when populated

### 4.5 Quality Columns

- hydro_data_completeness_score
- hydro_feature_quality_flag

Allowed quality flags:

- high
- medium
- low
- very_low
- null

Quality flag rules:

- no_hydro_coverage must have null quality
- rows with null hydro_data_completeness_score must have null quality
- rows with non-null hydro_data_completeness_score and hydro coverage must have a quality flag
- station_point_in_cell rows must not have high quality
- covered grid-month rows may still have null quality if no monthly flow or level values are available

Current validated quality flag counts:

- high: 1,392,073
- medium: 112,085
- low: 15,529
- very_low: 5,718
- null: 455,555

## 5. Aggregation Rules

### 5.1 Station-Month Aggregation

Daily hydrometric observations are aggregated by:

- province_key
- station_id
- measurement_type
- reference_month

Station-month metrics include:

- daily record count
- observation day count
- measurement observation count
- mean value
- min value
- max value
- median value
- p95 value
- measurement completeness ratio
- symbol counts
- flow zero-day count
- negative value count

### 5.2 Grid-Month Aggregation

Station-month records are joined to grid cells using the spatial mapping.

For weighted fields, the grid-month value is calculated using spatial_weight:

- mean_measurement_value
- median_measurement_value
- p95_measurement_value
- measurement_completeness_ratio

The weighted average must ignore null values. Each weighted field uses its own valid-value weight denominator.

For count fields, values are summed:

- daily_record_count
- observation_day_count
- measurement_observation_count
- flow_zero_day_count
- negative_value_count

For station counts, distinct station_id is used.

For min/max fields:

- min_measurement_value uses minimum
- max_measurement_value uses maximum

## 6. Validated Output Summary

The latest validated Hydro v2 output passed 28 validation checks.

Station-month output:

- rows: 158,831
- stations: 981
- months: 120
- measurement types: flow, level
- provinces: AB, BC

Grid-month output:

- rows: 1,980,960
- grid cells: 16,508
- months: 120
- grid systems: ab_10km, bc_10km
- unique keys: 1,980,960

Spatial assignment by grid:

- basin_polygon_intersection: 13,727
- station_point_in_cell: 2
- no_hydro_coverage: 2,779

Spatial assignment by grid-month:

- basin_polygon_intersection: 1,647,240
- station_point_in_cell: 240
- no_hydro_coverage: 333,480

Value coverage:

- flow_mean_measurement_value non-null rows: 1,510,965
- flow_mean_measurement_value coverage: 76.27%
- level_mean_measurement_value non-null rows: 1,517,240
- level_mean_measurement_value coverage: 76.59%

No-coverage semantics:

- no_hydro_coverage flow non-null rows: 0
- no_hydro_coverage level non-null rows: 0
- no_hydro_coverage quality non-null rows: 0
- no_hydro_coverage station count sum: 0

Basin ratio checks:

- maximum hydro_basin_grid_coverage_ratio: 1.0
- rows with hydro_basin_grid_coverage_ratio > 1: 0

## 7. Validation Contract

The validation must check:

1. station-month output is non-empty
2. station-month key is unique and non-null
3. station-month month range is 2016-01 to 2025-12
4. station-month provinces are AB and BC
5. station-month measurement types are flow and level
6. station coordinates are within expected AB/BC bounds
7. station-month count fields are internally consistent
8. station-month completeness ratio is between 0 and 1
9. station-month measurement values are internally ordered
10. flow values are non-negative
11. station symbol counts are internally consistent
12. grid-month row count equals 16,508 × 120
13. grid-month key is unique and non-null
14. grid-month month range is 2016-01 to 2025-12
15. grid-month provinces are AB and BC
16. grid-month systems are ab_10km and bc_10km
17. grid-month skeleton covers all valid 10km grid cells
18. spatial assignment methods are valid
19. production spatial assignment counts match expected Hydro v2 counts
20. each grid has exactly one spatial assignment method
21. hydro station counts are non-negative and internally consistent
22. no_hydro_coverage rows have null values and zero counts
23. station_point_in_cell rows have no basin fields and no high quality
24. basin_polygon_intersection rows have positive basin area and ratio between 0 and 1
25. flow feature semantics are valid
26. level feature semantics are valid
27. hydro ratio fields are between 0 and 1
28. quality flags are valid and consistent with completeness score
29. flow and level value coverage is non-zero

## 8. Notes for Downstream Use

Downstream marts and risk scores must not treat null hydro values as zero risk.

Recommended interpretation:

- basin_polygon_intersection:
  - strongest hydro spatial assignment
- station_point_in_cell:
  - limited fallback assignment
  - lower spatial confidence than basin polygons
- no_hydro_coverage:
  - no hydro feature coverage
  - must be handled as missing / unavailable hydro evidence

Future integrated risk marts should preserve:

- hydro_spatial_assignment_method
- hydro_data_completeness_score
- hydro_feature_quality_flag

These columns are necessary for explainability and quality-aware scoring.