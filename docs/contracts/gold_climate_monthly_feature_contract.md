# Gold Climate Monthly Feature Contract

## Tables

This pipeline produces two Gold climate feature tables:

```text
gold_climate_station_month_feature
gold_grid_month_climate_feature
```

## Purpose

The climate Gold layer converts ECCC daily station observations into monthly station-level and grid-level climate features for AB/BC climate risk analysis.

Climate v2 upgrades the grid-month table from sparse station-grid aggregation to a complete AB/BC 10km grid-month skeleton with explicit spatial mapping quality fields.

## Station-month table

### Table

`gold_climate_station_month_feature`

### Grain

```text
province_key × station_id × reference_month
```

### Temporal scope

```text
2016-01 through 2025-12
```

### Current validated output

```text
rows: 56,160
stations: 620
months: 120
province scope: AB, BC
```

### Key fields

```text
climate_station_month_key
station_id
province_key
reference_month
station_name
latitude
longitude
daily_record_count
temperature_observation_count
precipitation_observation_count
mean_temp_c
min_temp_c
max_temp_c
observed_min_temp_c
observed_max_temp_c
total_precip_mm
total_rain_mm
total_snow
precipitation_days
heavy_precipitation_days
extreme_heat_days
extreme_cold_days
freeze_thaw_days
temperature_completeness_ratio
precipitation_completeness_ratio
```

## Grid-month table

### Table

`gold_grid_month_climate_feature`

### Grain

```text
grid_cell_key × reference_month
```

### Spatial scope

```text
grid_system in ('ab_10km', 'bc_10km')
```

City 1km grids are intentionally excluded from climate v2 because station-based climate interpolation does not support reliable 1km urban precision.

### Temporal scope

```text
2016-01 through 2025-12
```

### Expected skeleton

```text
16,508 grid cells × 120 months = 1,980,960 rows
```

### Current validated output

```text
rows: 1,980,960
grid cells: 16,508
months: 120
AB 10km grids: 6,630
BC 10km grids: 9,878
```

## Spatial method

All grid geometry is based on EPSG:3347.

Climate station coordinates are projected from EPSG:4326 to EPSG:3347 before spatial matching and interpolation.

Climate v2 uses this mapping priority:

```text
1. direct_station_in_cell
2. direct_station_average_in_cell
3. idw_interpolated
4. no_station_within_radius
```

## Mapping method semantics

### `direct_station_in_cell`

The grid-month has exactly one station directly inside the grid cell.

The station-month feature values are used directly.

### `direct_station_average_in_cell`

The grid-month has two or more stations directly inside the grid cell.

Station-month feature values are aggregated by simple arithmetic average.

### `idw_interpolated`

The grid-month has no direct station inside the grid cell, but has at least one station within 150km.

Feature values are computed using inverse-distance weighting.

### `no_station_within_radius`

The grid-month has no direct station and no station within 150km.

Climate value fields are null.

## IDW configuration

```text
IDW radius: 150km
IDW minimum station count: 1
IDW power: 2.0
```

IDW weight formula:

```text
weight = 1 / distance^2
```

Distances are measured from grid centroid to station point in EPSG:3347.

## IDW confidence score

`climate_idw_confidence_score` ranges from 0 to 1.

Final formula:

```text
station_score = min(climate_station_count / 5, 1.0)
nearest_distance_score = max(0, 1 - climate_nearest_station_distance_km / 150)
mean_distance_score = max(0, 1 - climate_mean_station_distance_km / 150)

climate_idw_confidence_score =
    0.4 * station_score
  + 0.3 * nearest_distance_score
  + 0.3 * mean_distance_score
```

Direct station rows use:

```text
climate_idw_confidence_score = 1.0
```

No-station rows use:

```text
climate_idw_confidence_score = 0.0
```

## Quality flag semantics

`climate_feature_quality_flag` is not the same as `climate_mapping_method`.

Allowed non-null values:

```text
direct
high
medium
low
very_low
```

Rules:

```text
direct_station_in_cell / direct_station_average_in_cell:
    climate_feature_quality_flag = direct

idw_interpolated:
    quality flag is based on completeness score and confidence score

no_station_within_radius:
    climate_feature_quality_flag = null
```

`no_station_within_radius` is represented by `climate_mapping_method`, not duplicated in the quality flag.

## Null and zero semantics

Climate values must not be filled with zero when no station is available.

For `no_station_within_radius` rows:

```text
climate_station_count = 0
climate_idw_confidence_score = 0
climate value fields = null
climate_data_completeness_score = null
climate_feature_quality_flag = null
```

This is different from wildfire perimeter features, where zero can mean no observed perimeter overlap.

## Current mapping distribution

Latest validated Climate v2 output:

```text
idw_interpolated:                  1,786,619
no_station_within_radius:            146,617
direct_station_in_cell:               40,941
direct_station_average_in_cell:        6,783
```

Climate value coverage:

```text
91.62%
```

No-station share:

```text
7.40%
```

## Current quality distribution

```text
medium:   1,393,187
low:        193,200
high:       164,066
null:       146,617
direct:      47,724
very_low:    36,166
```

`null` quality rows correspond to `no_station_within_radius`.

## Validation requirements

Validation checks include:

```text
station-month row count
station-month key uniqueness
station-month month range
station province scope
station coordinate bounds
station precipitation nonnegative
station completeness ratios

grid-month complete skeleton
grid-month key uniqueness
AB/BC 10km grid scope
month range
EPSG:3347 grid CRS
mapping method validity
station count validity
distance validity
confidence score validity
no-station null semantics
mapped completeness presence
precipitation nonnegative
ratio bounds
quality flag semantics
```

Latest validation result:

```text
passed: true
checks: 22
```