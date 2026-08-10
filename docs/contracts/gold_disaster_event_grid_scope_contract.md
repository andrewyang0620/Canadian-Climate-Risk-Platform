# Gold Disaster Event Grid Scope Contract

## Table Name

`gold_disaster_event_grid_scope`

## Status

Gold spatial mapping table for disaster-event backtesting.

This table converts disaster event Census Division scope into affected 10 km grid-cell scope.

It is an intermediate label-preparation table and is not an input feature table.

## Purpose

`gold_disaster_event_grid_scope` provides the spatial relationship:

```text
one disaster event reference × one affected grid cell
```

It resolves:

```text
gold_disaster_event_cd_scope_reference
        ↓
gold_disaster_cd_spatial_reference
        ↓ spatial intersection
gold_grid_cell
        ↓
gold_disaster_event_grid_scope
```

The table preserves event lineage, source CD lineage, mapping quality, and spatial overlap metrics.

It is the immediate upstream input to:

```text
gold_grid_month_disaster_event_label
```

## Grain

One row represents:

```text
one disaster event reference × one affected grid_cell_key
```

The grain is unique by:

```text
disaster_event_reference_key
+
grid_cell_key
```

An event may map to multiple Census Divisions.

A grid cell may intersect multiple Census Divisions associated with the same event.

In that case, the output is deduplicated to one event-grid row while preserving all contributing CD keys and event-CD scope keys in JSON lineage columns.

## Source Inputs

| Source | Role |
|---|---|
| `gold_disaster_event_cd_scope_reference` | Provides grid-eligible disaster event × Census Division scope |
| `gold_disaster_cd_spatial_reference` | Provides Census Division polygons |
| `gold_grid_cell` | Provides AB/BC 10 km analysis-grid geometries |

## Target Grid Scope

Only the scoring grid systems are included:

```text
ab_10km
bc_10km
```

Current target grid count:

```text
AB 10 km grids = 6,630
BC 10 km grids = 9,878
total target grids = 16,508
```

Other grid systems or levels in `gold_grid_cell` are excluded.

## Coordinate Reference System

Both grid and Census Division geometries must use:

```text
EPSG:3347
NAD83 / Statistics Canada Lambert
```

No spatial intersection is allowed when source CRS values differ.

## Spatial Assignment Logic

For each Census Division used by a grid-eligible disaster event:

1. Load the Census Division polygon from `gold_disaster_cd_spatial_reference`.
2. Restrict candidate grids by province and geometry bounding box.
3. Intersect the Census Division polygon with `analysis_geometry_wkt` from `gold_grid_cell`.
4. Include a grid only when the intersection has positive area.
5. Exclude grids that only touch the Census Division boundary with zero intersection area.
6. Expand matching event-CD rows to event-grid rows.
7. Deduplicate the final result by event and grid.

The implementation uses:

```text
CD polygon × grid analysis geometry
```

It does not use full-cell centroids as the primary assignment rule.

This avoids incorrectly assigning clipped province-edge grids based only on the center of the original full square.

## Multi-CD Grid Handling

A grid can overlap more than one Census Division.

If the same disaster event includes all overlapping Census Divisions, the table stores one event-grid row and preserves:

```text
matched_census_division_keys_json
matched_census_division_count
source_event_cd_scope_keys_json
```

The total affected overlap area is summed across the contributing event-CD relationships and capped at the grid analysis-geometry area when converted to a coverage ratio.

## CSD Approximation

Some source events were originally mapped to Census Subdivision codes.

The current spatial reference uses Census Division polygons, so these mappings use:

```text
CSD → parent CD
```

Examples:

```text
4806016 → 4806
4816037 → 4816
5935029 → 5935
```

Rows derived from this approximation are marked with:

```text
is_csd_to_cd_approximation = true
```

The associated resolution lineage is preserved in:

```text
resolution_methods_json
source_mapped_geo_levels_json
```

CSD-to-parent-CD assignments are approximate spatial labels and must be disclosed in downstream backtesting reports.

## Required Columns

### Identity and Event Lineage

| Column | Description |
|---|---|
| `event_grid_scope_key` | Primary key for the event-grid row |
| `disaster_event_reference_key` | Gold disaster event reference key |
| `source_disaster_event_key` | Source disaster event-month key |
| `reference_month` | Event reference month |
| `event_year` | Event year |
| `event_month_number` | Event month number |

### Event Context

| Column | Description |
|---|---|
| `province_key` | Province associated with the event |
| `disaster_domain` | Normalized disaster domain |
| `location_text` | Original event location text |
| `location_tier` | Interpreted precision of the location mapping |

### Grid Identity

| Column | Description |
|---|---|
| `grid_cell_key` | Affected 10 km grid key |
| `grid_system` | `ab_10km` or `bc_10km` |
| `grid_province_key` | Province associated with the grid |
| `grid_analysis_area_sq_km` | Stored grid analysis area |
| `grid_geometry_area_sq_km` | Area calculated from the grid analysis geometry |

### Spatial and Mapping Lineage

| Column | Description |
|---|---|
| `matched_census_division_keys_json` | Contributing CD keys |
| `matched_census_division_count` | Number of contributing CDs |
| `source_event_cd_scope_keys_json` | Contributing event-CD scope keys |
| `source_mapped_geo_levels_json` | Original mapping levels |
| `resolution_methods_json` | Resolution methods such as `direct_cd` and `csd_parent_cd` |
| `mapping_confidences_json` | Mapping confidence values |
| `mapping_methods_json` | Mapping methods |

### Spatial Quality

| Column | Description |
|---|---|
| `affected_overlap_area_sq_km` | Positive intersection area assigned to the event |
| `affected_grid_coverage_ratio` | Event-CD overlap area divided by grid analysis-geometry area |
| `maximum_single_cd_coverage_ratio` | Maximum coverage ratio contributed by a single CD |
| `is_csd_to_cd_approximation` | True when any contributing mapping used CSD-to-parent-CD approximation |

### Eligibility Flags

| Column | Description |
|---|---|
| `is_backtest_window` | Must be true |
| `is_ab_bc_scope` | Must be true |
| `is_domain_relevant` | Must be true |
| `is_grid_backtest_eligible` | Must be true |

## Current Validated Output Summary

Latest validated output:

```text
row_count = 36,681
source_event_cd_scope_row_count = 113
source_grid_backtest_event_count = 36
unique_event_count = 36
unique_grid_cell_count = 9,379
source_census_division_count = 31
grid_cd_bridge_row_count = 10,193
minimum_reference_month = 2016-03
maximum_reference_month = 2021-12
```

Event-grid row counts by province:

```text
AB = 20,658
BC = 16,023
```

Event-grid row counts by disaster domain:

```text
wildfire = 13,911
flood = 13,839
severe_storm_or_climate = 8,931
climate_extreme = 0
```

Grid counts per event:

```text
minimum = 49
median = 807
mean = 1,018.9167
maximum = 2,871
```

CSD approximation:

```text
csd_approximation_event_grid_row_count = 7,234
```

## Spatial Quality Audit

The latest spatial quality audit showed:

```text
event-grid rows = 36,681
mean affected grid coverage ratio = 0.9424702
median affected grid coverage ratio = 1.0
```

Low-overlap rows:

```text
coverage ratio <= 1% = 270 rows
coverage ratio <= 5% = 627 rows
```

These rows remain in the event-grid scope because they have positive intersection area.

The final label table records whether a grid-month contains a low-overlap event so downstream backtesting can run sensitivity analyses.

## Validation

Validation runner:

```text
python -m src.gold.disaster.validate_event_grid_scope
```

Validation checks:

- Required columns exist.
- Output is nonempty.
- Primary key is non-null and unique.
- Event-grid grain is unique.
- Backtesting flags are non-null and true.
- Province, grid-system, and disaster-domain values are allowed.
- Time fields are internally consistent.
- Spatial areas are positive.
- Coverage ratios are within `(0, 1]`.
- JSON lineage fields are valid and nonempty.
- All source events are represented.
- All event-CD scope rows are represented in lineage.
- All grid keys exist in `gold_grid_cell`.
- All CD keys exist in the CD spatial reference.
- Event province and grid province agree.
- Grid system agrees with grid province.

Current validated result:

```text
checks = 14
rows = 36,681
events = 36
grids = 9,379
CDs = 31
```

## Label Semantics

This table identifies grids spatially associated with a mapped disaster-event scope.

It does not prove that every part of every included grid experienced the disaster.

For broad regional and CD_GROUP events, the affected-grid scope is an approximate validation footprint.

For CSD-to-parent-CD mappings, the footprint may be substantially broader than the source locality.

The overlap and mapping-quality fields must therefore remain available to downstream analysis.

## Relationship to the Final Label Table

This table is aggregated by:

```text
grid_cell_key
+
reference_month
```

to produce:

```text
gold_grid_month_disaster_event_label
```

Multiple event-grid rows in the same grid-month become one label row with event counts, domain counts, event lineage, and spatial-quality indicators.

## Relationship to the Feature Mart

This table must not be joined into:

```text
gold_grid_month_risk_feature_mart
```

as a predictive input.

Disaster events are realized outcomes used for validation.

Using this table as an input feature would create label leakage.

## Non-Goals

This table does not:

- Calculate risk scores.
- Perform backtesting metrics.
- Create model features.
- Claim authoritative event perimeters.
- Prove that every included grid was directly damaged.
- Include province-only or cross-province events that are not grid eligible.

## Acceptance Criteria

The table is accepted when all pass:

```text
python -m src.gold.disaster.run_event_grid_scope
python -m src.gold.disaster.validate_event_grid_scope
pytest tests/unit/test_gold_disaster_event_grid_scope.py -q
```

Expected stable high-level results:

```text
row_count = 36,681
unique_event_count = 36
unique_grid_cell_count = 9,379
unique_census_division_count = 31
```

## Ownership

This table belongs to:

```text
A3: Disaster Event Label Table
```

It implements the event-to-grid spatial assignment stage required before final grid-month label aggregation.