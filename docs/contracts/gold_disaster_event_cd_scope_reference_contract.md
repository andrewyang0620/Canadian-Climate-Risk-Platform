# Gold Disaster Event CD Scope Reference Contract

## Table Name

`gold_disaster_event_cd_scope_reference`

## Status

Gold intermediate spatial-scope reference table in the completed A3 disaster-event label pipeline.

It resolves grid-eligible event-month records into normalized Census Division scope before event-to-grid spatial intersection.

## Purpose

`gold_disaster_event_cd_scope_reference` prepares disaster events for later grid-level backtesting.

It converts event-level manual location mapping into normalized Census Division scope.

This table does not perform grid intersection.

This table does not perform grid intersection or calculate affected grid cells.

Those responsibilities are implemented downstream in `gold_disaster_event_grid_scope`.

Risk-score evaluation metrics remain part of B3.

## Grain

One row represents:

```text
one disaster event reference × one resolved Census Division
```

The table expands event-level mapping into CD-level scope.

Examples:

```text
one event mapped to one CD
  -> one row

one event mapped to a CD_GROUP of 10 CDs
  -> ten rows

one event mapped to a CSD
  -> one row using parent CD approximation
```

## Source Inputs

| Source | Role |
|---|---|
| `gold_disaster_event_reference` | Provides grid-eligible disaster event-month records |
| `gold_disaster_cd_spatial_reference` | Provides valid AB/BC Census Division reference keys and metadata |

## Filtering Logic

This table only includes events where all of the following are true:

```text
is_backtest_window = true
is_ab_bc_scope = true
is_domain_relevant = true
is_grid_backtest_eligible = true
```

Current validated input count:

```text
AB/BC + 2016-2025 + domain relevant events = 68
grid-level eligible events = 36
unique grid-level eligible locations = 17
```

## Mapping Resolution Rules

The table resolves `mapped_geo_level` as follows:

| source_mapped_geo_level | Resolution rule |
|---|---|
| `CD` | Use mapped CD code directly |
| `CD_GROUP` | Expand each mapped CD code into one row |
| `CSD` | Convert CSD code to parent CD using the first 4 digits |

Unsupported mapping levels are excluded from this table.

Excluded levels include:

```text
PROVINCE
PROVINCE_GROUP
MIXED_PROVINCE_CD_GROUP
UNMAPPED
```

These broader rows may still be useful for province-month validation, but they are not grid-level CD scope rows.

## CSD Approximation

The current implementation does not use CSD polygons.

When a mapped code is CSD-level, the resolver converts it to parent CD.

Examples:

```text
4806016 -> 4806
4816037 -> 4816
5935029 -> 5935
```

Rows using this approximation are marked with:

```text
is_csd_to_cd_approximation = true
resolution_method = csd_parent_cd
```

This is acceptable for the first grid-level backtesting implementation, but it must be disclosed in backtesting reports.

## Required Columns

| Column | Description |
|---|---|
| `event_cd_scope_key` | Primary key for event-CD scope row |
| `disaster_event_reference_key` | Gold disaster event reference key |
| `source_disaster_event_key` | Source disaster event-month key |
| `reference_month` | Event reference month |
| `event_year` | Event year |
| `event_month_number` | Event month number |
| `province_key` | Event province key |
| `disaster_domain` | Disaster domain |
| `location_text` | Original event location text |
| `location_tier` | Mapping location tier |
| `source_mapped_geo_level` | Original mapped geography level |
| `source_mapped_geo_codes_json` | Original mapped code list used for this row |
| `resolved_census_division_key` | Final CD key used for backtesting scope |
| `census_division_name` | CD name from spatial reference |
| `census_division_type` | CD type from spatial reference |
| `census_division_province_key` | CD province key |
| `resolution_method` | `direct_cd` or `csd_parent_cd` |
| `is_csd_to_cd_approximation` | True when CSD was collapsed to parent CD |
| `mapping_confidence` | Mapping confidence from event reference |
| `mapping_method` | Mapping method from event reference |
| `is_backtest_window` | Must be true for all rows |
| `is_ab_bc_scope` | Must be true for all rows |
| `is_domain_relevant` | Must be true for all rows |
| `is_grid_backtest_eligible` | Must be true for all rows |

## Current Validated Output Summary

Latest validated run:

```text
row_count = 113
source_grid_backtest_event_count = 36
unique_event_count = 36
unique_census_division_count = 31
minimum_reference_month = 2016-03
maximum_reference_month = 2021-12
```

Province counts:

```text
BC = 90
AB = 23
```

CD province counts:

```text
BC = 90
AB = 23
```

Disaster domain counts:

```text
severe_storm_or_climate = 62
flood = 34
wildfire = 17
```

Mapped geography level counts:

```text
CD_GROUP = 86
CD = 16
CSD = 11
```

Resolution method counts:

```text
direct_cd = 102
csd_parent_cd = 11
```

CSD approximation rows:

```text
csd_to_cd_approximation_row_count = 11
```

## Validation Rules

Validation runner:

```text
python -m src.gold.disaster.validate_event_cd_scope
```

Validation enforces:

- Required columns exist.
- Row count is nonzero.
- `event_cd_scope_key` is unique and non-null.
- Grain is unique by `disaster_event_reference_key + resolved_census_division_key`.
- Boolean backtesting flags are true for all rows.
- `source_mapped_geo_level` is limited to `CD`, `CD_GROUP`, and `CSD`.
- `resolution_method` is limited to `direct_cd` and `csd_parent_cd`.
- `reference_month`, `event_year`, and `event_month_number` are consistent.
- `event_year` is within 2016-2025.
- All resolved CD keys exist in `gold_disaster_cd_spatial_reference`.
- Event set matches grid-eligible events from `gold_disaster_event_reference`.
- CSD rows use `csd_parent_cd`.
- Non-CSD rows use `direct_cd`.

## Relationship to the Disaster Label Pipeline

This table is an intermediate A3 input to event-grid spatial assignment.

```text
gold_disaster_event_cd_scope_reference
        +
gold_disaster_cd_spatial_reference
        +
gold_grid_cell
        ↓
gold_disaster_event_grid_scope
        ↓
gold_grid_month_disaster_event_label
        ↓
B3 risk-score backtesting
```

## Relationship to Other Disaster Tables

### Upstream: gold_disaster_event_reference

`gold_disaster_event_reference` stores the full disaster event reference set.

Current validated numbers:

```text
full disaster reference rows = 1,311
AB/BC + 2016-2025 + domain relevant rows = 68
grid-level eligible rows = 36
```

Only the 36 grid-level eligible event rows flow into this CD scope table.

### Upstream: gold_disaster_cd_spatial_reference

`gold_disaster_cd_spatial_reference` stores AB/BC Census Division spatial reference records.

Current validated numbers:

```text
row_count = 48
BC Census Divisions = 29
AB Census Divisions = 19
```

All `resolved_census_division_key` values in this table must exist in `gold_disaster_cd_spatial_reference`.

## Non-Goals

This table does not:

- Store CD polygons.
- Store affected grid-cell lists.
- Perform polygon intersection.
- Produce score validation metrics.
- Replace event reference or CD spatial reference.
- Act as a model input feature.

## Known Limitations

1. CSD-level mappings are approximated to parent CD because CSD polygons are not part of the current B2.5 implementation.
2. CD_GROUP rows may represent broad regional impact areas and should be interpreted as approximate validation scope.
3. This table only covers grid-level eligible events, not all province-month eligible events.
4. The current disaster source ends at 2022-09, so the available validation period is effectively 2016-01 through 2022-09.
5. This table prepares scope for backtesting but does not prove model performance by itself.

## Acceptance Criteria

This table is considered valid when all pass:

```text
python -m src.gold.disaster.run_event_cd_scope
python -m src.gold.disaster.validate_event_cd_scope
pytest tests/unit/test_gold_disaster_event_cd_scope.py -q
```

Expected stable high-level results:

```text
source_grid_backtest_event_count = 36
unique_event_count = 36
row_count = 113
unique_census_division_count = 31
```

## Ownership

This table belongs to the disaster/hazard backtesting preparation layer.

It supports113
unique_census_division_count = 31
```

## Ownership

This table belongs to the disaster/hazard backtesting preparation layer.

It supports:

```text
B3: grid-level disaster risk score backtesting
```