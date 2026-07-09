# Gold Disaster Event Reference Contract

## Table Name

`gold_disaster_event_reference`

## Status

Production Gold reference table for disaster-event backtesting and BI context.

This table replaces the previously planned `gold_grid_month_disaster_event_label` design.

The grid-month disaster label table is intentionally not built because the disaster source is sparse and many event locations are province-level, cross-province, national, or broad regional descriptions. Expanding these events into the full grid-month skeleton would create misleading spatial precision.

## Purpose

`gold_disaster_event_reference` provides a cleaned, standardized, event-level disaster reference table for:

- Power BI historical disaster trend analysis.
- Risk-score backtesting.
- Case-study validation for selected grid-level events.
- Province-month validation for broader disaster events.

This table is not a model input feature table.

It must not be joined into the risk feature mart as a predictive feature because disaster events represent realized outcomes or validation labels, not upstream risk drivers.

## Grain

One row represents one disaster event-month record from `silver_disaster_event_month`.

The table is not expanded to:

```text
grid_cell_key × reference_month
```

Reason:

- Full disaster source has 1,311 rows.
- 2016-2025 backtesting window has 74 rows.
- Province-month eligible backtesting subset has 68 rows.
- Grid-level eligible subset under mapping v1 has 52 rows.
- Expanding sparse disaster records into the full grid-month skeleton would create mostly false/null labels and misleading spatial precision.

## Source Inputs

| Source | Role |
|---|---|
| `silver_disaster_event_month` | Primary disaster event-month source |
| `configs/backtesting/disaster_location_mapping.json` | Manual location mapping metadata used for backtesting eligibility and approximate spatial reference |

## Current Source Coverage

The current Silver disaster source contains AB/BC disaster records from:

```text
1900-05 through 2022-09
```

The backtesting window flag is defined as:

```text
2016 <= event_year <= 2025
```

However, because the current disaster source ends at 2022-09, the effective available disaster validation period is:

```text
2016-01 through 2022-09
```

This limitation must be disclosed in downstream validation reports.

## Output Location

Standard Gold output path pattern:

```text
lakehouse/gold/gold_disaster_event_reference/extract_date=<YYYY-MM-DD>/run_id=<uuid>/gold_disaster_event_reference.parquet
```

Metadata path pattern:

```text
lakehouse/gold/_metadata/gold_disaster_event_reference/extract_date=<YYYY-MM-DD>/run_id=<uuid>/metadata.json
```

## Primary Key

| Column | Description |
|---|---|
| `disaster_event_reference_key` | Stable Gold reference key generated from source disaster event-month key |

Primary key requirements:

- Non-null.
- Unique.
- Uses the prefix `disaster_event_ref__`.

## Required Columns

### Source Identity

| Column | Description |
|---|---|
| `disaster_event_reference_key` | Gold primary key |
| `source_disaster_event_key` | Source event-month key from Silver |
| `source_row_number` | Source row number from the loaded Silver table |
| `source_name` | Source system name |
| `source_geometry` | Source geometry text, usually WKT point when available |
| `description` | Source event description text |

### Time and Province

| Column | Description |
|---|---|
| `province_key` | Normalized province key. Expected values: `AB`, `BC` |
| `source_province_value` | Original province value from Silver |
| `reference_month` | Event month in `YYYY-MM` format |
| `event_year` | Event year |
| `event_month_number` | Event month number |

### Disaster Classification

| Column | Description |
|---|---|
| `normalized_event_type` | Standardized event type from Silver |
| `normalized_event_subtype` | Event subtype/group from Silver |
| `disaster_domain` | Derived domain classification |
| `is_wildfire_domain_relevant` | True when disaster domain is wildfire |
| `is_flood_domain_relevant` | True when disaster domain is flood |
| `is_climate_domain_relevant` | True when event validates climate, storm, or extreme-weather scoring |
| `is_domain_relevant` | True when the event is relevant to at least one project risk domain |

Allowed `disaster_domain` values:

```text
wildfire
flood
severe_storm_or_climate
climate_extreme
other_or_unmapped
```

Domain flag rules:

```text
is_wildfire_domain_relevant = disaster_domain == wildfire
is_flood_domain_relevant = disaster_domain == flood
is_climate_domain_relevant = disaster_domain in severe_storm_or_climate / climate_extreme
is_domain_relevant = disaster_domain in wildfire / flood / severe_storm_or_climate / climate_extreme
```

### Location and Mapping Metadata

| Column | Description |
|---|---|
| `location_text` | Original location text from Silver |
| `location_text_normalized` | Normalized location text for matching |
| `location_tier` | Manual interpretation of location precision |
| `mapped_geo_level` | Manual mapped geography level |
| `mapped_geo_codes_json` | JSON list of mapped CSD/CD/CD_GROUP/province codes |
| `mapping_method` | Manual mapping method |
| `mapping_confidence` | Mapping confidence |
| `is_grid_backtest_eligible` | Whether event can be used for grid-level backtesting |
| `is_province_month_backtest_eligible` | Whether event can be used for province-month validation |

Allowed `mapping_confidence` values:

```text
high
medium
low
low_for_grid
unmapped
```

Grid-level eligible rows must satisfy:

```text
is_grid_backtest_eligible = true
mapped_geo_level in CSD / CD / CD_GROUP
mapped_geo_codes_json is a non-empty JSON list
location_tier is not province / cross_province_region / large_region / unmapped / province_or_region
```

Province-wide, cross-province, national, and overly broad regional events are not eligible for grid-level validation.

They may remain eligible for province-month validation.

### Impact Values

| Column | Description |
|---|---|
| `estimated_total_cost_cad` | Estimated cost in CAD, when available |
| `normalized_total_cost_cad` | Normalized cost in CAD, when available |
| `fatalities_total` | Fatalities, when available |
| `injured_total` | Injuries, when available |
| `evacuated_total` | Evacuated people, when available |
| `affected_total` | Affected people, when available |

All numeric impact columns must be non-negative when non-null.

### Backtesting Flags

| Column | Description |
|---|---|
| `is_backtest_window` | True when `event_year` is between 2016 and 2025 |
| `is_ab_bc_scope` | True when province is AB or BC |
| `is_backtest_eligible` | True when event is in scope for province-month backtesting |

Backtesting flag rule:

```text
is_backtest_eligible =
    is_backtest_window
    AND is_ab_bc_scope
    AND is_domain_relevant
    AND is_province_month_backtest_eligible
```

## Current Validated Output Summary

Latest validated run showed:

```text
row_count = 1,311
column_count = 36
minimum_month = 1900-05
maximum_month = 2022-09
province_counts:
  BC = 761
  AB = 550

backtest_window_event_count = 74
backtest_eligible_event_count = 68
grid_backtest_eligible_event_count = 52
```

Domain distribution:

```text
other_or_unmapped = 670
climate_extreme = 324
flood = 122
severe_storm_or_climate = 109
wildfire = 86
```

Mapping confidence distribution from mapping v1:

```text
low = 515
unmapped = 425
low_for_grid = 319
medium = 33
high = 19
```

## Mapping Configuration

Manual mapping lives in:

```text
configs/backtesting/disaster_location_mapping.json
```

This config is versioned as `v1`.

The mapping config is not treated as authoritative disaster perimeter data. It is an auditable approximation used to support backtesting.

Examples of mapping behavior:

```text
Calgary, Alberta
  -> CSD 4806016
  -> grid-level eligible

Regional Municipality of Wood Buffalo
  -> CSD 4816037
  -> grid-level eligible

Regional District of the Central Okanagan, BC
  -> CD 5935
  -> grid-level eligible

British Columbia
  -> province scope
  -> province-month eligible only
  -> not grid-level eligible

Prairie Provinces
  -> cross-province region
  -> province-month eligible only
  -> not grid-level eligible
```

## Relationship to Backtesting

This table does not perform grid intersection.

Backtesting scripts should dynamically compute affected grids by combining:

```text
gold_disaster_event_reference
configs/backtesting/disaster_location_mapping.json
StatCan Census Division / Census Subdivision spatial reference
gold_grid_cell
gold_grid_month_risk_score
```

The intended backtesting flow is:

```text
gold_disaster_event_reference
        ↓ filter is_backtest_eligible
event location mapping
        ↓
CD / CSD / CD_GROUP spatial reference
        ↓
dynamic affected-grid calculation
        ↓
risk score validation
```

No affected-grid list is persisted into this Gold table.

## Relationship to Risk Feature Mart

This table must remain independent from:

```text
gold_grid_month_risk_feature_mart
```

It must not be joined into the feature mart as an input feature.

Reason:

```text
Disaster events are realized outcomes / validation labels.
They are not upstream explanatory features.
```

## Non-Goals

This table does not:

- Create `gold_grid_month_disaster_event_label`.
- Create `gold_grid_month_disaster_feature`.
- Expand disaster events into the full grid-month skeleton.
- Store final affected grid-cell lists.
- Claim authoritative disaster perimeter boundaries.
- Replace official disaster perimeter or event footprint data.

## Validation Rules

The validation runner must enforce:

```text
python -m src.gold.disaster.validate_reference
```

Validation checks include:

- Required columns exist.
- Row count is nonzero.
- Primary key is unique and non-null.
- Province values are valid.
- `reference_month`, `event_year`, and `event_month_number` are consistent.
- Domain flags match `disaster_domain`.
- Boolean fields are non-null.
- Mapping confidence values are valid.
- `mapped_geo_codes_json` is valid JSON list.
- Grid-eligible rows have non-empty mapped geo codes.
- Grid-eligible rows use valid mapped geo levels.
- Backtesting flags are internally consistent.
- Numeric impact fields are non-negative.
- Backtesting coverage is nonzero.
- Grid-level eligible backtesting coverage is nonzero.

## Known Limitations

1. The current source ends at 2022-09, so validation does not fully cover 2023-2025.
2. Location mappings are manually curated and approximate.
3. Many historical records are province-wide, cross-province, national, or broad regional events.
4. Grid-level validation should only use rows where `is_grid_backtest_eligible = true`.
5. Province-month validation may use broader events where `is_province_month_backtest_eligible = true`.
6. Cost, evacuation, injury, and fatality fields are not guaranteed to be complete.
7. This table is suitable for validation and BI context, not model training as an input feature.

## Power BI Usage

Power BI may use this table for:

- Historical disaster trends.
- Disaster type distribution.
- Province-level disaster frequency.
- Disaster cost and impact summaries where values are available.
- Risk score validation pages.

Power BI should clearly distinguish:

```text
all historical disaster events
backtest window events
province-month eligible events
grid-level eligible events
```

## Acceptance Criteria

This table is considered complete when:

```text
python -m src.gold.disaster.run_reference
python -m src.gold.disaster.validate_reference
pytest tests/unit -q
```

all pass successfully.

The generated metadata must show:

```text
row_count > 0
backtest_eligible_event_count > 0
grid_backtest_eligible_event_count > 0
```

## Ownership

This table belongs to the Gold disaster/reference layer.

It supports:

```text
A3: Gold Disaster Event Reference
B2.5: Census Division Spatial Reference for Backtesting
B3: Risk Score Backtesting
```