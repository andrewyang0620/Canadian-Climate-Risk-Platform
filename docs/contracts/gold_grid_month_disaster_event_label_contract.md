# Gold Grid-Month Disaster Event Label Contract

## Table Name

`gold_grid_month_disaster_event_label`

## Status

Final Gold disaster label table for risk-score backtesting.

This table completes the A3 disaster-event label workflow.

It is a validation-label table and is not a model feature table.

## Purpose

`gold_grid_month_disaster_event_label` provides observed disaster-event labels at the same spatial and monthly grain used by the risk-scoring system:

```text
one grid_cell_key × one reference_month
```

It supports downstream scoring validation, including event capture, lift, ranking analysis, and sensitivity analysis.

It must remain separate from predictive features to prevent label leakage.

## Grain

One row represents:

```text
one 10 km grid cell × one observed reference month
```

The grain is unique by:

```text
grid_cell_key
+
reference_month
```

## Source Inputs

| Source | Role |
|---|---|
| `gold_disaster_event_grid_scope` | Supplies event × affected-grid assignments |
| `gold_disaster_event_reference` | Defines disaster-source temporal coverage |
| `gold_grid_cell` | Supplies the complete AB/BC 10 km grid universe |

## Target Grid Universe

Only these scoring grids are included:

```text
ab_10km
bc_10km
```

Current grid universe:

```text
AB grids = 6,630
BC grids = 9,878
total grids = 16,508
```

## Label Observation Window

The configured backtesting period begins at:

```text
2016-01
```

The label table ends at the earlier of:

```text
disaster source maximum reference month
or
2025-12
```

Current disaster source coverage ends at:

```text
2022-09
```

Therefore the current label table covers:

```text
2016-01 through 2022-09
```

This is 81 months.

Months after 2022-09 are not generated as negative labels because the disaster source does not provide observable coverage for those months.

## Complete Skeleton

The output contains the complete target grid-month skeleton for the observable label window:

```text
16,508 grids × 81 months = 1,337,148 rows
```

Every target grid appears exactly once in every observed month.

## Label Semantics

### Positive Label

```text
disaster_event_occurred = true
```

means at least one grid-eligible disaster event was spatially assigned to the grid-month.

### Zero Label

```text
disaster_event_occurred = false
```

means:

```text
no recorded grid-eligible event from the current disaster source
was spatially assigned to this grid-month
```

It does not prove that no disaster occurred.

Possible reasons for a zero label include:

- No recorded disaster event occurred.
- An event was absent from the source database.
- An event location could not be mapped at grid level.
- An event was too broad for grid-level eligibility.
- An event existed but was not included in the current disaster domains.

This limitation must remain explicit in metadata and backtesting reports.

## Required Columns

### Primary Key and Grain

| Column | Description |
|---|---|
| `grid_month_disaster_label_key` | Primary key |
| `grid_cell_key` | 10 km scoring grid |
| `reference_month` | Label month in `YYYY-MM` format |
| `event_year` | Year derived from reference month |
| `event_month_number` | Month number derived from reference month |

### Grid Context

| Column | Description |
|---|---|
| `province_key` | Grid province |
| `grid_system` | `ab_10km` or `bc_10km` |
| `label_is_observed` | True for every generated row in the observable source period |

### Core Label Fields

| Column | Description |
|---|---|
| `disaster_event_occurred` | True when event count is greater than zero |
| `disaster_event_count` | Number of distinct disaster-event references assigned to the grid-month |
| `disaster_event_types` | Sorted comma-separated disaster domains |
| `disaster_event_reference_keys_json` | Sorted distinct event-reference keys |

### Disaster-Domain Counts

| Column | Description |
|---|---|
| `wildfire_event_count` | Wildfire event count |
| `flood_event_count` | Flood event count |
| `storm_or_climate_event_count` | Severe storm or climate-event count |
| `climate_extreme_event_count` | Climate-extreme event count |

Domain counts must satisfy:

```text
wildfire_event_count
+ flood_event_count
+ storm_or_climate_event_count
+ climate_extreme_event_count
=
disaster_event_count
```

### Spatial Resolution Counts

| Column | Description |
|---|---|
| `direct_cd_resolution_event_count` | Events with at least one direct-CD lineage |
| `csd_parent_cd_event_count` | Events with CSD-to-parent-CD resolution |
| `cd_scope_event_count` | Events with CD source scope |
| `cd_group_scope_event_count` | Events with CD_GROUP source scope |
| `csd_scope_event_count` | Events with CSD source scope |

These columns describe event-level spatial lineage in the grid-month.

They are not mutually exclusive when one event-grid record contains lineage from multiple source CDs or methods.

### Quality Counts and Flags

| Column | Description |
|---|---|
| `approximate_event_count` | Count of events using CSD-to-parent-CD approximation |
| `low_overlap_event_count` | Count of events with grid coverage ratio less than or equal to 5% |
| `has_csd_parent_cd_approximation` | True when approximate event count is greater than zero |
| `has_low_overlap_event` | True when low-overlap event count is greater than zero |

### Coverage Quality

| Column | Description |
|---|---|
| `minimum_event_grid_coverage_ratio` | Minimum event-grid coverage ratio among events in the grid-month |
| `mean_event_grid_coverage_ratio` | Mean event-grid coverage ratio |
| `maximum_event_grid_coverage_ratio` | Maximum event-grid coverage ratio |

For positive rows:

```text
0 < minimum <= mean <= maximum <= 1
```

For zero-label rows, all coverage-quality fields must be null.

## Current Validated Output Summary

Latest validated output:

```text
row_count = 1,337,148
grid_count = 16,508
month_count = 81
minimum_reference_month = 2016-01
maximum_reference_month = 2022-09
```

Label distribution:

```text
positive_label_row_count = 36,051
negative_label_row_count = 1,301,097
positive_label_rate = 0.0269611143
```

Spatial coverage:

```text
unique_positive_grid_count = 9,379
```

Event assignment reconciliation:

```text
source_event_grid_row_count = 36,681
total_disaster_event_assignments = 36,681
maximum_events_per_grid_month = 2
```

Domain assignments:

```text
wildfire = 13,911
flood = 13,839
storm_or_climate = 8,931
climate_extreme = 0
total = 36,681
```

Spatial-quality indicators:

```text
positive rows with CSD approximation = 7,234
positive rows with low-overlap event = 615
```

Province skeleton:

```text
BC = 800,118 rows
AB = 537,030 rows
```

These values reconcile as:

```text
9,878 BC grids × 81 months = 800,118
6,630 AB grids × 81 months = 537,030
```

## Multiple Events in One Grid-Month

A grid-month may contain more than one event.

Current maximum:

```text
maximum_events_per_grid_month = 2
```

Therefore:

```text
positive_label_row_count < total_disaster_event_assignments
```

Current difference:

```text
36,681 assignments - 36,051 positive grid-months = 630
```

This is expected aggregation, not data loss.

## Validation

Validation runner:

```text
python -m src.gold.disaster.validate_grid_month_label
```

Validation checks:

- Required columns exist.
- Output is nonempty.
- Primary key is non-null and unique.
- Grid-month grain is unique.
- Complete grid-month skeleton exists.
- Grid keys, provinces, and grid systems match `gold_grid_cell`.
- Observed months match disaster-source coverage.
- Time fields are internally consistent.
- All count columns are non-negative integers.
- Disaster-domain counts sum to total event count.
- Boolean fields match their count definitions.
- Zero-label rows have zero counts, empty lineage, and null quality metrics.
- Positive rows contain event lineage and quality metrics.
- Event JSON lineage count matches distinct event count.
- Coverage ratios satisfy minimum/mean/maximum ordering.
- Final labels reconcile exactly to a fresh aggregation of `gold_disaster_event_grid_scope`.

Current validated result:

```text
checks = 14
rows = 1,337,148
grids = 16,508
months = 81
positive rows = 36,051
assignments = 36,681
```

## Backtesting Usage

This table is intended for B3 risk-score validation.

Typical evaluation joins:

```text
gold_grid_month_risk_score
        +
gold_grid_month_disaster_event_label
```

Join grain:

```text
grid_cell_key
+
reference_month
```

Recommended backtesting views:

1. All grid-eligible labels.
2. Excluding rows with CSD-to-parent-CD approximation.
3. Excluding rows with low-overlap events.
4. Direct-CD-only labels.
5. Disaster-domain-specific labels.

This allows the report to distinguish model behavior from spatial-mapping assumptions.

## Relationship to the Feature Mart

This table must not enter:

```text
gold_grid_month_risk_feature_mart
```

It is an outcome/validation label.

Joining it into predictive features before scoring would create label leakage.

The correct order is:

```text
features
    ↓
risk score
    ↓
join labels after scoring
    ↓
backtesting
```

## Known Limitations

1. The disaster source ends at 2022-09.
2. Zero labels mean no recorded mapped event, not confirmed absence of disaster.
3. Manual location mappings are approximate.
4. Broad regional events may cover many grids.
5. CSD events are currently expanded through parent Census Division polygons.
6. Some positive grid assignments have very low polygon-overlap ratios.
7. The source is not an authoritative event-perimeter database.
8. The label table is suitable for prioritization-score validation, not precise damage-footprint modeling.

## Non-Goals

This table does not:

- Predict disasters.
- Create risk-score features.
- Replace official event footprints.
- Prove absence of disaster for zero-label rows.
- Calculate event-capture or lift metrics.
- Contain unobservable 2022-10 through 2025-12 negative labels.

## Acceptance Criteria

The table is accepted when all pass:

```text
python -m src.gold.disaster.run_grid_month_label
python -m src.gold.disaster.validate_grid_month_label
pytest tests/unit/test_gold_grid_month_disaster_event_label.py -q
```

Expected stable results:

```text
row_count = 1,337,148
grid_count = 16,508
month_count = 81
positive_label_row_count = 36,051
total_disaster_event_assignments = 36,681
```

## Ownership

This table completes:

```text
A3: Disaster Event Label Table
```

It is the final disaster label input required by:

```text
B3: Risk Score Backtesting
```