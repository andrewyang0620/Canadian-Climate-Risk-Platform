# Risk Scoring Design

## 1. Purpose

The scoring layer produces a monthly **Multi-Hazard Prioritization Score** for each 10 km grid cell in Alberta and British Columbia.

It answers:

> Within a province and month, which grid cells show the strongest combined climate, hydro, and wildfire hazard conditions?

This is a retrospective prioritization index based on observed hazard conditions. It is not a disaster probability, predictive ML model, or expected-loss model.

Development data is excluded because its available geographic resolution is too coarse for defensible 10 km scoring. It may still be used for aggregate BI or descriptive analysis.

---

## 2. Input and domain weights

The scorer only reads:

```text
gold_grid_month_risk_feature_mart
```

Grain:

```text
grid_cell_key × reference_month
```

Current mart:

```text
1,980,960 rows
107 columns
2016-01 to 2025-12
```

Disaster labels are not scoring inputs and are only used later for backtesting.

Domain weights:

| Domain   | Weight |
| -------- | -----: |
| Climate  |   0.35 |
| Hydro    |   0.35 |
| Wildfire |   0.30 |

These are fixed heuristic weights rather than weights fitted to disaster outcomes.

---

## 3. Climate score

### Signals

| Signal              | Field                              | Weight |
| ------------------- | ---------------------------------- | -----: |
| Extreme heat        | `climate_extreme_heat_days`        |   0.30 |
| Heavy precipitation | `climate_heavy_precipitation_days` |   0.25 |
| Freeze-thaw         | `climate_freeze_thaw_days`         |   0.15 |
| Extreme cold        | `climate_extreme_cold_days`        |   0.15 |
| Total precipitation | `climate_total_precip_mm`          |   0.15 |

### Normalization

Climate is normalized by:

```text
province_key × calendar_month
```

using the full 2016-2025 reference period.

Because many Climate signals contain legitimate zeros, use **zero-preserving positive percentile ranking**:

```text
null  -> null
0     -> 0.0
> 0   -> percentile among positive observations
         in the same province and calendar month
```

This avoids giving non-zero hazard scores to tied zero values.

Probe results confirmed strong zero inflation. For example:

```text
Extreme heat zero rate:
AB ≈ 72%
BC ≈ 74%
```

### Score

```text
climate_sub_score =
(
    0.30 × heat
  + 0.25 × heavy_precip
  + 0.15 × freeze_thaw
  + 0.15 × cold
  + 0.15 × total_precip
)
/ available_signal_weight
```

Missing signals are excluded and available weights are renormalized.

Track:

```text
climate_signal_weight_coverage
```

as the sum of available base signal weights.

### Quality

```text
climate_spatial_quality =
    1.0                         for direct station mapping
    climate_idw_confidence_score for IDW
    0.0                         for no coverage
```

Then:

```text
climate_effective_quality =
    climate_spatial_quality
    × climate_data_completeness_score
    × climate_signal_weight_coverage
```

Quality affects final confidence, not the Climate score itself.

---

## 4. Hydro score

Hydro cannot use province-wide raw flow or level ranking because absolute values are not directly comparable across rivers and gauges.

The main Hydro signals therefore use each grid's own seasonal history.

### Signals

| Signal                      | Field / derivation            | Weight |
| --------------------------- | ----------------------------- | -----: |
| Peak flow                   | `flow_p95_measurement_value`  |   0.30 |
| Flow variability            | `flow_max - flow_min`         |   0.15 |
| Zero-flow observation ratio | derived ratio                 |   0.15 |
| Peak level                  | `level_p95_measurement_value` |   0.25 |
| Level variability           | `level_max - level_min`       |   0.15 |

Flow family weight = `0.60`
Level family weight = `0.40`

### Local historical normalization

These four signals:

```text
flow_p95
flow_variability
level_p95
level_variability
```

are percentile ranked within:

```text
grid_cell_key × calendar_month
```

using the 2016-2025 history.

Example: July 2021 flow is compared with July observations for the same grid in other years.

A minimum of:

```text
5 historical years
```

is required for a signal to receive a local percentile.

Probe results support this threshold:

```text
Flow groups with >= 5 years:
AB ≈ 99.1%
BC ≈ 99.5%

Level groups with >= 5 years:
AB ≈ 99.1%
BC ≈ 99.5%
```

If fewer than five historical observations are available, that normalized signal is null.

### Zero-flow signal

`flow_zero_day_count` cannot be interpreted as literal calendar days because Hydro aggregation combines observations from multiple stations.

Probe results showed:

```text
flow_zero_day_count max = 153
flow_observation_day_count max = 806
```

Use:

```text
flow_zero_observation_ratio =
    flow_zero_day_count
    / flow_observation_day_count
```

only when:

```text
has_hydro_flow_feature == True
and flow_observation_day_count > 0
```

This ratio is already in `[0, 1]` and is used directly.

### Score

```text
hydro_sub_score =
(
    0.30 × flow_p95
  + 0.15 × flow_variability
  + 0.15 × zero_flow_ratio
  + 0.25 × level_p95
  + 0.15 × level_variability
)
/ available_signal_weight
```

Flow-only or level-only rows can still receive a Hydro score. Missing signals reduce:

```text
hydro_signal_weight_coverage
```

rather than being filled with zero.

### Availability and quality

Use actual feature flags:

```text
has_hydro_feature
has_hydro_flow_feature
has_hydro_level_feature
```

rather than spatial coverage alone.

Hydro spatial quality:

```text
basin_polygon_intersection:
    hydro_basin_grid_coverage_ratio

station_point_in_cell:
    0.70

no coverage:
    0.0
```

Then:

```text
hydro_effective_quality =
    hydro_spatial_quality
    × hydro_data_completeness_score
    × hydro_signal_weight_coverage
```

The `0.70` point-mapping factor is a scoring confidence heuristic and does not modify Hydro Gold quality.

---

## 5. Wildfire score

Wildfire has full grid-month coverage and uses known-zero semantics:

```text
0 overlap = confirmed no observed perimeter overlap
```

Wildfire activity is very sparse:

```text
Positive grid-month rate:
AB ≈ 0.33%
BC ≈ 0.47%

Median burn ratio among positive rows:
AB ≈ 0.0069
BC ≈ 0.0088
```

Because of this distribution, a binary-occurrence plus raw-magnitude formula would be dominated by the binary term.

The score therefore uses only:

```text
wildfire_intersection_area_ratio_of_grid
```

Other wildfire fields remain available for explanation but are not separately weighted.

Normalization:

```text
burn ratio = 0
    -> wildfire_sub_score = 0

burn ratio > 0
    -> percentile among positive wildfire observations
       within the same province
```

The reference period is 2016-2025.

Wildfire quality is:

```text
wildfire_effective_quality = 1.0
```

because both positive overlap and confirmed zero overlap have explicit meanings in the mart.

---

## 6. Composite score and missing data

A composite score requires at least:

```text
2 available domains
```

Probe results show this excludes:

```text
AB grid-months: 1.07%
BC grid-months: 3.53%
Overall:        2.54%
```

For eligible rows:

```text
composite_risk_score =
    sum(base_weight × domain_score)
    / sum(weights of available domains)
```

For example:

```text
Climate = null
Hydro = 0.70
Wildfire = 0.30
```

gives:

```text
(0.35 × 0.70 + 0.30 × 0.30)
/
(0.35 + 0.30)

= 0.515
```

Missing domains are excluded rather than treated as zero.

If only Wildfire is available:

```text
composite_risk_score = null
```

The Wildfire sub-score is still retained, but a single hazard domain is not reported as a multi-hazard score.

---

## 7. Confidence

`score_confidence` represents input evidence strength.

Unlike the composite score, missing-domain weights are **not** renormalized:

```text
score_confidence =
    0.35 × climate_effective_quality
  + 0.35 × hydro_effective_quality
  + 0.30 × wildfire_effective_quality
```

Example with Climate missing and perfect Hydro/Wildfire data:

```text
score_confidence =
0.35 × 0
+ 0.35 × 1
+ 0.30 × 1
= 0.65
```

This prevents incomplete data from receiving artificially high confidence.

---

## 8. Ranking and priority tiers

Very small provincial boundary fragments are excluded from ranking.

Probe results:

```text
boundary_coverage_ratio < 0.01

AB: 27 / 6,630 grids
BC: 108 / 9,878 grids

Total: 135 / 16,508 ≈ 0.82%
```

Ranking eligibility:

```text
domain_coverage_count >= 2
AND
boundary_coverage_ratio >= 0.01
```

Rows remain in the full skeleton even when they are not ranking eligible.

Possible exclusion reasons:

```text
insufficient_domain_coverage
boundary_sliver
```

For eligible rows:

```text
priority_percentile =
percentile rank of composite_risk_score
within province_key × reference_month
```

Priority tiers:

| Percentile   | Tier                |
| ------------ | ------------------- |
| >= 0.90      | `very_high`         |
| >= 0.75      | `high`              |
| >= 0.50      | `elevated`          |
| >= 0.25      | `moderate`          |
| < 0.25       | `low`               |
| Not eligible | `insufficient_data` |

`very_high` therefore means the top 10% of eligible grids in that province and month. It is not an absolute disaster-risk category.

---

## 9. Output

Target:

```text
gold_grid_month_risk_score
```

Grain:

```text
grid_cell_key × reference_month
```

The table keeps the full `1,980,960` row skeleton.

Main output fields:

```text
risk_score_key
grid_cell_key
reference_month
province_key
grid_system

climate_sub_score
hydro_sub_score
wildfire_sub_score

climate_signal_weight_coverage
hydro_signal_weight_coverage

climate_effective_quality
hydro_effective_quality
wildfire_effective_quality

climate_domain_available
hydro_domain_available
wildfire_domain_available
domain_coverage_count

composite_risk_score
score_confidence

climate_component_contribution
hydro_component_contribution
wildfire_component_contribution

ranking_eligible
ranking_exclusion_reason
priority_percentile
priority_tier
```

Canonical scores remain in:

```text
0.0–1.0
```

For BI presentation, `priority_percentile = 0.93` may be shown as the **93rd percentile**, but not as a 93% probability of disaster.

---

## 10. Backtesting

Disaster labels are used only after scoring.

Available label period:

```text
2016-01 to 2022-09
```

Later months are outside the label window and must not be treated as negative examples.

The validation is contemporaneous rather than predictive, especially because Wildfire features describe observed perimeter overlap during the same month.

Main metrics:

```text
event_capture@10
capture_lift@10
precision@10
precision_lift@10
PR-AUC
Spearman rank correlation
Jaccard overlap of top-10% grids
```

For top 10% ranking:

```text
capture_lift@10 =
event_capture@10 / 0.10
```

Validation should primarily be summarized at event or province-month level. Disaster-expanded grid rows are spatially correlated and should not be treated as independent disaster events.

Where useful, report results separately for:

```text
all eligible labels
direct geographic mappings
excluding CSD-parent approximations
excluding low-overlap assignments
```

Domain-level diagnostics should also compare:

```text
wildfire events -> wildfire_sub_score
flood events    -> hydro_sub_score
heat/storm      -> climate_sub_score
```

---

## 11. Weight sensitivity

Baseline weights remain fixed:

```text
Climate / Hydro / Wildfire
0.35 / 0.35 / 0.30
```

Sensitivity analysis checks whether results are robust to reasonable alternatives:

```text
Equal:
0.333 / 0.333 / 0.333

Climate-heavy:
0.45 / 0.30 / 0.25

Hydro-heavy:
0.30 / 0.45 / 0.25

Wildfire-heavy:
0.30 / 0.30 / 0.40
```

The alternative weights are used for robustness checks only. Disaster labels are not used to select a new "best" set of scoring weights.

---

## 12. Limitations

* Domain and signal weights are heuristic rather than fitted.
* Normalization uses the full 2016-2025 retrospective reference period.
* Hydro local history is limited to roughly ten years.
* Hydro grid features aggregate observations from mapped stations and basins.
* Wildfire score represents observed monthly perimeter activity, not future wildfire susceptibility.
* Disaster labels have varying spatial precision.
* National scoring is limited to 10 km resolution.
* Population, asset exposure, vulnerability, and expected loss are outside the score.
* Development data is excluded because its geography is too coarse for grid-level scoring.
