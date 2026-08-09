# Gold Grid-Month Risk Score

## Purpose

`gold_grid_month_risk_score` provides a monthly multi-hazard prioritization score for the AB and BC 10 km grid.

It is a retrospective prioritization index based on observed climate, hydrometric, and wildfire conditions. It is not a disaster probability or expected-loss model.

## Grain

One row per:

`grid_cell_key × reference_month`

Reference period:

`2016-01` to `2025-12`

Input:

`gold_grid_month_risk_feature_mart`

## Domain scores

The score contains three domain subscores:

- `climate_sub_score`
- `hydro_sub_score`
- `wildfire_sub_score`

All scores use the `[0, 1]` scale.

Base domain weights:

- Climate: `0.35`
- Hydro: `0.35`
- Wildfire: `0.30`

Missing domains are not treated as zero. Available domain weights are renormalized when the composite score is calculated.

At least two domains are required for `composite_risk_score`.

## Confidence

`score_confidence` measures the strength of available scoring evidence.

Missing domain weights are not renormalized for confidence.

A high score and a high confidence value therefore represent different things.

## Ranking

Eligible rows are ranked within:

`province_key × reference_month`

Ranking requires:

- at least two available domains
- `boundary_coverage_ratio >= 0.01`

`priority_percentile` is a relative provincial monthly ranking.

Priority tiers:

- `very_high`: percentile >= 0.90
- `high`: percentile >= 0.75
- `elevated`: percentile >= 0.50
- `moderate`: percentile >= 0.25
- `low`: percentile < 0.25
- `insufficient_data`: not ranking eligible

## Missing data

Known zero and missing data are different.

Known zero hazard observations remain zero.

Missing signals or domains remain null and are excluded from the relevant weighted score.

## Validation

The output must preserve the feature-mart grid-month skeleton.

Validation checks:

- unique grid-month grain
- score values within `[0, 1]`
- domain coverage consistency
- composite eligibility and weight accounting
- component contributions sum to the composite score
- confidence matches domain quality weights
- ranking eligibility and exclusion reasons are consistent

Disaster event labels are not scoring inputs. They are used separately for backtesting.