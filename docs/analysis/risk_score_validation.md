# Risk Score Historical Validation

## 1. Purpose

This analysis evaluates whether `gold_grid_month_risk_score` provides useful spatial prioritization against mapped historical disaster events in Alberta and British Columbia.

The score is a retrospective monthly multi-hazard prioritization index. This validation does not test future disaster prediction, event probability, or expected loss.

The main question is:

> Within the same province and month, are grids associated with recorded disaster events ranked higher than other comparable grids?

Disaster labels are used only for validation and were not used to construct or tune the baseline risk score.

---

## 2. Validation data

The grid-level validation set contains:

- 22 underlying source disaster events
- 36 event-month observations
- 36,681 event-grid assignments
- event-month coverage from 2016-03 to 2021-12
- 11 Alberta source events
- 11 British Columbia source events

The source events include:

- 7 wildfire events
- 6 flood events
- 9 severe storm or climate events

Multi-month disasters are evaluated by month first and then aggregated to the underlying source event. This prevents long-running events from receiving disproportionate weight in the final summary.

Only grid-mapped disaster events are included in this analysis. Province-wide or otherwise unmapped events are not treated as grid-level positives.

---

## 3. Method

### 3.1 Composite score validation

For each event-month:

1. Select the event's mapped affected grids.
2. Restrict comparison to the same province and reference month.
3. Keep grids eligible for risk ranking.
4. Use grids with no recorded grid-eligible disaster event in that province-month as controls.
5. Compare affected and control grid rankings.

This avoids using grids belonging to another recorded event in the same province-month as controls.

The primary metrics are:

- `event_capture_at_10`: share of rankable affected grids found in the top 10% of provincial monthly priority rankings
- `capture_lift_at_10`: capture divided by the 10% random-ranking baseline
- `event_auc`: rank-based discrimination between affected and control grids

An AUC of 0.5 represents no rank discrimination. Values above 0.5 indicate that affected grids tend to receive higher scores.

Metrics are calculated at the event-month level and averaged within each source event before overall aggregation.

### 3.2 Domain diagnostics

Each disaster domain is also evaluated against its corresponding sub-score:

- flood → `hydro_sub_score`
- severe storm or climate → `climate_sub_score`
- wildfire → `wildfire_sub_score`

Domain validation uses grids with sufficient boundary coverage and a non-null relevant domain score. It does not require composite-score eligibility.

### 3.3 Robustness checks

Two sensitivity analyses are used.

Label-quality sensitivity compares the baseline mapped footprints with:

- CSD-to-parent-CD approximations excluded
- grid overlaps of 5% or less excluded
- both filters applied

Weight sensitivity compares the baseline domain weights with equal, climate-heavy, hydro-heavy, and wildfire-heavy alternatives.

The sensitivity analysis is used to test robustness, not to optimize the baseline weights.

---

## 4. Overall results

| Metric | Mean | Median |
|---|---:|---:|
| Event capture @ top 10% | 19.3% | 6.9% |
| Capture lift @ top 10% | 1.93× | 0.69× |
| Event AUC | 0.540 | 0.507 |

On average, the top 10% of ranked grids captured 19.3% of mapped historical disaster footprints, corresponding to 1.93 times the random-ranking baseline.

The difference between the mean and median is important. Performance is heterogeneous across events: several events show strong spatial alignment, while the typical event is much closer to random ranking.

The composite score should therefore be interpreted as providing a modest positive prioritization signal overall rather than uniformly strong disaster discrimination.

---

## 5. Provincial results

The overall result hides a large difference between Alberta and British Columbia.

| Province | Source events | Mean capture @10 | Mean lift @10 | Mean AUC |
|---|---:|---:|---:|---:|
| Alberta | 11 | 2.7% | 0.27× | 0.374 |
| British Columbia | 11 | 35.8% | 3.58× | 0.706 |

British Columbia shows materially stronger historical alignment. Its top 10% priority grids captured 35.8% of mapped event footprints on average, with a mean event AUC of 0.706.

Alberta shows substantially weaker measured discrimination.

This provincial split is relevant because priority percentiles are defined within `province_key × reference_month`; the score does not rank Alberta and British Columbia against one another.

---

## 6. Spatial-label precision

The provincial performance gap coincides with a substantial difference in the spatial resolution of the available disaster labels.

| Measure | Alberta | British Columbia |
|---|---:|---:|
| Source events | 11 | 11 |
| CSD → parent-CD events | 7 | 1 |
| Direct-CD events | 4 | 10 |
| Broad-footprint events | 6 | 3 |
| Mean affected share of rankable province-month grids | 12.7% | 8.7% |
| Median affected share | 16.1% | 4.6% |
| Median affected grid count | 1,081 | 450 |

The median Alberta event footprint covers approximately 16% of its province-month rankable grid universe, compared with approximately 5% in British Columbia.

The difference is not limited to CSD-to-parent-CD approximations. Several directly mapped Alberta regional events are also spatially broad. For example, mapped footprints for High Level and Fort Vermilion cover roughly 30% of their province-month grid universes.

This matters because the disaster labels represent administrative event scopes rather than authoritative physical damage perimeters. When a localized disaster is represented by a broad administrative footprint, many grids may be labelled as affected even though the physical hazard signal is concentrated in a smaller area. This dilutes affected-grid score distributions and makes grid-level discrimination harder.

The Alberta result should therefore be interpreted with this spatial-label limitation in mind.

The available data does not establish that spatial overmapping fully explains the provincial difference. British Columbia also contains some broad regional event footprints that score well. The evidence supports label precision as an important confounding factor, not as the sole explanation for weaker Alberta results.

---

## 7. Domain diagnostics

Domain-level results are stronger than the full composite for Hydro and Climate.

| Disaster domain | Source events | Mean AUC | Median AUC | Mean score gap | Affected score coverage |
|---|---:|---:|---:|---:|---:|
| Flood / Hydro | 6 | 0.671 | 0.714 | +0.110 | 86.5% |
| Severe storm / Climate | 9 | 0.654 | 0.813 | +0.055 | 99.4% |
| Wildfire | 7 | 0.523 | 0.514 | +0.025 | 99.5% |

### Hydro

Hydro provides the clearest domain-level discrimination.

Affected flood grids receive substantially higher Hydro scores on average, with a mean AUC of 0.671 and median AUC of 0.714.

The lower Hydro coverage reflects the spatial availability of hydrometric observations and assignments rather than zero hazard.

### Climate

Climate also shows useful historical alignment. Mean AUC is 0.654 and median AUC is 0.813.

Performance is heterogeneous: several climate-related events align strongly with the Climate sub-score while others do not.

### Wildfire

Wildfire discrimination is weak, with mean and median AUC close to 0.5.

The current Wildfire sub-score measures contemporaneous observed perimeter overlap. Disaster-event labels, however, frequently represent broader administrative affected areas rather than the physical burn perimeter. These two spatial concepts are not expected to align perfectly.

This result is treated as a limitation of the current validation and feature representation rather than evidence of predictive wildfire skill.

---

## 8. Label-quality sensitivity

Removing CSD-to-parent-CD approximations produces a smaller but spatially more precise event subset.

| Scenario | Source events | Mean capture @10 | Mean lift @10 | Mean AUC | Median AUC |
|---|---:|---:|---:|---:|---:|
| Baseline | 22 | 19.3% | 1.93× | 0.540 | 0.507 |
| Exclude CSD approximation | 14 | 23.1% | 2.31× | 0.602 | 0.620 |
| Exclude low overlap | 22 | 19.2% | 1.92× | 0.540 | 0.506 |
| Exclude CSD + low overlap | 14 | 23.0% | 2.30× | 0.601 | 0.619 |

The higher-spatial-precision subset shows stronger discrimination than the full sample.

Because the event sample changes from 22 to 14 source events, this comparison should not be interpreted as a causal estimate of the effect of mapping quality. It does, however, provide supporting evidence that coarse administrative footprints materially affect measured grid-level validation performance.

Removing grid intersections with 5% or less overlap has almost no effect on the results. Small polygon-edge overlaps are therefore not a major driver of the validation outcome.

The more important spatial issue is the breadth of the mapped administrative event footprint itself.

---

## 9. Domain sensitivity to label quality

Higher-precision mapping particularly strengthens Hydro and Climate validation.

| Domain | Baseline mean AUC | Excluding CSD approximation |
|---|---:|---:|
| Flood / Hydro | 0.671 | 0.827 |
| Severe storm / Climate | 0.654 | 0.704 |
| Wildfire | 0.523 | 0.525 |

Hydro shows the largest improvement, reaching a mean event AUC of approximately 0.83 in the higher-precision subset.

Wildfire remains close to random even after removing CSD approximations. Its weaker alignment therefore cannot be explained by that mapping issue alone.

---

## 10. Weight robustness

The baseline composite uses:

- Climate: 0.35
- Hydro: 0.35
- Wildfire: 0.30

Reasonable alternative weight scenarios produce only modest changes in validation metrics and preserve the underlying provincial monthly ranking structure.

| Scenario | Mean AUC | Mean capture @10 | Mean Spearman vs baseline | Mean top-10 Jaccard |
|---|---:|---:|---:|---:|
| Baseline | 0.540 | 19.3% | 1.000 | 1.000 |
| Equal | 0.541 | 19.3% | 0.999 | 0.974 |
| Climate-heavy | 0.558 | 19.2% | 0.977 | 0.780 |
| Hydro-heavy | 0.528 | 19.2% | 0.985 | 0.793 |
| Wildfire-heavy | 0.542 | 19.6% | 0.996 | 0.937 |

The near-identical equal-weight result and high rank correlations across all scenarios indicate that the validation result is not dependent on a narrowly chosen set of heuristic weights.

The alternative weights are not used to retune the baseline score.

---

## 11. Representative events

Several events show strong composite prioritization.

### Kootenay Boundary flood — May 2018

- Composite event AUC: 0.937
- Capture @ top 10%: 82.9%
- Capture lift: 8.29×
- Hydro AUC: 0.966

This is the strongest combined composite and domain-level result in the validation sample.

### South Coast and Lower Mainland storm — March 2016

- Composite event AUC: 0.837
- Capture @ top 10%: 69.6%
- Capture lift: 6.96×
- Climate AUC: 0.860

The Climate sub-score and composite ranking both strongly prioritize the mapped event footprint.

### Southern British Columbia flood — November–December 2021

- Mean composite event AUC: 0.834
- Mean capture @ top 10%: 31.0%
- Mean capture lift: 3.10×
- Mean Hydro AUC: 0.805

This provides a useful multi-month example of persistent spatial prioritization.

### Southern Alberta storm — August 2018

This event illustrates an important limitation.

- Composite event AUC: 0.343
- Capture @ top 10%: 1.8%
- Climate AUC: 0.839

The relevant Climate domain aligns strongly with the event footprint while the multi-hazard composite does not. The event occurs within a broad mapped regional footprint, and Hydro scores are substantially lower in the labelled affected area than in controls.

The example shows why domain diagnostics and spatial-label interpretation are necessary when evaluating a multi-hazard composite.

---

## 12. Limitations

### Validation is contemporaneous, not predictive

The score is evaluated against disaster events occurring in the same month as the hazard observations.

No claim is made that the score predicts future disasters.

The available mapped event labels do not extend far enough to support the originally proposed 2023–2025 out-of-time validation.

### Disaster footprints are approximate

The event-grid labels are derived from mapped administrative areas rather than authoritative physical impact perimeters.

Some locations require CSD-to-parent-CD approximation, while some direct regional mappings still cover large portions of a province.

A grid labelled as affected therefore represents membership in a mapped disaster-event scope, not confirmed physical damage at that grid.

### Small event sample

The final analysis contains 22 underlying source events. Results are therefore useful as historical evidence but should not be interpreted as population-level performance estimates.

### Provincial label quality differs

Alberta and British Columbia have materially different mapped footprint distributions. This limits direct interpretation of the provincial validation gap.

### Wildfire representation is narrow

The current Wildfire score is based on observed perimeter overlap. It does not include a broader fire-weather or susceptibility model.

Future work could incorporate variables such as fire weather, drought, vegetation, or historical fire susceptibility if a predictive wildfire-risk objective is required.

---

## 13. Conclusion

Risk Score v1 provides useful but heterogeneous retrospective spatial prioritization.

Across all 22 source events, the top 10% of provincial monthly priority grids captured 19.3% of mapped disaster footprints on average, corresponding to a 1.93× random-ranking baseline. Overall event AUC is 0.540, indicating modest discrimination across the full mixed-quality validation sample.

The strongest evidence appears in British Columbia, where mean composite AUC reaches 0.706 and average capture lift reaches 3.58×. Hydro and Climate also show useful domain-level discrimination, with mean AUC values of 0.671 and 0.654 respectively.

Alberta performs substantially worse in the grid-level backtest. Diagnostic analysis shows that Alberta validation events are represented by much broader administrative footprints: the median mapped event covers about 16% of the rankable province-month grid universe compared with about 5% in British Columbia. This spatial-label difference is an important confounding factor when interpreting the provincial performance gap.

Higher-spatial-precision event subsets produce stronger validation results, while removing small polygon-edge overlaps has almost no effect. Reasonable changes to domain weights also leave the ranking structure largely unchanged.

The evidence supports retaining the model as a **multi-hazard prioritization score**, not as a disaster prediction model. Its strongest current use is relative monthly spatial prioritization, with validation results interpreted alongside domain coverage and disaster-label spatial precision.