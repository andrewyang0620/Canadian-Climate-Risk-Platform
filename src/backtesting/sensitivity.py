from __future__ import annotations

import pandas as pd

from src.backtesting.risk_score import (
    build_domain_event_month_metrics,
    build_event_month_metrics,
    build_source_event_domain_metrics,
    build_source_event_metrics,
)


LABEL_QUALITY_SCENARIOS = (
    "baseline",
    "exclude_csd_approximation",
    "exclude_low_overlap",
    "exclude_csd_and_low_overlap",
)

DOMAIN_SCORE_COLUMNS = {
    "climate": "climate_sub_score",
    "hydro": "hydro_sub_score",
    "wildfire": "wildfire_sub_score",
}


def build_label_quality_sensitivity(
    *,
    event_scope: pd.DataFrame,
    labels: pd.DataFrame,
    scores: pd.DataFrame,
    top_k_fraction: float = 0.10,
    minimum_boundary_coverage_ratio: float = 0.01,
    low_overlap_threshold: float = 0.05,
) -> dict[str, pd.DataFrame]:
    """If get rid of the CSDn appox and low overlap, will result be significantly different"""
    event_month_parts = []
    source_event_parts = []
    domain_event_month_parts = []
    source_event_domain_parts = []

    for scenario in LABEL_QUALITY_SCENARIOS:
        scenario_scope = _filter_event_scope(
            event_scope=event_scope,
            scenario=scenario,
            low_overlap_threshold=low_overlap_threshold,
        )

        event_month = build_event_month_metrics(
            event_scope=scenario_scope,
            labels=labels,
            scores=scores,
            top_k_fraction=top_k_fraction,
        )
        event_month.insert(
            0,
            "label_scenario",
            scenario,
        )

        source_event = build_source_event_metrics(
            event_month
        )
        source_event.insert(
            0,
            "label_scenario",
            scenario,
        )

        domain_event_month = build_domain_event_month_metrics(
            event_scope=scenario_scope,
            labels=labels,
            scores=scores,
            minimum_boundary_coverage_ratio=minimum_boundary_coverage_ratio,
        )
        domain_event_month.insert(
            0,
            "label_scenario",
            scenario,
        )

        source_event_domain = build_source_event_domain_metrics(
            domain_event_month
        )
        source_event_domain.insert(
            0,
            "label_scenario",
            scenario,
        )

        event_month_parts.append(
            event_month
        )
        source_event_parts.append(
            source_event
        )
        domain_event_month_parts.append(
            domain_event_month
        )
        source_event_domain_parts.append(
            source_event_domain
        )

    return {
        "event_month_metrics": pd.concat(
            event_month_parts,
            ignore_index=True,
        ),
        "source_event_metrics": pd.concat(
            source_event_parts,
            ignore_index=True,
        ),
        "domain_event_month_metrics": pd.concat(
            domain_event_month_parts,
            ignore_index=True,
        ),
        "source_event_domain_metrics": pd.concat(
            source_event_domain_parts,
            ignore_index=True,
        ),
    }


def summarize_label_quality_sensitivity(
    source_event_metrics: pd.DataFrame,
) -> pd.DataFrame:
    return (
        source_event_metrics
        .groupby(
            "label_scenario",
            as_index=False,
            sort=False,
        )
        .agg(
            source_event_count=(
                "source_event_id",
                "nunique",
            ),
            event_month_count=(
                "event_month_count",
                "sum",
            ),
            mean_event_capture_at_10=(
                "mean_event_capture_at_10",
                "mean",
            ),
            median_event_capture_at_10=(
                "mean_event_capture_at_10",
                "median",
            ),
            mean_capture_lift_at_10=(
                "mean_capture_lift_at_10",
                "mean",
            ),
            median_capture_lift_at_10=(
                "mean_capture_lift_at_10",
                "median",
            ),
            mean_event_auc=(
                "mean_event_auc",
                "mean",
            ),
            median_event_auc=(
                "mean_event_auc",
                "median",
            ),
            mean_score_gap=(
                "mean_score_gap",
                "mean",
            ),
        )
    )


def summarize_domain_label_quality_sensitivity(
    source_event_metrics: pd.DataFrame,
) -> pd.DataFrame:
    return (
        source_event_metrics
        .groupby(
            [
                "label_scenario",
                "disaster_domain",
            ],
            as_index=False,
            sort=False,
        )
        .agg(
            source_event_count=(
                "source_event_id",
                "nunique",
            ),
            event_month_count=(
                "event_month_count",
                "sum",
            ),
            mean_event_domain_auc=(
                "mean_event_domain_auc",
                "mean",
            ),
            median_event_domain_auc=(
                "mean_event_domain_auc",
                "median",
            ),
            mean_domain_score_gap=(
                "mean_domain_score_gap",
                "mean",
            ),
            mean_affected_domain_score_coverage=(
                "mean_affected_domain_score_coverage",
                "mean",
            ),
            minimum_affected_domain_score_coverage=(
                "minimum_affected_domain_score_coverage",
                "min",
            ),
        )
    )


def _filter_event_scope(
    *,
    event_scope: pd.DataFrame,
    scenario: str,
    low_overlap_threshold: float,
) -> pd.DataFrame:
    keep = pd.Series(
        True,
        index=event_scope.index,
    )

    if scenario in {
        "exclude_csd_approximation",
        "exclude_csd_and_low_overlap",
    }:
        keep &= ~event_scope[
            "is_csd_to_cd_approximation"
        ].astype(bool)

    if scenario in {
        "exclude_low_overlap",
        "exclude_csd_and_low_overlap",
    }:
        keep &= event_scope[
            "affected_grid_coverage_ratio"
        ].gt(low_overlap_threshold)

    return event_scope.loc[
        keep
    ].copy()
    
    
def build_weight_scenario_scores(
    scores: pd.DataFrame,
    *,
    domain_weights: dict[str, float],
    minimum_available_domains: int = 2,
    minimum_boundary_coverage_ratio: float = 0.01,
) -> pd.DataFrame:
    """After change the weight of three hazards, how will the composite risk score and priority percentile change?"""
    result = scores[
        [
            "grid_cell_key",
            "reference_month",
            "province_key",
            "boundary_coverage_ratio",
            "climate_sub_score",
            "hydro_sub_score",
            "wildfire_sub_score",
        ]
    ].copy()

    weighted_sum = pd.Series(0.0, index=result.index)
    available_weight = pd.Series(0.0, index=result.index)
    available_count = pd.Series(0, index=result.index, dtype="int8")

    for domain, score_column in DOMAIN_SCORE_COLUMNS.items():
        available = result[score_column].notna()
        weight = float(domain_weights[domain])

        weighted_sum += result[score_column].fillna(0.0) * weight
        available_weight += available.astype(float) * weight
        available_count += available.astype("int8")

    composite_eligible = available_count >= minimum_available_domains

    result["composite_risk_score"] = (
        weighted_sum / available_weight
    ).where(composite_eligible)

    result["ranking_eligible"] = (
        composite_eligible
        & result["boundary_coverage_ratio"].ge(minimum_boundary_coverage_ratio)
    )

    result["priority_percentile"] = float("nan")

    eligible = result["ranking_eligible"]

    result.loc[eligible, "priority_percentile"] = (
        result.loc[eligible]
        .groupby(
            ["province_key", "reference_month"],
            sort=False,
        )["composite_risk_score"]
        .rank(
            pct=True,
            method="average",
            ascending=True,
        )
    )

    return result


def build_rank_stability_metrics(
    *,
    baseline_scores: pd.DataFrame,
    scenario_scores: pd.DataFrame,
    top_k_fraction: float = 0.10,
) -> pd.DataFrame:
    """After changing the weights, does the ranking change significantly?"""
    comparison = baseline_scores[
        [
            "grid_cell_key",
            "reference_month",
            "province_key",
            "ranking_eligible",
            "priority_percentile",
        ]
    ].rename(
        columns={
            "ranking_eligible": "baseline_ranking_eligible",
            "priority_percentile": "baseline_priority_percentile",
        }
    )

    scenario = scenario_scores[
        [
            "grid_cell_key",
            "reference_month",
            "ranking_eligible",
            "priority_percentile",
        ]
    ].rename(
        columns={
            "ranking_eligible": "scenario_ranking_eligible",
            "priority_percentile": "scenario_priority_percentile",
        }
    )

    comparison = comparison.merge(
        scenario,
        on=["grid_cell_key", "reference_month"],
        how="inner",
        validate="one_to_one",
    )

    comparison = comparison[
        comparison["baseline_ranking_eligible"].astype(bool)
        & comparison["scenario_ranking_eligible"].astype(bool)
    ].copy()

    top_threshold = 1.0 - top_k_fraction
    rows = []

    for (province, month), group in comparison.groupby(
        ["province_key", "reference_month"],
        sort=True,
    ):
        baseline_top = group["baseline_priority_percentile"].ge(top_threshold)
        scenario_top = group["scenario_priority_percentile"].ge(top_threshold)

        intersection = int((baseline_top & scenario_top).sum())
        union = int((baseline_top | scenario_top).sum())

        # The two columns are already within-group percentile ranks,
        # so their Pearson correlation is the Spearman rank correlation.
        spearman = group["baseline_priority_percentile"].corr(
            group["scenario_priority_percentile"]
        )

        rows.append(
            {
                "province_key": str(province),
                "reference_month": str(month),
                "common_rankable_grid_count": int(len(group)),
                "spearman_rank_correlation": float(spearman),
                "baseline_top10_count": int(baseline_top.sum()),
                "scenario_top10_count": int(scenario_top.sum()),
                "top10_intersection_count": intersection,
                "top10_union_count": union,
                "top10_jaccard": float(intersection / union),
            }
        )

    return pd.DataFrame(rows)


def build_weight_sensitivity(
    *,
    event_scope: pd.DataFrame,
    labels: pd.DataFrame,
    scores: pd.DataFrame,
    weight_scenarios: dict[str, dict[str, float]],
    top_k_fraction: float = 0.10,
    minimum_available_domains: int = 2,
    minimum_boundary_coverage_ratio: float = 0.01,
) -> dict[str, pd.DataFrame]:
    """Re-culculate the  metrics"""
    event_month_parts = []
    source_event_parts = []
    rank_stability_parts = []

    for scenario, weights in weight_scenarios.items():
        scenario_scores = build_weight_scenario_scores(
            scores,
            domain_weights=weights,
            minimum_available_domains=minimum_available_domains,
            minimum_boundary_coverage_ratio=minimum_boundary_coverage_ratio,
        )

        event_month = build_event_month_metrics(
            event_scope=event_scope,
            labels=labels,
            scores=scenario_scores,
            top_k_fraction=top_k_fraction,
        )
        event_month.insert(0, "weight_scenario", scenario)

        source_event = build_source_event_metrics(event_month)
        source_event.insert(0, "weight_scenario", scenario)

        rank_stability = build_rank_stability_metrics(
            baseline_scores=scores,
            scenario_scores=scenario_scores,
            top_k_fraction=top_k_fraction,
        )
        rank_stability.insert(0, "weight_scenario", scenario)

        event_month_parts.append(event_month)
        source_event_parts.append(source_event)
        rank_stability_parts.append(rank_stability)

    return {
        "event_month_metrics": pd.concat(event_month_parts, ignore_index=True),
        "source_event_metrics": pd.concat(source_event_parts, ignore_index=True),
        "rank_stability_metrics": pd.concat(rank_stability_parts, ignore_index=True),
    }


def summarize_weight_sensitivity(
    *,
    source_event_metrics: pd.DataFrame,
    rank_stability_metrics: pd.DataFrame,
) -> pd.DataFrame:
    event_summary = (
        source_event_metrics
        .groupby("weight_scenario", as_index=False, sort=False)
        .agg(
            source_event_count=("source_event_id", "nunique"),
            event_month_count=("event_month_count", "sum"),
            mean_event_capture_at_10=("mean_event_capture_at_10", "mean"),
            median_event_capture_at_10=("mean_event_capture_at_10", "median"),
            mean_capture_lift_at_10=("mean_capture_lift_at_10", "mean"),
            median_capture_lift_at_10=("mean_capture_lift_at_10", "median"),
            mean_event_auc=("mean_event_auc", "mean"),
            median_event_auc=("mean_event_auc", "median"),
        )
    )

    rank_summary = (
        rank_stability_metrics
        .groupby("weight_scenario", as_index=False, sort=False)
        .agg(
            province_month_count=("reference_month", "size"),
            mean_spearman_rank_correlation=("spearman_rank_correlation", "mean"),
            median_spearman_rank_correlation=("spearman_rank_correlation", "median"),
            mean_top10_jaccard=("top10_jaccard", "mean"),
            median_top10_jaccard=("top10_jaccard", "median"),
        )
    )

    return event_summary.merge(
        rank_summary,
        on="weight_scenario",
        how="inner",
        validate="one_to_one",
    )