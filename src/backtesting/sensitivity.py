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


def build_label_quality_sensitivity(
    *,
    event_scope: pd.DataFrame,
    labels: pd.DataFrame,
    scores: pd.DataFrame,
    top_k_fraction: float = 0.10,
    minimum_boundary_coverage_ratio: float = 0.01,
    low_overlap_threshold: float = 0.05,
) -> dict[str, pd.DataFrame]:
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