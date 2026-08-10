from __future__ import annotations
from typing import Any

import numpy as np
import pandas as pd


DOMAIN_SCORE_COLUMNS = {
    "wildfire": "wildfire_sub_score",
    "flood": "hydro_sub_score",
    "severe_storm_or_climate": "climate_sub_score",
    "climate_extreme": "climate_sub_score",
}

def build_event_month_metrics(
    *,
    event_scope: pd.DataFrame,
    labels: pd.DataFrame,
    scores: pd.DataFrame,
    top_k_fraction: float = 0.10,
) -> pd.DataFrame:
    score_labels = scores.merge(
        labels[
            [
                "grid_cell_key",
                "reference_month",
                "disaster_event_occurred",
            ]
        ],
        on=["grid_cell_key", "reference_month"],
        how="inner",
        validate="one_to_one",
    )

    rows = []
    for event_key, event_rows in event_scope.groupby(
        "disaster_event_reference_key",
        sort=True
    ):
        event = event_rows.iloc[0]
        province = str(event["province_key"])
        reference_month = str(event["reference_month"])
        universe = score_labels[
            score_labels["province_key"].eq(province)
            & score_labels["reference_month"].eq(reference_month)
        ].copy()
        
        affected_keys = set(event_rows["grid_cell_key"].astype(str))
        affected = universe[
            universe["grid_cell_key"]
            .astype(str)
            .isin(affected_keys)
        ]
        affected_rankable = affected[
            affected["ranking_eligible"].astype(bool)
        ]
        
        controls = universe[
            universe["ranking_eligible"].astype(bool)
            & ~universe["disaster_event_occurred"].astype(bool)
        ]
        top_threshold = 1.0 - top_k_fraction
        top_grids = universe[
            universe["ranking_eligible"].astype(bool)
            & universe["priority_percentile"].ge(top_threshold)
        ]
        affected_top = affected_rankable[
            affected_rankable["priority_percentile"].ge(top_threshold)
        ]
        affected_rankable_count = len(affected_rankable)
        affected_top_count = len(affected_top)
        top_grid_count = len(top_grids)
        
        capture = affected_top_count / affected_rankable_count
        precision = affected_top_count / top_grid_count
        
        event_auc = _rank_auc(
            positive_scores=affected_rankable["composite_risk_score"],
            negative_scores=controls["composite_risk_score"],
        )
        
        source_key = str(event["source_disaster_event_key"])
        source_event_id = _source_event_id_from_key(
            source_key=source_key,
            province_key=province,
            reference_month=reference_month,
        )
        
        rows.append(
            {
                "disaster_event_reference_key": str(event_key),
                "source_disaster_event_key": source_key,
                "source_event_id": source_event_id,
                "reference_month": reference_month,
                "province_key": province,
                "disaster_domain": str(event["disaster_domain"]),
                "location_text": event["location_text"],
                "location_tier": event["location_tier"],
                "affected_grid_count": int(len(event_rows)),
                "affected_rankable_count": int(affected_rankable_count),
                "affected_rankable_rate": float(
                    affected_rankable_count / len(event_rows)
                ),
                "control_rankable_count": int(len(controls)),
                "top10_grid_count": int(top_grid_count),
                "affected_top10_count": int(affected_top_count),
                "event_capture_at_10": float(capture),
                "capture_lift_at_10": float(capture / top_k_fraction),
                "precision_at_10": float(precision),
                "event_auc": float(event_auc),
                "mean_affected_score": float(
                    affected_rankable["composite_risk_score"].mean()
                ),
                "mean_control_score": float(
                    controls["composite_risk_score"].mean()
                ),
                "score_gap": float(
                    affected_rankable["composite_risk_score"].mean()
                    - controls["composite_risk_score"].mean()
                ),
                "mean_affected_priority_percentile": float(
                    affected_rankable["priority_percentile"].mean()
                ),
                "median_affected_priority_percentile": float(
                    affected_rankable["priority_percentile"].median()
                ),
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "reference_month",
                "province_key",
                "disaster_event_reference_key",
            ]
        )
        .reset_index(drop=True)
    )
    
    
def build_domain_event_month_metrics(
    *,
    event_scope: pd.DataFrame,
    labels: pd.DataFrame,
    scores: pd.DataFrame,
    minimum_boundary_coverage_ratio: float = 0.01,
) -> pd.DataFrame:
    score_labels = scores.merge(
        labels[
            [
                "grid_cell_key",
                "reference_month",
                "disaster_event_occurred",
            ]
        ],
        on=["grid_cell_key", "reference_month"],
        how="inner",
        validate="one_to_one",
    )

    rows = []

    for event_key, event_rows in event_scope.groupby(
        "disaster_event_reference_key",
        sort=True,
    ):
        event = event_rows.iloc[0]

        province = str(event["province_key"])
        reference_month = str(event["reference_month"])
        disaster_domain = str(event["disaster_domain"])

        score_column = DOMAIN_SCORE_COLUMNS[disaster_domain]

        universe = score_labels[
            score_labels["province_key"].eq(province)
            & score_labels["reference_month"].eq(reference_month)
        ].copy()

        domain_universe = universe[
            universe["boundary_coverage_ratio"].ge(
                minimum_boundary_coverage_ratio
            )
            & universe[score_column].notna()
        ].copy()

        domain_universe["domain_percentile"] = domain_universe[
            score_column
        ].rank(
            pct=True,
            method="average",
        )

        affected_keys = set(event_rows["grid_cell_key"].astype(str))

        affected = domain_universe[
            domain_universe["grid_cell_key"].astype(str).isin(affected_keys)
        ]

        controls = domain_universe[
            ~domain_universe["disaster_event_occurred"].astype(bool)
        ]

        affected_grid_count = len(event_rows)

        event_auc = _rank_auc(
            positive_scores=affected[score_column],
            negative_scores=controls[score_column],
        )

        source_key = str(event["source_disaster_event_key"])

        source_event_id = _source_event_id_from_key(
            source_key=source_key,
            province_key=province,
            reference_month=reference_month,
        )

        rows.append(
            {
                "disaster_event_reference_key": str(event_key),
                "source_disaster_event_key": source_key,
                "source_event_id": source_event_id,
                "reference_month": reference_month,
                "province_key": province,
                "disaster_domain": disaster_domain,
                "domain_score_column": score_column,
                "location_text": event["location_text"],
                "location_tier": event["location_tier"],
                "affected_grid_count": int(affected_grid_count),
                "affected_domain_score_count": int(len(affected)),
                "affected_domain_score_coverage": float(
                    len(affected) / affected_grid_count
                ),
                "control_domain_score_count": int(len(controls)),
                "event_domain_auc": float(event_auc),
                "mean_affected_domain_score": float(
                    affected[score_column].mean()
                ),
                "mean_control_domain_score": float(
                    controls[score_column].mean()
                ),
                "domain_score_gap": float(
                    affected[score_column].mean()
                    - controls[score_column].mean()
                ),
                "mean_affected_domain_percentile": float(
                    affected["domain_percentile"].mean()
                ),
                "median_affected_domain_percentile": float(
                    affected["domain_percentile"].median()
                ),
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "reference_month",
                "province_key",
                "disaster_event_reference_key",
            ]
        )
        .reset_index(drop=True)
    )


def build_source_event_metrics(event_month_metrics: pd.DataFrame) -> pd.DataFrame:
    return (
        event_month_metrics.groupby(
            "source_event_id",
            as_index=False,
            sort=True,
        )
        .agg(
            province_key=("province_key", "first"),
            disaster_domain=("disaster_domain", "first"),
            location_text=("location_text", "first"),
            location_tier=("location_tier", "first"),
            first_reference_month=("reference_month", "min"),
            last_reference_month=("reference_month", "max"),
            event_month_count=("reference_month", "size"),
            mean_affected_rankable_rate=("affected_rankable_rate", "mean"),
            mean_event_capture_at_10=("event_capture_at_10", "mean"),
            mean_capture_lift_at_10=("capture_lift_at_10", "mean"),
            mean_precision_at_10=("precision_at_10", "mean"),
            mean_event_auc=("event_auc", "mean"),
            mean_score_gap=("score_gap", "mean"),
            mean_affected_priority_percentile=(
                "mean_affected_priority_percentile",
                "mean",
            ),
        )
        .sort_values(
            [
                "first_reference_month",
                "province_key",
                "source_event_id",
            ]
        )
        .reset_index(drop=True)
    )
    
    
def build_source_event_domain_metrics(event_month_metrics: pd.DataFrame) -> pd.DataFrame:
    return (
        event_month_metrics
        .groupby(
            "source_event_id",
            as_index=False,
            sort=True,
        )
        .agg(
            province_key=(
                "province_key",
                "first",
            ),
            disaster_domain=(
                "disaster_domain",
                "first",
            ),
            domain_score_column=(
                "domain_score_column",
                "first",
            ),
            location_text=(
                "location_text",
                "first",
            ),
            location_tier=(
                "location_tier",
                "first",
            ),
            first_reference_month=(
                "reference_month",
                "min",
            ),
            last_reference_month=(
                "reference_month",
                "max",
            ),
            event_month_count=(
                "reference_month",
                "size",
            ),
            mean_affected_domain_score_coverage=(
                "affected_domain_score_coverage",
                "mean",
            ),
            minimum_affected_domain_score_coverage=(
                "affected_domain_score_coverage",
                "min",
            ),
            mean_event_domain_auc=(
                "event_domain_auc",
                "mean",
            ),
            mean_domain_score_gap=(
                "domain_score_gap",
                "mean",
            ),
            mean_affected_domain_percentile=(
                "mean_affected_domain_percentile",
                "mean",
            ),
        )
        .sort_values(
            [
                "first_reference_month",
                "province_key",
                "source_event_id",
            ]
        )
        .reset_index(drop=True)
    )
    
    
def summarize_source_events(source_event_metrics: pd.DataFrame,) -> dict[str, Any]:
    return {
        "source_event_count": int(len(source_event_metrics)),
        "event_month_count": int(
            source_event_metrics["event_month_count"].sum()
        ),
        "mean_event_capture_at_10": float(
            source_event_metrics["mean_event_capture_at_10"].mean()
        ),
        "median_event_capture_at_10": float(
            source_event_metrics["mean_event_capture_at_10"].median()
        ),
        "mean_capture_lift_at_10": float(
            source_event_metrics["mean_capture_lift_at_10"].mean()
        ),
        "median_capture_lift_at_10": float(
            source_event_metrics["mean_capture_lift_at_10"].median()
        ),
        "mean_event_auc": float(
            source_event_metrics["mean_event_auc"].mean()
        ),
        "median_event_auc": float(
            source_event_metrics["mean_event_auc"].median()
        ),
        "source_event_counts_by_domain": {
            str(key): int(value)
            for key, value in source_event_metrics[
                "disaster_domain"
            ].value_counts().items()
        },
    }
    
    
def summarize_domain_source_events(source_event_metrics: pd.DataFrame) -> dict[str, Any]:
    domain_summary = {}

    for domain, group in source_event_metrics.groupby(
        "disaster_domain",
        sort=True,
    ):
        domain_summary[str(domain)] = {
            "source_event_count": int(
                len(group)
            ),
            "mean_event_domain_auc": float(
                group[
                    "mean_event_domain_auc"
                ].mean()
            ),
            "median_event_domain_auc": float(
                group[
                    "mean_event_domain_auc"
                ].median()
            ),
            "mean_domain_score_gap": float(
                group[
                    "mean_domain_score_gap"
                ].mean()
            ),
            "mean_affected_domain_score_coverage": float(
                group[
                    "mean_affected_domain_score_coverage"
                ].mean()
            ),
        }

    return {
        "source_event_count": int(
            len(source_event_metrics)
        ),
        "event_month_count": int(
            source_event_metrics[
                "event_month_count"
            ].sum()
        ),
        "domains": domain_summary,
    }
    
    
def _rank_auc(
    *,
    positive_scores: pd.Series,
    negative_scores: pd.Series,
) -> float:
    positive = pd.to_numeric(
        positive_scores,
        errors="raise",
    ).dropna()

    negative = pd.to_numeric(
        negative_scores,
        errors="raise",
    ).dropna()

    scores = pd.concat(
        [positive, negative],
        ignore_index=True,
    )

    labels = np.concatenate(
        [
            np.ones(len(positive), dtype="int8"),
            np.zeros(len(negative), dtype="int8"),
        ]
    )

    ranks = scores.rank(method="average")

    positive_rank_sum = float(ranks[labels == 1].sum())

    positive_count = len(positive)
    negative_count = len(negative)

    mann_whitney_u = positive_rank_sum - positive_count * (positive_count + 1) / 2

    return float(
        mann_whitney_u
        / (positive_count * negative_count)
    )


def _source_event_id_from_key(
    *,
    source_key: str,
    province_key: str,
    reference_month: str,
) -> str:
    parts = source_key.rsplit("_", 2)

    if (
        len(parts) == 3
        and parts[1] == province_key
        and parts[2] == reference_month
    ):
        return parts[0]

    return source_key