from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd


REQUIRED_TIER_NAMES = {
    "very_high",
    "high",
    "elevated",
    "moderate",
}


class RiskRankingError(ValueError):
    """Raised when risk-ranking inputs are invalid."""


def build_ranking_features(
    dataframe: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    minimum_boundary_coverage_ratio: float,
    priority_tiers: Mapping[str, float],
) -> pd.DataFrame:
    """Build ranking eligibility, percentile, and priority tier."""

    _validate_parameters(
        group_columns=group_columns,
        minimum_boundary_coverage_ratio=minimum_boundary_coverage_ratio,
        priority_tiers=priority_tiers,
    )

    _require_columns(
        dataframe,
        [
            *group_columns,
            "boundary_coverage_ratio",
            "composite_score_eligible",
            "composite_risk_score",
        ],
    )

    composite_score = pd.to_numeric(
        dataframe["composite_risk_score"],
        errors="raise",
    )

    boundary_coverage = pd.to_numeric(
        dataframe["boundary_coverage_ratio"],
        errors="raise",
    )

    composite_eligible = dataframe[
        "composite_score_eligible"
    ].astype(bool)

    _validate_inputs(
        dataframe=dataframe,
        group_columns=group_columns,
        composite_score=composite_score,
        composite_eligible=composite_eligible,
        boundary_coverage=boundary_coverage,
    )

    result = pd.DataFrame(index=dataframe.index)

    boundary_eligible = (
        boundary_coverage
        >= minimum_boundary_coverage_ratio
    )

    result["ranking_eligible"] = (
        composite_eligible
        & boundary_eligible
    )

    result["ranking_exclusion_reason"] = "none"

    result.loc[
        ~composite_eligible,
        "ranking_exclusion_reason",
    ] = "insufficient_domain_coverage"

    result.loc[
        composite_eligible & ~boundary_eligible,
        "ranking_exclusion_reason",
    ] = "boundary_sliver"

    result["priority_percentile"] = np.nan

    eligible_mask = result["ranking_eligible"]

    if eligible_mask.any():
        ranking_frame = dataframe.loc[
            eligible_mask,
            list(group_columns),
        ].copy()

        ranking_frame["composite_risk_score"] = (
            composite_score.loc[eligible_mask]
        )

        ranking_frame["priority_percentile"] = (
            ranking_frame
            .groupby(
                list(group_columns),
                observed=True,
                dropna=False,
            )["composite_risk_score"]
            .rank(
                pct=True,
                method="average",
                ascending=True,
            )
        )

        result.loc[
            eligible_mask,
            "priority_percentile",
        ] = ranking_frame["priority_percentile"]

    result["priority_tier"] = "insufficient_data"

    percentile = result["priority_percentile"]

    very_high = float(priority_tiers["very_high"])
    high = float(priority_tiers["high"])
    elevated = float(priority_tiers["elevated"])
    moderate = float(priority_tiers["moderate"])

    result.loc[
        eligible_mask
        & percentile.ge(very_high),
        "priority_tier",
    ] = "very_high"

    result.loc[
        eligible_mask
        & percentile.ge(high)
        & percentile.lt(very_high),
        "priority_tier",
    ] = "high"

    result.loc[
        eligible_mask
        & percentile.ge(elevated)
        & percentile.lt(high),
        "priority_tier",
    ] = "elevated"

    result.loc[
        eligible_mask
        & percentile.ge(moderate)
        & percentile.lt(elevated),
        "priority_tier",
    ] = "moderate"

    result.loc[
        eligible_mask
        & percentile.lt(moderate),
        "priority_tier",
    ] = "low"

    _validate_outputs(result)

    return result


def _validate_parameters(
    *,
    group_columns: Sequence[str],
    minimum_boundary_coverage_ratio: float,
    priority_tiers: Mapping[str, float],
) -> None:
    if not group_columns:
        raise RiskRankingError(
            "group_columns must not be empty."
        )
        
    if len(set(group_columns)) != len(group_columns):
        raise RiskRankingError(
            "group_columns must not contain duplicates."
        )
        
    if not 0.0 <= minimum_boundary_coverage_ratio <= 1.0:
        raise RiskRankingError(
            "minimum_boundary_coverage_ratio must be in [0, 1]."
        )
        
    if set(priority_tiers) != REQUIRED_TIER_NAMES:
        raise RiskRankingError(
            "priority_tiers must contain very_high, high, "
            "elevated, and moderate."
        )
        
    very_high = float(priority_tiers["very_high"])
    high = float(priority_tiers["high"])
    elevated = float(priority_tiers["elevated"])
    moderate = float(priority_tiers["moderate"])
    
    if not all(
        0.0 <= value <= 1.0
        for value in [very_high, high, elevated, moderate]
    ):
        raise RiskRankingError(
            "priority_tiers values must be in [0, 1]."
        )
        
    if not (very_high > high > elevated > moderate):
        raise RiskRankingError(
            "Priority tier thresholds must satisfy "
            "very_high > high > elevated > moderate."
        )
        

def _validate_inputs(
    *,
    dataframe: pd.DataFrame,
    group_columns: Sequence[str],
    composite_score: pd.Series,
    composite_eligible: pd.Series,
    boundary_coverage: pd.Series,
) -> None:
    if dataframe[list(group_columns)].isna().any().any():
        raise RiskRankingError(
            "Ranking group columns must not contain null values."
        )

    if dataframe[
        "composite_score_eligible"
    ].isna().any():
        raise RiskRankingError(
            "composite_score_eligible must not contain null values."
        )

    invalid_score = (
        composite_score.notna()
        & ~composite_score.between(0.0, 1.0)
    )

    if invalid_score.any():
        raise RiskRankingError(
            "composite_risk_score must be in [0, 1]."
        )

    if not composite_eligible.equals(
        composite_score.notna()
    ):
        raise RiskRankingError(
            "composite_score_eligible is inconsistent with "
            "composite_risk_score."
        )

    if (
        boundary_coverage.isna().any()
        or not boundary_coverage.between(
            0.0,
            1.0,
        ).all()
    ):
        raise RiskRankingError(
            "boundary_coverage_ratio must be in [0, 1] "
            "and must not contain null values."
        )


def _validate_outputs(
    result: pd.DataFrame,
) -> None:
    percentile = result[
        "priority_percentile"
    ].dropna()

    if not percentile.between(
        0.0,
        1.0,
    ).all():
        raise RiskRankingError(
            "priority_percentile must be in [0, 1]."
        )

    eligible = result["ranking_eligible"]

    if result.loc[
        eligible,
        "priority_percentile",
    ].isna().any():
        raise RiskRankingError(
            "Ranking-eligible rows must have "
            "priority_percentile."
        )

    if result.loc[
        ~eligible,
        "priority_percentile",
    ].notna().any():
        raise RiskRankingError(
            "Ranking-ineligible rows must not have "
            "priority_percentile."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: Sequence[str],
) -> None:
    missing = (
        set(required_columns) - set(dataframe.columns)
    )

    if missing:
        raise RiskRankingError(
            "Missing required ranking columns: "
            f"{sorted(missing)}"
        )