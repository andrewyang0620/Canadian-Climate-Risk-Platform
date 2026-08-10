from __future__ import annotations

import numpy as np
import pandas as pd

from src.scoring.normalization import (
    RiskScoringNormalizationError,
    grouped_zero_preserving_positive_percentile,
)

WILDFIRE_SIGNAL_COLUMN = "wildfire_intersection_area_ratio_of_grid"

REQUIRED_COLUMNS = {
    "province_key",
    "has_wildfire_perimeter_feature",
    "has_wildfire_observed_perimeter_overlap",
    "wildfire_perimeter_count",
    WILDFIRE_SIGNAL_COLUMN,
}

class WildfireScoringError(ValueError):
    """Raised when Wildfire scoring inputs are invalid"""
    

def build_wildfire_scoring_features(dataframe: pd.DataFrame, *, fixed_quality: float=1.0) -> pd.DataFrame:
    """
    Build the Wildfire sub-score.

    Zero perimeter overlap remains exactly 0. Positive burn-area
    ratios are percentile ranked within province.
    """
    _require_columns(dataframe)
    _validate_parameters(fixed_quality=fixed_quality)
    _validate_wildfire_inputs(dataframe)

    try:
        normalized = grouped_zero_preserving_positive_percentile(
            dataframe,
            value_column=WILDFIRE_SIGNAL_COLUMN,
            group_columns=["province_key"],
        )
    except RiskScoringNormalizationError as exc:
        raise WildfireScoringError(str(exc)) from exc

    result = pd.DataFrame(index=dataframe.index)

    result[
        "wildfire_intersection_area_ratio_of_grid_normalized"
    ] = normalized

    result["wildfire_sub_score"] = normalized

    feature_available = dataframe[
        "has_wildfire_perimeter_feature"
    ].astype(bool)

    result["wildfire_domain_available"] = (
        feature_available
        & result["wildfire_sub_score"].notna()
    )

    result["wildfire_effective_quality"] = np.where(
        result["wildfire_domain_available"],
        fixed_quality,
        0.0,
    ).astype("float64")

    _validate_output_ranges(result)

    return result
    
def _validate_parameters(*, fixed_quality: float) -> None:
    if not 0.0 <= fixed_quality <= 1.0:
        raise WildfireScoringError(
            "fixed_quality must be in [0, 1]."
        )
        
def _validate_wildfire_inputs(dataframe: pd.DataFrame) -> None:
    flag_columns = [
        "has_wildfire_perimeter_feature",
        "has_wildfire_observed_perimeter_overlap",
    ]

    if dataframe[flag_columns].isna().any().any():
        raise WildfireScoringError(
            "Wildfire availability flags must not contain null values."
        )

    feature_available = dataframe[
        "has_wildfire_perimeter_feature"
    ].astype(bool)

    observed_overlap = dataframe[
        "has_wildfire_observed_perimeter_overlap"
    ].astype(bool)

    perimeter_count = pd.to_numeric(
        dataframe["wildfire_perimeter_count"],
        errors="raise",
    )

    burn_ratio = pd.to_numeric(
        dataframe[WILDFIRE_SIGNAL_COLUMN],
        errors="raise",
    )

    # Mart contract:
    # has_wildfire_perimeter_feature =
    # wildfire_perimeter_count is not null.
    expected_feature_available = perimeter_count.notna()

    if not feature_available.equals(
        expected_feature_available
    ):
        raise WildfireScoringError(
            "has_wildfire_perimeter_feature is inconsistent with "
            "wildfire_perimeter_count."
        )

    if burn_ratio.loc[~feature_available].notna().any():
        raise WildfireScoringError(
            "Rows without Wildfire feature coverage must have "
            "null burn-area ratio."
        )

    available_count = perimeter_count.loc[
        feature_available
    ]

    available_ratio = burn_ratio.loc[
        feature_available
    ]

    if available_count.lt(0).any():
        raise WildfireScoringError(
            "wildfire_perimeter_count cannot be negative."
        )

    if available_ratio.isna().any():
        raise WildfireScoringError(
            "Wildfire feature rows must have a burn-area ratio."
        )

    if not available_ratio.between(
        0.0,
        1.0,
    ).all():
        raise WildfireScoringError(
            "wildfire_intersection_area_ratio_of_grid "
            "must be in [0, 1]."
        )

    expected_overlap = perimeter_count.fillna(0).gt(0)

    if not observed_overlap.equals(
        expected_overlap
    ):
        raise WildfireScoringError(
            "has_wildfire_observed_perimeter_overlap is "
            "inconsistent with wildfire_perimeter_count."
        )

    no_overlap = (
        feature_available
        & ~observed_overlap
    )

    if burn_ratio.loc[no_overlap].ne(0.0).any():
        raise WildfireScoringError(
            "Wildfire no-overlap rows must have zero burn-area ratio."
        )

    overlap = (
        feature_available
        & observed_overlap
    )

    if burn_ratio.loc[overlap].le(0.0).any():
        raise WildfireScoringError(
            "Wildfire overlap rows must have positive burn-area ratio."
        )
        
def _validate_output_ranges(result: pd.DataFrame) -> None:
    columns = [
        "wildfire_intersection_area_ratio_of_grid_normalized",
        "wildfire_sub_score",
        "wildfire_effective_quality",
    ]
    
    for column in columns:
        values = result[column].dropna()
        if not values.between(0.0, 1.0).all():
            raise WildfireScoringError(
                f"{column} must be in [0, 1]."
            )
            

def _require_columns(dataframe: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(dataframe.columns)
    if missing:
        raise WildfireScoringError(
            "Missing required Wildfire scoring columns: "
            f"{sorted(missing)}"
        )