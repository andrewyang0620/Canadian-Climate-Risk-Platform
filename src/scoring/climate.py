from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from src.scoring.normalization import (
    RiskScoringNormalizationError,
    calendar_month_from_reference_month,
    grouped_zero_preserving_positive_percentile,
)

CLIMATE_SIGNAL_COLUMNS = (
    "climate_extreme_heat_days",
    "climate_heavy_precipitation_days",
    "climate_freeze_thaw_days",
    "climate_extreme_cold_days",
    "climate_total_precip_mm",
)

DIRECT_MAPPING_METHODS = {
    "direct_station_in_cell",
    "direct_station_average_in_cell",
}

IDW_MAPPING_METHOD = "idw_interpolated"
NO_COVERAGE_METHOD = "no_station_within_radius"

REQUIRED_COLUMNS = {
    "province_key",
    "reference_month",
    "has_climate_feature",
    "climate_mapping_method",
    "climate_idw_confidence_score",
    "climate_data_completeness_score",
    *CLIMATE_SIGNAL_COLUMNS,
}

class ClimateScoringError(ValueError):
    """Raised when Climate scoring inputs are invalid."""
    
def build_climate_scoring_features(
    dataframe: pd.DataFrame,
    *,
    signal_weights: Mapping[str, float],
) -> pd.DataFrame:
    """Climate normalized signals, sub-score and scoring quality."""
    _require_columns(dataframe)
    _validate_signal_weights(signal_weights)
    _validate_climate_inputs(dataframe)

    result = pd.DataFrame(index=dataframe.index)

    calendar_month = calendar_month_from_reference_month(
        dataframe["reference_month"]
    )

    normalization_frame = dataframe.copy()
    normalization_frame["calendar_month"] = calendar_month

    normalized_columns: dict[str, str] = {}

    for signal_column in CLIMATE_SIGNAL_COLUMNS:
        normalized_column = f"{signal_column}_normalized"

        try:
            result[normalized_column] = (
                grouped_zero_preserving_positive_percentile(
                    normalization_frame,
                    value_column=signal_column,
                    group_columns=[
                        "province_key",
                        "calendar_month",
                    ],
                )
            )
        except RiskScoringNormalizationError as exc:
            raise ClimateScoringError(str(exc)) from exc

        normalized_columns[signal_column] = normalized_column

    weighted_sum = pd.Series(
        0.0,
        index=dataframe.index,
        dtype="float64",
    )

    available_weight = pd.Series(
        0.0,
        index=dataframe.index,
        dtype="float64",
    )

    for signal_column, normalized_column in normalized_columns.items():
        weight = float(signal_weights[signal_column])
        normalized = result[normalized_column]

        weighted_sum += normalized.fillna(0.0) * weight
        available_weight += normalized.notna().astype("float64") * weight

    result["climate_signal_weight_coverage"] = available_weight

    result["climate_sub_score"] = (
        weighted_sum / available_weight.replace(0.0, np.nan)
    )

    result["climate_domain_available"] = (
        dataframe["has_climate_feature"].astype(bool)
        & result["climate_sub_score"].notna()
    )

    result["climate_spatial_quality"] = _build_spatial_quality(
        dataframe
    )

    completeness = pd.to_numeric(
        dataframe["climate_data_completeness_score"],
        errors="raise",
    )

    result["climate_effective_quality"] = (
        result["climate_spatial_quality"]
        * completeness.fillna(0.0)
        * result["climate_signal_weight_coverage"]
    ).clip(0.0, 1.0)

    no_coverage_mask = (
        dataframe["climate_mapping_method"]
        == NO_COVERAGE_METHOD
    )

    result.loc[
        no_coverage_mask,
        "climate_sub_score",
    ] = np.nan

    result.loc[
        no_coverage_mask,
        "climate_signal_weight_coverage",
    ] = 0.0

    result.loc[
        no_coverage_mask,
        "climate_effective_quality",
    ] = 0.0

    result.loc[
        no_coverage_mask,
        "climate_domain_available",
    ] = False

    _validate_output_ranges(result)

    return result
    
def _build_spatial_quality(dataframe: pd.DataFrame) -> pd.Series:
    methods = dataframe["climate_mapping_method"]

    result = pd.Series(
        np.nan,
        index=dataframe.index,
        dtype="float64",
    )

    direct_mask = methods.isin(DIRECT_MAPPING_METHODS)
    idw_mask = methods.eq(IDW_MAPPING_METHOD)
    no_coverage_mask = methods.eq(NO_COVERAGE_METHOD)

    result.loc[direct_mask] = 1.0

    result.loc[idw_mask] = pd.to_numeric(
        dataframe.loc[
            idw_mask,
            "climate_idw_confidence_score",
        ],
        errors="raise",
    )

    result.loc[no_coverage_mask] = 0.0

    unknown_mask = result.isna()

    if unknown_mask.any():
        unknown_methods = sorted(
            methods.loc[unknown_mask]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        raise ClimateScoringError(
            "Unsupported climate_mapping_method values: "
            f"{unknown_methods}"
        )

    return result

def _validate_signal_weights(signal_weights: Mapping[str, float]) -> None:
    expected = set(CLIMATE_SIGNAL_COLUMNS)
    provided = set(signal_weights)

    missing = expected - provided
    extra = provided - expected

    if missing or extra:
        raise ClimateScoringError(
            "Climate signal weights do not match expected signals. "
            f"Missing: {sorted(missing)}; extra: {sorted(extra)}"
        )

    weights = pd.Series(
        signal_weights,
        dtype="float64",
    )

    if weights.le(0).any():
        raise ClimateScoringError(
            "Climate signal weights must be positive."
        )

    if not np.isclose(weights.sum(), 1.0):
        raise ClimateScoringError(
            "Climate signal weights must sum to 1.0."
        )
        
def _validate_climate_inputs(dataframe: pd.DataFrame) -> None:
    methods = dataframe["climate_mapping_method"]

    allowed_methods = (
        DIRECT_MAPPING_METHODS
        | {
            IDW_MAPPING_METHOD,
            NO_COVERAGE_METHOD,
        }
    )

    invalid_methods = (
        methods.notna()
        & ~methods.isin(allowed_methods)
    )

    if invalid_methods.any():
        values = sorted(
            methods.loc[invalid_methods]
            .astype(str)
            .unique()
            .tolist()
        )

        raise ClimateScoringError(
            f"Unsupported climate_mapping_method values: {values}"
        )

    feature_mask = dataframe[
        "has_climate_feature"
    ].astype(bool)

    expected_feature_mask = ~methods.eq(
        NO_COVERAGE_METHOD
    )

    if not feature_mask.equals(
        expected_feature_mask
    ):
        raise ClimateScoringError(
            "has_climate_feature is inconsistent with "
            "climate_mapping_method."
        )

    idw_confidence = pd.to_numeric(
        dataframe["climate_idw_confidence_score"],
        errors="raise",
    )

    invalid_confidence = (
        idw_confidence.notna()
        & ~idw_confidence.between(0.0, 1.0)
    )

    if invalid_confidence.any():
        raise ClimateScoringError(
            "climate_idw_confidence_score must be in [0, 1]."
        )

    completeness = pd.to_numeric(
        dataframe["climate_data_completeness_score"],
        errors="raise",
    )

    invalid_completeness = (
        completeness.notna()
        & ~completeness.between(0.0, 1.0)
    )

    if invalid_completeness.any():
        raise ClimateScoringError(
            "climate_data_completeness_score must be in [0, 1]."
        )

    if completeness.loc[feature_mask].isna().any():
        raise ClimateScoringError(
            "Climate feature rows must have "
            "climate_data_completeness_score."
        )
        
def _validate_output_ranges(result: pd.DataFrame) -> None:
    columns = [
        "climate_sub_score",
        "climate_signal_weight_coverage",
        "climate_spatial_quality",
        "climate_effective_quality",
    ]

    for column in columns:
        values = result[column].dropna()

        if not values.between(0.0, 1.0).all():
            raise ClimateScoringError(
                f"{column} contains values outside [0, 1]."
            )
            
def _require_columns(dataframe: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(dataframe.columns)
    
    if missing:
        raise ClimateScoringError(
            f"Missing required Climate scoring columns: "
            f"{sorted(missing)}"
        )