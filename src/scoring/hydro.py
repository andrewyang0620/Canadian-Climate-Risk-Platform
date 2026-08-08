from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from src.scoring.normalization import (
    RiskScoringNormalizationError,
    calendar_month_from_reference_month,
    grouped_historical_percentile,
)

HYDRO_SIGNAL_NAMES = (
    "flow_p95",
    "flow_variability",
    "flow_zero_observation_ratio",
    "level_p95",
    "level_variability",
)

BASIN_METHOD = "basin_polygon_intersection"
POINT_METHOD = "station_point_in_cell"
NO_COVERAGE_METHOD = "no_hydro_coverage"

REQUIRED_COLUMNS = {
    "grid_cell_key",
    "reference_month",
    "has_hydro_spatial_coverage",
    "has_hydro_flow_feature",
    "has_hydro_level_feature",
    "has_hydro_feature",
    "hydro_spatial_assignment_method",
    "hydro_basin_grid_coverage_ratio",
    "hydro_data_completeness_score",
    "flow_mean_measurement_value",
    "flow_p95_measurement_value",
    "flow_min_measurement_value",
    "flow_max_measurement_value",
    "flow_zero_day_count",
    "flow_observation_day_count",
    "level_mean_measurement_value",
    "level_p95_measurement_value",
    "level_min_measurement_value",
    "level_max_measurement_value",
}


class HydroScoringError(ValueError):
    """Raised when Hydro scoring inputs are invalid."""


def build_hydro_scoring_features(
    dataframe: pd.DataFrame,
    *,
    signal_weights: Mapping[str, float],
    minimum_history_years: int,
    point_quality_factor: float,
) -> pd.DataFrame:
    """
    Build Hydro derived signals, historical percentiles,
    sub-score and scoring quality.

    The full reference-period mart should be supplied so local
    historical percentiles use the complete available history.
    """
    _require_columns(dataframe)
    _validate_parameters(
        signal_weights=signal_weights,
        minimum_history_years=minimum_history_years,
        point_quality_factor=point_quality_factor,
    )
    _validate_hydro_inputs(dataframe)

    working = dataframe.copy()

    working["calendar_month"] = calendar_month_from_reference_month(
        working["reference_month"]
    )

    working["flow_variability"] = (
        pd.to_numeric(
            working["flow_max_measurement_value"],
            errors="raise",
        )
        - pd.to_numeric(
            working["flow_min_measurement_value"],
            errors="raise",
        )
    )

    working["level_variability"] = (
        pd.to_numeric(
            working["level_max_measurement_value"],
            errors="raise",
        )
        - pd.to_numeric(
            working["level_min_measurement_value"],
            errors="raise",
        )
    )

    working["flow_zero_observation_ratio"] = (
        _build_zero_flow_observation_ratio(working)
    )

    result = pd.DataFrame(index=dataframe.index)

    result["flow_variability"] = working["flow_variability"]
    result["level_variability"] = working["level_variability"]
    result["flow_zero_observation_ratio"] = working[
        "flow_zero_observation_ratio"
    ]

    historical_signals = {
        "flow_p95": "flow_p95_measurement_value",
        "flow_variability": "flow_variability",
        "level_p95": "level_p95_measurement_value",
        "level_variability": "level_variability",
    }

    for signal_name, value_column in historical_signals.items():
        try:
            result[f"{signal_name}_normalized"] = (
                grouped_historical_percentile(
                    working,
                    value_column=value_column,
                    group_columns=[
                        "grid_cell_key",
                        "calendar_month",
                    ],
                    minimum_history_count=minimum_history_years,
                )
            )
        except RiskScoringNormalizationError as exc:
            raise HydroScoringError(str(exc)) from exc

    # The zero-flow ratio is already bounded in [0, 1].
    result["flow_zero_observation_ratio_normalized"] = result[
        "flow_zero_observation_ratio"
    ]

    normalized_columns = {
        "flow_p95": "flow_p95_normalized",
        "flow_variability": "flow_variability_normalized",
        "flow_zero_observation_ratio": (
            "flow_zero_observation_ratio_normalized"
        ),
        "level_p95": "level_p95_normalized",
        "level_variability": "level_variability_normalized",
    }

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

    for signal_name, normalized_column in normalized_columns.items():
        weight = float(signal_weights[signal_name])
        values = result[normalized_column]

        weighted_sum += values.fillna(0.0) * weight
        available_weight += values.notna().astype("float64") * weight

    result["hydro_signal_weight_coverage"] = available_weight

    result["hydro_sub_score"] = (
        weighted_sum
        / available_weight.replace(0.0, np.nan)
    )

    raw_domain_available = dataframe[
        "has_hydro_feature"
    ].astype(bool)

    result["hydro_domain_available"] = (
        raw_domain_available
        & result["hydro_sub_score"].notna()
    )

    result["hydro_spatial_quality"] = _build_spatial_quality(
        dataframe,
        point_quality_factor=point_quality_factor,
    )

    completeness = pd.to_numeric(
        dataframe["hydro_data_completeness_score"],
        errors="raise",
    )

    result["hydro_effective_quality"] = (
        result["hydro_spatial_quality"]
        * completeness.fillna(0.0)
        * result["hydro_signal_weight_coverage"]
    ).clip(0.0, 1.0)

    no_feature_mask = ~raw_domain_available

    result.loc[
        no_feature_mask,
        "hydro_sub_score",
    ] = np.nan

    result.loc[
        no_feature_mask,
        "hydro_signal_weight_coverage",
    ] = 0.0

    result.loc[
        no_feature_mask,
        "hydro_effective_quality",
    ] = 0.0

    result.loc[
        no_feature_mask,
        "hydro_domain_available",
    ] = False

    _validate_output_ranges(result)

    return result


def _build_zero_flow_observation_ratio(dataframe: pd.DataFrame) -> pd.Series:
    zero_count = pd.to_numeric(
        dataframe["flow_zero_day_count"],
        errors="raise",
    )

    observation_count = pd.to_numeric(
        dataframe["flow_observation_day_count"],
        errors="raise",
    )

    flow_available = dataframe[
        "has_hydro_flow_feature"
    ].astype(bool)

    valid = (
        flow_available
        & observation_count.gt(0)
    )

    result = pd.Series(
        np.nan,
        index=dataframe.index,
        dtype="float64",
    )

    result.loc[valid] = (
        zero_count.loc[valid]
        / observation_count.loc[valid]
    )

    invalid = (
        result.notna()
        & ~result.between(0.0, 1.0)
    )

    if invalid.any():
        raise HydroScoringError(
            "flow_zero_observation_ratio must be in [0, 1]."
        )

    return result


def _build_spatial_quality(dataframe: pd.DataFrame, *, point_quality_factor: float) -> pd.Series:
    methods = dataframe[
        "hydro_spatial_assignment_method"
    ]

    result = pd.Series(
        np.nan,
        index=dataframe.index,
        dtype="float64",
    )

    basin_mask = methods.eq(BASIN_METHOD)
    point_mask = methods.eq(POINT_METHOD)
    no_coverage_mask = methods.eq(NO_COVERAGE_METHOD)

    result.loc[basin_mask] = pd.to_numeric(
        dataframe.loc[
            basin_mask,
            "hydro_basin_grid_coverage_ratio",
        ],
        errors="raise",
    )

    result.loc[point_mask] = point_quality_factor
    result.loc[no_coverage_mask] = 0.0

    if result.isna().any():
        unknown_methods = sorted(
            methods.loc[result.isna()]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        raise HydroScoringError(
            "Unsupported hydro_spatial_assignment_method values: "
            f"{unknown_methods}"
        )

    return result


def _validate_parameters(
    *,
    signal_weights: Mapping[str, float],
    minimum_history_years: int,
    point_quality_factor: float,
) -> None:
    expected = set(HYDRO_SIGNAL_NAMES)
    provided = set(signal_weights)

    missing = expected - provided
    extra = provided - expected

    if missing or extra:
        raise HydroScoringError(
            "Hydro signal weights do not match expected signals. "
            f"Missing: {sorted(missing)}; extra: {sorted(extra)}"
        )

    weights = pd.Series(
        signal_weights,
        dtype="float64",
    )

    if weights.le(0).any():
        raise HydroScoringError(
            "Hydro signal weights must be positive."
        )

    if not np.isclose(weights.sum(), 1.0):
        raise HydroScoringError(
            "Hydro signal weights must sum to 1.0."
        )

    if minimum_history_years < 1:
        raise HydroScoringError(
            "minimum_history_years must be at least 1."
        )

    if not 0.0 <= point_quality_factor <= 1.0:
        raise HydroScoringError(
            "point_quality_factor must be in [0, 1]."
        )


def _validate_hydro_inputs(dataframe: pd.DataFrame) -> None:
    flag_columns = [
        "has_hydro_spatial_coverage",
        "has_hydro_flow_feature",
        "has_hydro_level_feature",
        "has_hydro_feature",
    ]

    if dataframe[flag_columns].isna().any().any():
        raise HydroScoringError(
            "Hydro availability flags must not contain null values."
        )

    spatial_flag = dataframe[
        "has_hydro_spatial_coverage"
    ].astype(bool)

    flow_flag = dataframe[
        "has_hydro_flow_feature"
    ].astype(bool)

    level_flag = dataframe[
        "has_hydro_level_feature"
    ].astype(bool)

    domain_flag = dataframe[
        "has_hydro_feature"
    ].astype(bool)

    methods = dataframe[
        "hydro_spatial_assignment_method"
    ]

    allowed_methods = {
        BASIN_METHOD,
        POINT_METHOD,
        NO_COVERAGE_METHOD,
    }

    invalid_methods = (
        methods.isna()
        | ~methods.isin(allowed_methods)
    )

    if invalid_methods.any():
        values = sorted(
            methods.loc[invalid_methods]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        raise HydroScoringError(
            "Unsupported hydro_spatial_assignment_method values: "
            f"{values}"
        )

    expected_spatial_flag = ~methods.eq(
        NO_COVERAGE_METHOD
    )

    if not spatial_flag.equals(
        expected_spatial_flag
    ):
        raise HydroScoringError(
            "has_hydro_spatial_coverage is inconsistent with "
            "hydro_spatial_assignment_method."
        )

    flow_mean = pd.to_numeric(
        dataframe["flow_mean_measurement_value"],
        errors="raise",
    )

    level_mean = pd.to_numeric(
        dataframe["level_mean_measurement_value"],
        errors="raise",
    )

    if not flow_flag.equals(flow_mean.notna()):
        raise HydroScoringError(
            "has_hydro_flow_feature is inconsistent with "
            "flow_mean_measurement_value."
        )

    if not level_flag.equals(level_mean.notna()):
        raise HydroScoringError(
            "has_hydro_level_feature is inconsistent with "
            "level_mean_measurement_value."
        )

    expected_domain_flag = flow_flag | level_flag

    if not domain_flag.equals(
        expected_domain_flag
    ):
        raise HydroScoringError(
            "has_hydro_feature is inconsistent with "
            "flow/level feature flags."
        )

    observation_count = pd.to_numeric(
        dataframe["flow_observation_day_count"],
        errors="raise",
    )

    zero_count = pd.to_numeric(
        dataframe["flow_zero_day_count"],
        errors="raise",
    )

    if observation_count.lt(0).any():
        raise HydroScoringError(
            "flow_observation_day_count cannot be negative."
        )

    if zero_count.lt(0).any():
        raise HydroScoringError(
            "flow_zero_day_count cannot be negative."
        )

    invalid_zero_count = (
        flow_flag
        & zero_count.gt(observation_count)
    )

    if invalid_zero_count.any():
        raise HydroScoringError(
            "flow_zero_day_count cannot exceed "
            "flow_observation_day_count."
        )

    invalid_flow_observation_count = (
        flow_flag
        & observation_count.le(0)
    )

    if invalid_flow_observation_count.any():
        raise HydroScoringError(
            "Flow feature rows must have positive "
            "flow_observation_day_count."
        )

    basin_coverage = pd.to_numeric(
        dataframe["hydro_basin_grid_coverage_ratio"],
        errors="raise",
    )

    invalid_basin_coverage = (
        basin_coverage.notna()
        & ~basin_coverage.between(0.0, 1.0)
    )

    if invalid_basin_coverage.any():
        raise HydroScoringError(
            "hydro_basin_grid_coverage_ratio must be in [0, 1]."
        )

    basin_mask = methods.eq(BASIN_METHOD)

    if basin_coverage.loc[basin_mask].isna().any():
        raise HydroScoringError(
            "Basin-mapped rows must have "
            "hydro_basin_grid_coverage_ratio."
        )

    completeness = pd.to_numeric(
        dataframe["hydro_data_completeness_score"],
        errors="raise",
    )

    invalid_completeness = (
        completeness.notna()
        & ~completeness.between(0.0, 1.0)
    )

    if invalid_completeness.any():
        raise HydroScoringError(
            "hydro_data_completeness_score must be in [0, 1]."
        )

    if completeness.loc[domain_flag].isna().any():
        raise HydroScoringError(
            "Hydro feature rows must have "
            "hydro_data_completeness_score."
        )


def _validate_output_ranges(result: pd.DataFrame) -> None:
    columns = [
        "flow_zero_observation_ratio",
        "flow_p95_normalized",
        "flow_variability_normalized",
        "flow_zero_observation_ratio_normalized",
        "level_p95_normalized",
        "level_variability_normalized",
        "hydro_signal_weight_coverage",
        "hydro_sub_score",
        "hydro_spatial_quality",
        "hydro_effective_quality",
    ]

    for column in columns:
        values = result[column].dropna()

        if not values.between(0.0, 1.0).all():
            raise HydroScoringError(
                f"{column} contains values outside [0, 1]."
            )


def _require_columns(dataframe: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(dataframe.columns)

    if missing:
        raise HydroScoringError(
            "Missing required Hydro scoring columns: "
            f"{sorted(missing)}"
        )