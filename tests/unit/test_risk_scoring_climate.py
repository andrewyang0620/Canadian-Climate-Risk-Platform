import pandas as pd
import pytest

from src.scoring.climate import (
    ClimateScoringError,
    build_climate_scoring_features,
)


CLIMATE_WEIGHTS = {
    "climate_extreme_heat_days": 0.30,
    "climate_heavy_precipitation_days": 0.25,
    "climate_freeze_thaw_days": 0.15,
    "climate_extreme_cold_days": 0.15,
    "climate_total_precip_mm": 0.15,
}


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "province_key": ["AB", "AB", "AB"],
            "reference_month": [
                "2021-07",
                "2022-07",
                "2023-07",
            ],
            "has_climate_feature": [
                True,
                True,
                True,
            ],
            "climate_mapping_method": [
                "direct_station_in_cell",
                "direct_station_in_cell",
                "direct_station_in_cell",
            ],
            "climate_idw_confidence_score": [
                1.0,
                1.0,
                1.0,
            ],
            "climate_data_completeness_score": [
                1.0,
                1.0,
                1.0,
            ],
            "climate_extreme_heat_days": [
                0.0,
                2.0,
                4.0,
            ],
            "climate_heavy_precipitation_days": [
                0.0,
                1.0,
                2.0,
            ],
            "climate_freeze_thaw_days": [
                0.0,
                1.0,
                2.0,
            ],
            "climate_extreme_cold_days": [
                0.0,
                0.0,
                1.0,
            ],
            "climate_total_precip_mm": [
                0.0,
                50.0,
                100.0,
            ],
        }
    )


def test_climate_score_preserves_zero_hazard():
    result = build_climate_scoring_features(
        _base_frame(),
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        0,
        "climate_sub_score",
    ] == 0.0

    assert result.loc[
        0,
        "climate_signal_weight_coverage",
    ] == 1.0


def test_climate_score_uses_positive_percentiles():
    result = build_climate_scoring_features(
        _base_frame(),
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        1,
        "climate_extreme_heat_days_normalized",
    ] == 0.5

    assert result.loc[
        2,
        "climate_extreme_heat_days_normalized",
    ] == 1.0


def test_climate_score_renormalizes_missing_signal():
    df = _base_frame()

    df.loc[
        1,
        "climate_total_precip_mm",
    ] = None

    result = build_climate_scoring_features(
        df,
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        1,
        "climate_signal_weight_coverage",
    ] == pytest.approx(0.85)

    expected = (
        0.30 * 0.5
        + 0.25 * 0.5
        + 0.15 * 0.5
        + 0.15 * 0.0
    ) / 0.85

    assert result.loc[
        1,
        "climate_sub_score",
    ] == pytest.approx(expected)


def test_no_coverage_produces_null_score_and_zero_quality():
    df = _base_frame()

    df.loc[
        0,
        "has_climate_feature",
    ] = False

    df.loc[
        0,
        "climate_mapping_method",
    ] = "no_station_within_radius"

    df.loc[
        0,
        "climate_idw_confidence_score",
    ] = 0.0

    df.loc[
        0,
        "climate_data_completeness_score",
    ] = None

    for column in CLIMATE_WEIGHTS:
        df.loc[0, column] = None

    result = build_climate_scoring_features(
        df,
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert pd.isna(
        result.loc[
            0,
            "climate_sub_score",
        ]
    )

    assert result.loc[
        0,
        "climate_signal_weight_coverage",
    ] == 0.0

    assert result.loc[
        0,
        "climate_spatial_quality",
    ] == 0.0

    assert result.loc[
        0,
        "climate_effective_quality",
    ] == 0.0

    assert not bool(
        result.loc[
            0,
            "climate_domain_available",
        ]
    )


def test_direct_mapping_quality_uses_full_spatial_quality():
    df = _base_frame()

    df.loc[
        1,
        "climate_data_completeness_score",
    ] = 0.8

    result = build_climate_scoring_features(
        df,
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        1,
        "climate_spatial_quality",
    ] == 1.0

    assert result.loc[
        1,
        "climate_effective_quality",
    ] == pytest.approx(0.8)


def test_idw_quality_uses_idw_confidence():
    df = _base_frame()

    df.loc[
        1,
        "climate_mapping_method",
    ] = "idw_interpolated"

    df.loc[
        1,
        "climate_idw_confidence_score",
    ] = 0.5

    df.loc[
        1,
        "climate_data_completeness_score",
    ] = 0.8

    result = build_climate_scoring_features(
        df,
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        1,
        "climate_spatial_quality",
    ] == pytest.approx(0.5)

    assert result.loc[
        1,
        "climate_effective_quality",
    ] == pytest.approx(0.4)


def test_missing_signal_reduces_effective_quality():
    df = _base_frame()

    df.loc[
        1,
        "climate_total_precip_mm",
    ] = None

    result = build_climate_scoring_features(
        df,
        signal_weights=CLIMATE_WEIGHTS,
    )

    assert result.loc[
        1,
        "climate_signal_weight_coverage",
    ] == pytest.approx(0.85)

    assert result.loc[
        1,
        "climate_effective_quality",
    ] == pytest.approx(0.85)


def test_rejects_inconsistent_feature_flag():
    df = _base_frame()

    df.loc[
        0,
        "has_climate_feature",
    ] = False

    with pytest.raises(
        ClimateScoringError,
        match="inconsistent",
    ):
        build_climate_scoring_features(
            df,
            signal_weights=CLIMATE_WEIGHTS,
        )


def test_rejects_weights_that_do_not_sum_to_one():
    bad_weights = CLIMATE_WEIGHTS.copy()
    bad_weights[
        "climate_extreme_heat_days"
    ] = 0.20

    with pytest.raises(
        ClimateScoringError,
        match="sum to 1.0",
    ):
        build_climate_scoring_features(
            _base_frame(),
            signal_weights=bad_weights,
        )