import pandas as pd
import pytest

from src.scoring.hydro import (
    HydroScoringError,
    build_hydro_scoring_features,
)


HYDRO_WEIGHTS = {
    "flow_p95": 0.30,
    "flow_variability": 0.15,
    "flow_zero_observation_ratio": 0.15,
    "level_p95": 0.25,
    "level_variability": 0.15,
}


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "grid_cell_key": ["grid_a"] * 5,
            "reference_month": [
                "2019-07",
                "2020-07",
                "2021-07",
                "2022-07",
                "2023-07",
            ],
            "has_hydro_spatial_coverage": [True] * 5,
            "has_hydro_flow_feature": [True] * 5,
            "has_hydro_level_feature": [True] * 5,
            "has_hydro_feature": [True] * 5,
            "hydro_spatial_assignment_method": [
                "basin_polygon_intersection"
            ] * 5,
            "hydro_basin_grid_coverage_ratio": [0.8] * 5,
            "hydro_data_completeness_score": [1.0] * 5,
            "flow_mean_measurement_value": [
                5.0,
                10.0,
                15.0,
                20.0,
                25.0,
            ],
            "flow_p95_measurement_value": [
                10.0,
                20.0,
                30.0,
                40.0,
                50.0,
            ],
            "flow_min_measurement_value": [
                1.0,
                2.0,
                3.0,
                4.0,
                5.0,
            ],
            "flow_max_measurement_value": [
                2.0,
                4.0,
                6.0,
                8.0,
                10.0,
            ],
            "flow_zero_day_count": [
                0,
                1,
                2,
                3,
                4,
            ],
            "flow_observation_day_count": [
                10,
                10,
                10,
                10,
                10,
            ],
            "level_mean_measurement_value": [
                50.0,
                100.0,
                150.0,
                200.0,
                250.0,
            ],
            "level_p95_measurement_value": [
                100.0,
                200.0,
                300.0,
                400.0,
                500.0,
            ],
            "level_min_measurement_value": [
                10.0,
                20.0,
                30.0,
                40.0,
                50.0,
            ],
            "level_max_measurement_value": [
                20.0,
                40.0,
                60.0,
                80.0,
                100.0,
            ],
        }
    )


def _score(
    dataframe: pd.DataFrame,
    *,
    minimum_history_years: int = 5,
    point_quality_factor: float = 0.70,
) -> pd.DataFrame:
    return build_hydro_scoring_features(
        dataframe,
        signal_weights=HYDRO_WEIGHTS,
        minimum_history_years=minimum_history_years,
        point_quality_factor=point_quality_factor,
    )


def test_hydro_builds_derived_signals():
    result = _score(_base_frame())

    assert result.loc[
        2,
        "flow_variability",
    ] == 3.0

    assert result.loc[
        2,
        "level_variability",
    ] == 30.0

    assert result.loc[
        2,
        "flow_zero_observation_ratio",
    ] == pytest.approx(0.2)


def test_hydro_uses_local_historical_percentiles():
    result = _score(_base_frame())

    assert result[
        "flow_p95_normalized"
    ].tolist() == pytest.approx(
        [
            0.2,
            0.4,
            0.6,
            0.8,
            1.0,
        ]
    )

    assert result[
        "level_p95_normalized"
    ].tolist() == pytest.approx(
        [
            0.2,
            0.4,
            0.6,
            0.8,
            1.0,
        ]
    )


def test_hydro_requires_minimum_history():
    df = _base_frame().iloc[:4].copy()

    result = _score(
        df,
        minimum_history_years=5,
    )

    assert result[
        "flow_p95_normalized"
    ].isna().all()

    assert result[
        "flow_variability_normalized"
    ].isna().all()

    assert result[
        "level_p95_normalized"
    ].isna().all()

    assert result[
        "level_variability_normalized"
    ].isna().all()

    # Zero-flow ratio does not require historical ranking.
    assert result[
        "flow_zero_observation_ratio"
    ].notna().all()


def test_hydro_full_signal_coverage_is_one():
    result = _score(_base_frame())

    assert (
        result["hydro_signal_weight_coverage"]
        == 1.0
    ).all()


def test_hydro_flow_only_renormalizes_weights():
    df = _base_frame()

    df["has_hydro_level_feature"] = False
    df["level_mean_measurement_value"] = None
    df["level_p95_measurement_value"] = None
    df["level_min_measurement_value"] = None
    df["level_max_measurement_value"] = None

    result = _score(df)

    assert result.loc[
        2,
        "hydro_signal_weight_coverage",
    ] == pytest.approx(0.60)

    expected = (
        0.30 * 0.60
        + 0.15 * 0.60
        + 0.15 * 0.20
    ) / 0.60

    assert result.loc[
        2,
        "hydro_sub_score",
    ] == pytest.approx(expected)

    assert bool(
        result.loc[
            2,
            "hydro_domain_available",
        ]
    )


def test_hydro_level_only_renormalizes_weights():
    df = _base_frame()

    df["has_hydro_flow_feature"] = False

    df["flow_mean_measurement_value"] = None
    df["flow_p95_measurement_value"] = None
    df["flow_min_measurement_value"] = None
    df["flow_max_measurement_value"] = None

    df["flow_zero_day_count"] = 0
    df["flow_observation_day_count"] = 0

    result = _score(df)

    assert result.loc[
        2,
        "hydro_signal_weight_coverage",
    ] == pytest.approx(0.40)

    expected = (
        0.25 * 0.60
        + 0.15 * 0.60
    ) / 0.40

    assert result.loc[
        2,
        "hydro_sub_score",
    ] == pytest.approx(expected)

    assert pd.isna(
        result.loc[
            2,
            "flow_zero_observation_ratio",
        ]
    )


def test_basin_quality_uses_basin_coverage():
    df = _base_frame()

    df[
        "hydro_data_completeness_score"
    ] = 0.9

    result = _score(df)

    assert result.loc[
        2,
        "hydro_spatial_quality",
    ] == pytest.approx(0.8)

    assert result.loc[
        2,
        "hydro_effective_quality",
    ] == pytest.approx(
        0.8 * 0.9
    )


def test_point_mapping_uses_point_quality_factor():
    df = _base_frame()

    df[
        "hydro_spatial_assignment_method"
    ] = "station_point_in_cell"

    df[
        "hydro_basin_grid_coverage_ratio"
    ] = None

    result = _score(
        df,
        point_quality_factor=0.70,
    )

    assert result.loc[
        2,
        "hydro_spatial_quality",
    ] == pytest.approx(0.70)

    assert result.loc[
        2,
        "hydro_effective_quality",
    ] == pytest.approx(0.70)


def test_partial_signals_reduce_effective_quality():
    df = _base_frame()

    df["has_hydro_level_feature"] = False
    df["level_mean_measurement_value"] = None
    df["level_p95_measurement_value"] = None
    df["level_min_measurement_value"] = None
    df["level_max_measurement_value"] = None

    result = _score(df)

    assert result.loc[
        2,
        "hydro_signal_weight_coverage",
    ] == pytest.approx(0.60)

    assert result.loc[
        2,
        "hydro_effective_quality",
    ] == pytest.approx(
        0.8 * 1.0 * 0.60
    )


def test_no_hydro_feature_produces_null_score_and_zero_quality():
    df = _base_frame()

    df.loc[
        0,
        "has_hydro_spatial_coverage",
    ] = False

    df.loc[
        0,
        "has_hydro_flow_feature",
    ] = False

    df.loc[
        0,
        "has_hydro_level_feature",
    ] = False

    df.loc[
        0,
        "has_hydro_feature",
    ] = False

    df.loc[
        0,
        "hydro_spatial_assignment_method",
    ] = "no_hydro_coverage"

    df.loc[
        0,
        "hydro_basin_grid_coverage_ratio",
    ] = None

    df.loc[
        0,
        "hydro_data_completeness_score",
    ] = None

    for column in [
        "flow_mean_measurement_value",
        "flow_p95_measurement_value",
        "flow_min_measurement_value",
        "flow_max_measurement_value",
        "level_mean_measurement_value",
        "level_p95_measurement_value",
        "level_min_measurement_value",
        "level_max_measurement_value",
    ]:
        df.loc[0, column] = None

    df.loc[
        0,
        "flow_zero_day_count",
    ] = 0

    df.loc[
        0,
        "flow_observation_day_count",
    ] = 0

    result = _score(df)

    assert pd.isna(
        result.loc[
            0,
            "hydro_sub_score",
        ]
    )

    assert result.loc[
        0,
        "hydro_signal_weight_coverage",
    ] == 0.0

    assert result.loc[
        0,
        "hydro_spatial_quality",
    ] == 0.0

    assert result.loc[
        0,
        "hydro_effective_quality",
    ] == 0.0

    assert not bool(
        result.loc[
            0,
            "hydro_domain_available",
        ]
    )


def test_rejects_zero_count_greater_than_observation_count():
    df = _base_frame()

    df.loc[
        0,
        "flow_zero_day_count",
    ] = 11

    with pytest.raises(
        HydroScoringError,
        match="cannot exceed",
    ):
        _score(df)


def test_rejects_inconsistent_domain_flag():
    df = _base_frame()

    df.loc[
        0,
        "has_hydro_feature",
    ] = False

    with pytest.raises(
        HydroScoringError,
        match="has_hydro_feature is inconsistent",
    ):
        _score(df)


def test_rejects_weights_that_do_not_sum_to_one():
    bad_weights = HYDRO_WEIGHTS.copy()
    bad_weights["flow_p95"] = 0.20

    with pytest.raises(
        HydroScoringError,
        match="sum to 1.0",
    ):
        build_hydro_scoring_features(
            _base_frame(),
            signal_weights=bad_weights,
            minimum_history_years=5,
            point_quality_factor=0.70,
        )