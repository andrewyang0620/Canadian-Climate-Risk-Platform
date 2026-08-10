import pandas as pd
import pytest

from src.scoring.normalization import (
    RiskScoringNormalizationError,
    calendar_month_from_reference_month,
    grouped_historical_percentile,
    grouped_zero_preserving_positive_percentile,
)


def test_calendar_month_from_reference_month():
    reference_month = pd.Series(
        [
            "2016-01",
            "2021-07",
            "2025-12",
        ]
    )

    result = calendar_month_from_reference_month(reference_month)

    assert result.tolist() == [1, 7, 12]


def test_zero_preserving_percentile_keeps_zero_at_zero():
    df = pd.DataFrame(
        {
            "province_key": ["AB", "AB", "AB"],
            "calendar_month": [7, 7, 7],
            "signal": [0.0, 2.0, 8.0],
        }
    )

    result = grouped_zero_preserving_positive_percentile(
        df,
        value_column="signal",
        group_columns=[
            "province_key",
            "calendar_month",
        ],
    )

    assert result.iloc[0] == 0.0
    assert result.iloc[1] == 0.5
    assert result.iloc[2] == 1.0


def test_zero_preserving_percentile_ranks_groups_independently():
    df = pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
                "BC",
                "BC",
            ],
            "signal": [
                1.0,
                10.0,
                100.0,
                200.0,
            ],
        }
    )

    result = grouped_zero_preserving_positive_percentile(
        df,
        value_column="signal",
        group_columns=["province_key"],
    )

    assert result.tolist() == [
        0.5,
        1.0,
        0.5,
        1.0,
    ]


def test_zero_preserving_percentile_uses_average_rank_for_ties():
    df = pd.DataFrame(
        {
            "province_key": ["BC", "BC", "BC"],
            "signal": [0.0, 5.0, 5.0],
        }
    )

    result = grouped_zero_preserving_positive_percentile(
        df,
        value_column="signal",
        group_columns=["province_key"],
    )

    assert result.iloc[0] == 0.0
    assert result.iloc[1] == 0.75
    assert result.iloc[2] == 0.75


def test_zero_preserving_percentile_keeps_null_as_null():
    df = pd.DataFrame(
        {
            "province_key": ["AB", "AB", "AB"],
            "signal": [None, 0.0, 2.0],
        }
    )

    result = grouped_zero_preserving_positive_percentile(
        df,
        value_column="signal",
        group_columns=["province_key"],
    )

    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == 0.0
    assert result.iloc[2] == 1.0


def test_zero_preserving_percentile_rejects_negative_values():
    df = pd.DataFrame(
        {
            "province_key": ["AB", "AB"],
            "signal": [0.0, -1.0],
        }
    )

    with pytest.raises(
        RiskScoringNormalizationError,
        match="negative values",
    ):
        grouped_zero_preserving_positive_percentile(
            df,
            value_column="signal",
            group_columns=["province_key"],
        )


def test_historical_percentile_uses_local_history():
    df = pd.DataFrame(
        {
            "grid_cell_key": ["grid_a"] * 5,
            "calendar_month": [7] * 5,
            "signal": [
                10.0,
                20.0,
                30.0,
                40.0,
                50.0,
            ],
        }
    )

    result = grouped_historical_percentile(
        df,
        value_column="signal",
        group_columns=[
            "grid_cell_key",
            "calendar_month",
        ],
        minimum_history_count=5,
    )

    assert result.tolist() == [
        0.2,
        0.4,
        0.6,
        0.8,
        1.0,
    ]


def test_historical_percentile_rejects_short_history():
    df = pd.DataFrame(
        {
            "grid_cell_key": ["grid_a"] * 4,
            "calendar_month": [7] * 4,
            "signal": [
                10.0,
                20.0,
                30.0,
                40.0,
            ],
        }
    )

    result = grouped_historical_percentile(
        df,
        value_column="signal",
        group_columns=[
            "grid_cell_key",
            "calendar_month",
        ],
        minimum_history_count=5,
    )

    assert result.isna().all()


def test_historical_percentile_counts_only_nonnull_history():
    df = pd.DataFrame(
        {
            "grid_cell_key": ["grid_a"] * 6,
            "calendar_month": [7] * 6,
            "signal": [
                10.0,
                20.0,
                None,
                30.0,
                40.0,
                50.0,
            ],
        }
    )

    result = grouped_historical_percentile(
        df,
        value_column="signal",
        group_columns=[
            "grid_cell_key",
            "calendar_month",
        ],
        minimum_history_count=5,
    )

    assert result.iloc[0] == 0.2
    assert result.iloc[1] == 0.4
    assert pd.isna(result.iloc[2])
    assert result.iloc[3] == 0.6
    assert result.iloc[4] == 0.8
    assert result.iloc[5] == 1.0


def test_historical_percentile_groups_grids_independently():
    df = pd.DataFrame(
        {
            "grid_cell_key": (["grid_a"] * 5 + ["grid_b"] * 5),
            "calendar_month": [7] * 10,
            "signal": [
                1.0,
                2.0,
                3.0,
                4.0,
                5.0,
                100.0,
                200.0,
                300.0,
                400.0,
                500.0,
            ],
        }
    )

    result = grouped_historical_percentile(
        df,
        value_column="signal",
        group_columns=[
            "grid_cell_key",
            "calendar_month",
        ],
        minimum_history_count=5,
    )

    assert result.iloc[0] == 0.2
    assert result.iloc[4] == 1.0

    assert result.iloc[5] == 0.2
    assert result.iloc[9] == 1.0
