import pandas as pd
import pytest

from src.scoring.wildfire import (
    WildfireScoringError,
    build_wildfire_scoring_features,
)


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
                "AB",
                "BC",
                "BC",
                "BC",
            ],
            "has_wildfire_perimeter_feature": [
                True,
                True,
                True,
                True,
                True,
                True,
            ],
            "has_wildfire_observed_perimeter_overlap": [
                False,
                True,
                True,
                False,
                True,
                True,
            ],
            "wildfire_perimeter_count": [
                0,
                1,
                2,
                0,
                1,
                1,
            ],
            "wildfire_intersection_area_ratio_of_grid": [
                0.0,
                0.01,
                0.03,
                0.0,
                0.20,
                0.40,
            ],
        }
    )


def test_wildfire_preserves_zero_and_ranks_positive_values():
    result = build_wildfire_scoring_features(
        _base_frame()
    )

    assert result[
        "wildfire_sub_score"
    ].tolist() == pytest.approx(
        [
            0.0,
            0.5,
            1.0,
            0.0,
            0.5,
            1.0,
        ]
    )


def test_wildfire_ranks_provinces_independently():
    result = build_wildfire_scoring_features(
        _base_frame()
    )

    # AB 0.01 and BC 0.20 are both the smaller
    # positive values in their own provinces.
    assert result.loc[
        1,
        "wildfire_sub_score",
    ] == pytest.approx(0.5)

    assert result.loc[
        4,
        "wildfire_sub_score",
    ] == pytest.approx(0.5)


def test_wildfire_uses_average_rank_for_ties():
    df = pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
                "AB",
            ],
            "has_wildfire_perimeter_feature": [
                True,
                True,
                True,
            ],
            "has_wildfire_observed_perimeter_overlap": [
                False,
                True,
                True,
            ],
            "wildfire_perimeter_count": [
                0,
                1,
                1,
            ],
            "wildfire_intersection_area_ratio_of_grid": [
                0.0,
                0.02,
                0.02,
            ],
        }
    )

    result = build_wildfire_scoring_features(df)

    assert result.loc[
        0,
        "wildfire_sub_score",
    ] == 0.0

    assert result.loc[
        1,
        "wildfire_sub_score",
    ] == pytest.approx(0.75)

    assert result.loc[
        2,
        "wildfire_sub_score",
    ] == pytest.approx(0.75)


def test_wildfire_available_rows_have_full_quality():
    result = build_wildfire_scoring_features(
        _base_frame(),
        fixed_quality=1.0,
    )

    assert (
        result["wildfire_domain_available"]
    ).all()

    assert (
        result["wildfire_effective_quality"]
        == 1.0
    ).all()


def test_wildfire_unavailable_row_has_null_score():
    df = _base_frame()

    df.loc[
        0,
        "has_wildfire_perimeter_feature",
    ] = False

    df.loc[
        0,
        "wildfire_perimeter_count",
    ] = None

    df.loc[
        0,
        "wildfire_intersection_area_ratio_of_grid",
    ] = None

    result = build_wildfire_scoring_features(df)

    assert pd.isna(
        result.loc[
            0,
            "wildfire_sub_score",
        ]
    )

    assert not bool(
        result.loc[
            0,
            "wildfire_domain_available",
        ]
    )

    assert result.loc[
        0,
        "wildfire_effective_quality",
    ] == 0.0


def test_rejects_burn_ratio_outside_range():
    df = _base_frame()

    df.loc[
        1,
        "wildfire_intersection_area_ratio_of_grid",
    ] = 1.1

    with pytest.raises(
        WildfireScoringError,
        match="must be in",
    ):
        build_wildfire_scoring_features(df)


def test_rejects_overlap_flag_inconsistent_with_count():
    df = _base_frame()

    df.loc[
        1,
        "has_wildfire_observed_perimeter_overlap",
    ] = False

    with pytest.raises(
        WildfireScoringError,
        match="inconsistent",
    ):
        build_wildfire_scoring_features(df)


def test_rejects_positive_ratio_without_overlap():
    df = _base_frame()

    df.loc[
        0,
        "wildfire_intersection_area_ratio_of_grid",
    ] = 0.10

    with pytest.raises(
        WildfireScoringError,
        match="no-overlap",
    ):
        build_wildfire_scoring_features(df)


def test_rejects_invalid_fixed_quality():
    with pytest.raises(
        WildfireScoringError,
        match="fixed_quality",
    ):
        build_wildfire_scoring_features(
            _base_frame(),
            fixed_quality=1.2,
        )