import pandas as pd
import pytest

from src.scoring.composite import (
    CompositeScoringError,
    build_composite_scoring_features,
)


DOMAIN_WEIGHTS = {
    "climate": 0.35,
    "hydro": 0.35,
    "wildfire": 0.30,
}


def _base_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "climate_sub_score": [
                0.80,
                None,
                None,
            ],
            "hydro_sub_score": [
                0.60,
                0.70,
                None,
            ],
            "wildfire_sub_score": [
                0.20,
                0.30,
                0.80,
            ],
            "climate_domain_available": [
                True,
                False,
                False,
            ],
            "hydro_domain_available": [
                True,
                True,
                False,
            ],
            "wildfire_domain_available": [
                True,
                True,
                True,
            ],
            "climate_effective_quality": [
                1.00,
                0.00,
                0.00,
            ],
            "hydro_effective_quality": [
                1.00,
                1.00,
                0.00,
            ],
            "wildfire_effective_quality": [
                1.00,
                1.00,
                1.00,
            ],
        }
    )


def _score(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    return build_composite_scoring_features(
        dataframe,
        domain_weights=DOMAIN_WEIGHTS,
        minimum_available_domains=2,
    )


def test_all_domains_use_base_weights():
    result = _score(_base_frame())

    expected = (
        0.35 * 0.80
        + 0.35 * 0.60
        + 0.30 * 0.20
    )

    assert result.loc[
        0,
        "composite_risk_score",
    ] == pytest.approx(expected)

    assert result.loc[
        0,
        "climate_effective_weight",
    ] == pytest.approx(0.35)

    assert result.loc[
        0,
        "hydro_effective_weight",
    ] == pytest.approx(0.35)

    assert result.loc[
        0,
        "wildfire_effective_weight",
    ] == pytest.approx(0.30)


def test_missing_domain_renormalizes_composite_weights():
    result = _score(_base_frame())

    expected = (
        0.35 * 0.70
        + 0.30 * 0.30
    ) / (
        0.35 + 0.30
    )

    assert result.loc[
        1,
        "composite_risk_score",
    ] == pytest.approx(expected)

    assert result.loc[
        1,
        "climate_effective_weight",
    ] == 0.0

    assert result.loc[
        1,
        "hydro_effective_weight",
    ] == pytest.approx(
        0.35 / 0.65
    )

    assert result.loc[
        1,
        "wildfire_effective_weight",
    ] == pytest.approx(
        0.30 / 0.65
    )


def test_domain_coverage_is_tracked():
    result = _score(_base_frame())

    assert result[
        "domain_coverage_count"
    ].tolist() == [
        3,
        2,
        1,
    ]

    assert result.loc[
        0,
        "domain_coverage_ratio",
    ] == pytest.approx(1.0)

    assert result.loc[
        1,
        "domain_coverage_ratio",
    ] == pytest.approx(2.0 / 3.0)

    assert result.loc[
        2,
        "domain_coverage_ratio",
    ] == pytest.approx(1.0 / 3.0)


def test_two_domains_are_composite_eligible():
    result = _score(_base_frame())

    assert bool(
        result.loc[
            1,
            "composite_score_eligible",
        ]
    )


def test_single_domain_does_not_receive_composite_score():
    result = _score(_base_frame())

    assert not bool(
        result.loc[
            2,
            "composite_score_eligible",
        ]
    )

    assert pd.isna(
        result.loc[
            2,
            "composite_risk_score",
        ]
    )

    assert pd.isna(
        result.loc[
            2,
            "wildfire_effective_weight",
        ]
    )

    assert pd.isna(
        result.loc[
            2,
            "wildfire_component_contribution",
        ]
    )


def test_confidence_does_not_renormalize_missing_domains():
    result = _score(_base_frame())

    # Climate is missing on row 1.
    expected = (
        0.35 * 0.0
        + 0.35 * 1.0
        + 0.30 * 1.0
    )

    assert result.loc[
        1,
        "score_confidence",
    ] == pytest.approx(0.65)

    # Only Wildfire evidence exists on row 2.
    assert result.loc[
        2,
        "score_confidence",
    ] == pytest.approx(0.30)


def test_confidence_uses_partial_domain_quality():
    df = _base_frame()

    df.loc[
        0,
        "climate_effective_quality",
    ] = 0.80

    df.loc[
        0,
        "hydro_effective_quality",
    ] = 0.50

    result = _score(df)

    expected = (
        0.35 * 0.80
        + 0.35 * 0.50
        + 0.30 * 1.00
    )

    assert result.loc[
        0,
        "score_confidence",
    ] == pytest.approx(expected)


def test_component_contributions_sum_to_composite_score():
    result = _score(_base_frame())

    contribution_columns = [
        "climate_component_contribution",
        "hydro_component_contribution",
        "wildfire_component_contribution",
    ]

    for index in [0, 1]:
        contribution_sum = result.loc[
            index,
            contribution_columns,
        ].sum()

        assert contribution_sum == pytest.approx(
            result.loc[
                index,
                "composite_risk_score",
            ]
        )


def test_rejects_availability_inconsistent_with_score():
    df = _base_frame()

    df.loc[
        0,
        "climate_domain_available",
    ] = False

    with pytest.raises(
        CompositeScoringError,
        match="inconsistent",
    ):
        _score(df)


def test_rejects_quality_outside_range():
    df = _base_frame()

    df.loc[
        0,
        "hydro_effective_quality",
    ] = 1.20

    with pytest.raises(
        CompositeScoringError,
        match=r"\[0, 1\]",
    ):
        _score(df)


def test_rejects_nonzero_quality_for_unavailable_domain():
    df = _base_frame()

    df.loc[
        1,
        "climate_effective_quality",
    ] = 0.50

    with pytest.raises(
        CompositeScoringError,
        match="must be 0",
    ):
        _score(df)


def test_rejects_domain_weights_that_do_not_sum_to_one():
    bad_weights = DOMAIN_WEIGHTS.copy()
    bad_weights["wildfire"] = 0.20

    with pytest.raises(
        CompositeScoringError,
        match="sum to 1.0",
    ):
        build_composite_scoring_features(
            _base_frame(),
            domain_weights=bad_weights,
            minimum_available_domains=2,
        )


def test_rejects_invalid_minimum_domain_count():
    with pytest.raises(
        CompositeScoringError,
        match="minimum_available_domains",
    ):
        build_composite_scoring_features(
            _base_frame(),
            domain_weights=DOMAIN_WEIGHTS,
            minimum_available_domains=4,
        )