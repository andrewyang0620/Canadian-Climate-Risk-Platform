import pandas as pd
import pytest

from src.scoring.ranking import (
    RiskRankingError,
    build_ranking_features,
)


PRIORITY_TIERS = {
    "very_high": 0.90,
    "high": 0.75,
    "elevated": 0.50,
    "moderate": 0.25,
}


def _score(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    return build_ranking_features(
        dataframe,
        group_columns=[
            "province_key",
            "reference_month",
        ],
        minimum_boundary_coverage_ratio=0.01,
        priority_tiers=PRIORITY_TIERS,
    )


def test_ranking_eligibility_requires_composite_and_boundary():
    df = pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
                "AB",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "boundary_coverage_ratio": [
                1.0,
                0.005,
                1.0,
            ],
            "composite_score_eligible": [
                True,
                True,
                False,
            ],
            "composite_risk_score": [
                0.5,
                0.8,
                None,
            ],
        }
    )

    result = _score(df)

    assert result[
        "ranking_eligible"
    ].tolist() == [
        True,
        False,
        False,
    ]


def test_boundary_sliver_gets_exclusion_reason():
    df = pd.DataFrame(
        {
            "province_key": ["AB"],
            "reference_month": ["2021-07"],
            "boundary_coverage_ratio": [0.005],
            "composite_score_eligible": [True],
            "composite_risk_score": [0.80],
        }
    )

    result = _score(df)

    assert result.loc[
        0,
        "ranking_exclusion_reason",
    ] == "boundary_sliver"

    assert pd.isna(
        result.loc[
            0,
            "priority_percentile",
        ]
    )

    assert result.loc[
        0,
        "priority_tier",
    ] == "insufficient_data"


def test_insufficient_domain_coverage_gets_exclusion_reason():
    df = pd.DataFrame(
        {
            "province_key": ["BC"],
            "reference_month": ["2021-07"],
            "boundary_coverage_ratio": [1.0],
            "composite_score_eligible": [False],
            "composite_risk_score": [None],
        }
    )

    result = _score(df)

    assert result.loc[
        0,
        "ranking_exclusion_reason",
    ] == "insufficient_domain_coverage"

    assert result.loc[
        0,
        "priority_tier",
    ] == "insufficient_data"


def test_ranking_is_independent_by_province():
    df = pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
                "BC",
                "BC",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "boundary_coverage_ratio": [1.0] * 4,
            "composite_score_eligible": [True] * 4,
            "composite_risk_score": [
                0.10,
                0.20,
                0.80,
                0.90,
            ],
        }
    )

    result = _score(df)

    assert result[
        "priority_percentile"
    ].tolist() == pytest.approx(
        [
            0.5,
            1.0,
            0.5,
            1.0,
        ]
    )


def test_ranking_is_independent_by_month():
    df = pd.DataFrame(
        {
            "province_key": ["AB"] * 4,
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-08",
                "2021-08",
            ],
            "boundary_coverage_ratio": [1.0] * 4,
            "composite_score_eligible": [True] * 4,
            "composite_risk_score": [
                0.10,
                0.20,
                0.80,
                0.90,
            ],
        }
    )

    result = _score(df)

    assert result[
        "priority_percentile"
    ].tolist() == pytest.approx(
        [
            0.5,
            1.0,
            0.5,
            1.0,
        ]
    )


def test_ranking_uses_average_rank_for_ties():
    df = pd.DataFrame(
        {
            "province_key": ["AB"] * 4,
            "reference_month": ["2021-07"] * 4,
            "boundary_coverage_ratio": [1.0] * 4,
            "composite_score_eligible": [True] * 4,
            "composite_risk_score": [
                0.10,
                0.20,
                0.20,
                0.40,
            ],
        }
    )

    result = _score(df)

    assert result[
        "priority_percentile"
    ].tolist() == pytest.approx(
        [
            0.25,
            0.625,
            0.625,
            1.0,
        ]
    )


def test_priority_tiers_follow_percentile_thresholds():
    df = pd.DataFrame(
        {
            "province_key": ["AB"] * 20,
            "reference_month": ["2021-07"] * 20,
            "boundary_coverage_ratio": [1.0] * 20,
            "composite_score_eligible": [True] * 20,
            "composite_risk_score": [
                value / 20
                for value in range(1, 21)
            ],
        }
    )

    result = _score(df)

    assert result.loc[
        19,
        "priority_tier",
    ] == "very_high"

    assert result.loc[
        15,
        "priority_tier",
    ] == "high"

    assert result.loc[
        11,
        "priority_tier",
    ] == "elevated"

    assert result.loc[
        5,
        "priority_tier",
    ] == "moderate"

    assert result.loc[
        1,
        "priority_tier",
    ] == "low"


def test_boundary_threshold_is_inclusive():
    df = pd.DataFrame(
        {
            "province_key": [
                "AB",
                "AB",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
            ],
            "boundary_coverage_ratio": [
                0.01,
                0.0099,
            ],
            "composite_score_eligible": [
                True,
                True,
            ],
            "composite_risk_score": [
                0.50,
                0.60,
            ],
        }
    )

    result = _score(df)

    assert bool(
        result.loc[
            0,
            "ranking_eligible",
        ]
    )

    assert not bool(
        result.loc[
            1,
            "ranking_eligible",
        ]
    )


def test_rejects_invalid_boundary_coverage():
    df = pd.DataFrame(
        {
            "province_key": ["AB"],
            "reference_month": ["2021-07"],
            "boundary_coverage_ratio": [1.10],
            "composite_score_eligible": [True],
            "composite_risk_score": [0.50],
        }
    )

    with pytest.raises(
        RiskRankingError,
        match="boundary_coverage_ratio",
    ):
        _score(df)


def test_rejects_inconsistent_composite_eligibility():
    df = pd.DataFrame(
        {
            "province_key": ["AB"],
            "reference_month": ["2021-07"],
            "boundary_coverage_ratio": [1.0],
            "composite_score_eligible": [False],
            "composite_risk_score": [0.50],
        }
    )

    with pytest.raises(
        RiskRankingError,
        match="inconsistent",
    ):
        _score(df)


def test_rejects_invalid_priority_tier_order():
    df = pd.DataFrame(
        {
            "province_key": ["AB"],
            "reference_month": ["2021-07"],
            "boundary_coverage_ratio": [1.0],
            "composite_score_eligible": [True],
            "composite_risk_score": [0.50],
        }
    )

    bad_tiers = {
        "very_high": 0.75,
        "high": 0.90,
        "elevated": 0.50,
        "moderate": 0.25,
    }

    with pytest.raises(
        RiskRankingError,
        match="very_high > high",
    ):
        build_ranking_features(
            df,
            group_columns=[
                "province_key",
                "reference_month",
            ],
            minimum_boundary_coverage_ratio=0.01,
            priority_tiers=bad_tiers,
        )