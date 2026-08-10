import pandas as pd
import pytest

from src.backtesting.sensitivity import (
    LABEL_QUALITY_SCENARIOS,
    build_label_quality_sensitivity,
    build_rank_stability_metrics,
    build_weight_scenario_scores,
    build_weight_sensitivity,
    summarize_domain_label_quality_sensitivity,
    summarize_label_quality_sensitivity,
    summarize_weight_sensitivity,
)


def _event_scope() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "disaster_event_reference_key": [
                "event_direct",
                "event_direct",
                "event_csd",
                "event_csd",
            ],
            "source_disaster_event_key": [
                "100_AB_2021-07",
                "100_AB_2021-07",
                "200_AB_2021-07",
                "200_AB_2021-07",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "province_key": [
                "AB",
                "AB",
                "AB",
                "AB",
            ],
            "disaster_domain": [
                "wildfire",
                "wildfire",
                "flood",
                "flood",
            ],
            "location_text": [
                "Direct fire",
                "Direct fire",
                "Approximate flood",
                "Approximate flood",
            ],
            "location_tier": [
                "regional",
                "regional",
                "municipal",
                "municipal",
            ],
            "grid_cell_key": [
                "grid_a",
                "grid_b",
                "grid_c",
                "grid_d",
            ],
            "affected_grid_coverage_ratio": [
                1.0,
                0.02,
                1.0,
                1.0,
            ],
            "is_csd_to_cd_approximation": [
                False,
                False,
                True,
                True,
            ],
        }
    )


def _labels() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "grid_cell_key": [
                "grid_a",
                "grid_b",
                "grid_c",
                "grid_d",
                "grid_e",
                "grid_f",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "disaster_event_occurred": [
                True,
                True,
                True,
                True,
                False,
                False,
            ],
        }
    )


def _scores() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "grid_cell_key": [
                "grid_a",
                "grid_b",
                "grid_c",
                "grid_d",
                "grid_e",
                "grid_f",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "province_key": [
                "AB",
                "AB",
                "AB",
                "AB",
                "AB",
                "AB",
            ],
            "boundary_coverage_ratio": [
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ],
            "ranking_eligible": [
                True,
                True,
                True,
                True,
                True,
                True,
            ],
            "composite_risk_score": [
                0.95,
                0.80,
                0.70,
                0.60,
                0.40,
                0.20,
            ],
            "priority_percentile": [
                1.00,
                0.80,
                0.70,
                0.60,
                0.40,
                0.20,
            ],
            "climate_sub_score": [
                0.70,
                0.60,
                0.40,
                0.30,
                0.20,
                0.10,
            ],
            "hydro_sub_score": [
                0.30,
                0.20,
                0.90,
                0.80,
                0.40,
                0.10,
            ],
            "wildfire_sub_score": [
                0.90,
                0.80,
                0.10,
                0.00,
                0.00,
                0.00,
            ],
        }
    )


def test_builds_all_label_quality_scenarios():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    assert set(
        event_month[
            "label_scenario"
        ]
    ) == set(
        LABEL_QUALITY_SCENARIOS
    )


def test_baseline_keeps_all_events():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    baseline = event_month[
        event_month[
            "label_scenario"
        ].eq("baseline")
    ]

    assert set(
        baseline[
            "source_event_id"
        ]
    ) == {
        "100",
        "200",
    }

    assert baseline[
        "affected_grid_count"
    ].sum() == 4


def test_csd_scenario_removes_approximate_event():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    direct = event_month[
        event_month[
            "label_scenario"
        ].eq(
            "exclude_csd_approximation"
        )
    ]

    assert direct[
        "source_event_id"
    ].tolist() == [
        "100"
    ]

    assert direct.iloc[0][
        "affected_grid_count"
    ] == 2


def test_low_overlap_scenario_trims_positive_footprint():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    baseline = event_month[
        event_month[
            "label_scenario"
        ].eq("baseline")
        & event_month[
            "source_event_id"
        ].eq("100")
    ].iloc[0]

    filtered = event_month[
        event_month[
            "label_scenario"
        ].eq(
            "exclude_low_overlap"
        )
        & event_month[
            "source_event_id"
        ].eq("100")
    ].iloc[0]

    assert baseline[
        "affected_grid_count"
    ] == 2

    assert filtered[
        "affected_grid_count"
    ] == 1

    assert filtered[
        "affected_top10_count"
    ] == 1

    assert filtered[
        "event_capture_at_10"
    ] == pytest.approx(
        1.0
    )


def test_removed_positive_grid_does_not_become_control():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    filtered = event_month[
        event_month[
            "label_scenario"
        ].eq(
            "exclude_low_overlap"
        )
        & event_month[
            "source_event_id"
        ].eq("100")
    ].iloc[0]

    assert filtered[
        "control_rankable_count"
    ] == 2


def test_combined_scenario_applies_both_filters():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    event_month = result[
        "event_month_metrics"
    ]

    combined = event_month[
        event_month[
            "label_scenario"
        ].eq(
            "exclude_csd_and_low_overlap"
        )
    ]

    assert combined[
        "source_event_id"
    ].tolist() == [
        "100"
    ]

    assert combined.iloc[0][
        "affected_grid_count"
    ] == 1


def test_label_quality_summary_tracks_event_subset():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    summary = summarize_label_quality_sensitivity(
        result[
            "source_event_metrics"
        ]
    )

    baseline = summary[
        summary[
            "label_scenario"
        ].eq("baseline")
    ].iloc[0]

    direct = summary[
        summary[
            "label_scenario"
        ].eq(
            "exclude_csd_approximation"
        )
    ].iloc[0]

    assert baseline[
        "source_event_count"
    ] == 2

    assert baseline[
        "event_month_count"
    ] == 2

    assert direct[
        "source_event_count"
    ] == 1

    assert direct[
        "event_month_count"
    ] == 1


def test_domain_sensitivity_uses_same_filtered_footprints():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    domain_event_month = result[
        "domain_event_month_metrics"
    ]

    baseline = domain_event_month[
        domain_event_month[
            "label_scenario"
        ].eq("baseline")
    ]

    assert set(
        baseline[
            "domain_score_column"
        ]
    ) == {
        "wildfire_sub_score",
        "hydro_sub_score",
    }

    direct = domain_event_month[
        domain_event_month[
            "label_scenario"
        ].eq(
            "exclude_csd_approximation"
        )
    ]

    assert direct[
        "source_event_id"
    ].tolist() == [
        "100"
    ]

    assert direct.iloc[0][
        "domain_score_column"
    ] == "wildfire_sub_score"


def test_domain_label_quality_summary_groups_scenario_and_domain():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    summary = (
        summarize_domain_label_quality_sensitivity(
            result[
                "source_event_domain_metrics"
            ]
        )
    )

    baseline = summary[
        summary[
            "label_scenario"
        ].eq("baseline")
    ]

    assert set(
        baseline[
            "disaster_domain"
        ]
    ) == {
        "wildfire",
        "flood",
    }

    direct = summary[
        summary[
            "label_scenario"
        ].eq(
            "exclude_csd_approximation"
        )
    ]

    assert direct[
        "source_event_count"
    ].sum() == 1

    assert direct.iloc[0][
        "disaster_domain"
    ] == "wildfire"


def test_sensitivity_returns_all_result_tables():
    result = build_label_quality_sensitivity(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    assert set(
        result
    ) == {
        "event_month_metrics",
        "source_event_metrics",
        "domain_event_month_metrics",
        "source_event_domain_metrics",
    }
    
    
def _weight_scores() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "grid_cell_key": [
                "grid_a",
                "grid_b",
                "grid_c",
                "grid_d",
                "grid_e",
                "grid_f",
            ],
            "reference_month": [
                "2021-07",
            ] * 6,
            "province_key": [
                "AB",
            ] * 6,
            "boundary_coverage_ratio": [
                1.0,
            ] * 6,
            "ranking_eligible": [
                True,
            ] * 6,
            "composite_risk_score": [
                0.34,
                0.31,
                0.63,
                0.56,
                0.40,
                0.30,
            ],
            "priority_percentile": [
                0.50,
                0.3333333333,
                1.00,
                0.8333333333,
                0.6666666667,
                0.1666666667,
            ],
            "climate_sub_score": [
                0.10,
                0.10,
                0.90,
                0.80,
                0.50,
                0.30,
            ],
            "hydro_sub_score": [
                0.10,
                0.10,
                0.90,
                0.80,
                0.50,
                0.30,
            ],
            "wildfire_sub_score": [
                0.90,
                0.80,
                0.00,
                0.00,
                0.20,
                0.20,
            ],
        }
    )


def _weight_event_scope() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "disaster_event_reference_key": [
                "event_fire",
                "event_fire",
            ],
            "source_disaster_event_key": [
                "100_AB_2021-07",
                "100_AB_2021-07",
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
            ],
            "province_key": [
                "AB",
                "AB",
            ],
            "disaster_domain": [
                "wildfire",
                "wildfire",
            ],
            "location_text": [
                "Example fire",
                "Example fire",
            ],
            "location_tier": [
                "regional",
                "regional",
            ],
            "grid_cell_key": [
                "grid_a",
                "grid_b",
            ],
        }
    )


def _weight_labels() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "grid_cell_key": [
                "grid_a",
                "grid_b",
                "grid_c",
                "grid_d",
                "grid_e",
                "grid_f",
            ],
            "reference_month": [
                "2021-07",
            ] * 6,
            "disaster_event_occurred": [
                True,
                True,
                False,
                False,
                False,
                False,
            ],
        }
    )
    
def test_weight_scenario_renormalizes_available_domains():
    scores = _weight_scores()

    scores.loc[
        scores["grid_cell_key"].eq("grid_a"),
        "climate_sub_score",
    ] = None

    result = build_weight_scenario_scores(
        scores,
        domain_weights={
            "climate": 0.35,
            "hydro": 0.35,
            "wildfire": 0.30,
        },
    )

    row = result[
        result["grid_cell_key"].eq("grid_a")
    ].iloc[0]

    expected = (
        0.35 * 0.10
        + 0.30 * 0.90
    ) / 0.65

    assert row[
        "composite_risk_score"
    ] == pytest.approx(expected)


def test_weight_scenario_keeps_two_domain_minimum():
    scores = _weight_scores()

    scores.loc[
        scores["grid_cell_key"].eq("grid_a"),
        [
            "climate_sub_score",
            "hydro_sub_score",
        ],
    ] = None

    result = build_weight_scenario_scores(
        scores,
        domain_weights={
            "climate": 0.35,
            "hydro": 0.35,
            "wildfire": 0.30,
        },
    )

    row = result[
        result["grid_cell_key"].eq("grid_a")
    ].iloc[0]

    assert pd.isna(
        row["composite_risk_score"]
    )

    assert not row[
        "ranking_eligible"
    ]


def test_weight_scenario_rebuilds_province_month_ranking():
    result = build_weight_scenario_scores(
        _weight_scores(),
        domain_weights={
            "climate": 0.10,
            "hydro": 0.10,
            "wildfire": 0.80,
        },
    )

    top_grid = (
        result
        .sort_values(
            "priority_percentile",
            ascending=False,
        )
        .iloc[0]
    )

    assert top_grid[
        "grid_cell_key"
    ] == "grid_a"

    assert top_grid[
        "priority_percentile"
    ] == pytest.approx(1.0)


def test_baseline_rank_stability_is_one():
    scores = _weight_scores()

    baseline = build_weight_scenario_scores(
        scores,
        domain_weights={
            "climate": 0.35,
            "hydro": 0.35,
            "wildfire": 0.30,
        },
    )

    scores[
        "composite_risk_score"
    ] = baseline[
        "composite_risk_score"
    ]

    scores[
        "ranking_eligible"
    ] = baseline[
        "ranking_eligible"
    ]

    scores[
        "priority_percentile"
    ] = baseline[
        "priority_percentile"
    ]

    stability = build_rank_stability_metrics(
        baseline_scores=scores,
        scenario_scores=baseline,
    )

    row = stability.iloc[0]

    assert row[
        "spearman_rank_correlation"
    ] == pytest.approx(1.0)

    assert row[
        "top10_jaccard"
    ] == pytest.approx(1.0)
    

def test_weight_change_can_change_top10_capture():
    result = build_weight_sensitivity(
        event_scope=_weight_event_scope(),
        labels=_weight_labels(),
        scores=_weight_scores(),
        weight_scenarios={
            "baseline": {
                "climate": 0.35,
                "hydro": 0.35,
                "wildfire": 0.30,
            },
            "wildfire_heavy": {
                "climate": 0.10,
                "hydro": 0.10,
                "wildfire": 0.80,
            },
        },
    )

    event_month = result[
        "event_month_metrics"
    ]

    baseline = event_month[
        event_month[
            "weight_scenario"
        ].eq("baseline")
    ].iloc[0]

    wildfire_heavy = event_month[
        event_month[
            "weight_scenario"
        ].eq("wildfire_heavy")
    ].iloc[0]

    assert baseline[
        "event_capture_at_10"
    ] == pytest.approx(0.0)

    assert wildfire_heavy[
        "event_capture_at_10"
    ] == pytest.approx(0.5)


def test_weight_sensitivity_returns_rank_stability():
    result = build_weight_sensitivity(
        event_scope=_weight_event_scope(),
        labels=_weight_labels(),
        scores=_weight_scores(),
        weight_scenarios={
            "baseline": {
                "climate": 0.35,
                "hydro": 0.35,
                "wildfire": 0.30,
            },
            "wildfire_heavy": {
                "climate": 0.10,
                "hydro": 0.10,
                "wildfire": 0.80,
            },
        },
    )

    stability = result[
        "rank_stability_metrics"
    ]

    assert set(
        stability[
            "weight_scenario"
        ]
    ) == {
        "baseline",
        "wildfire_heavy",
    }

    baseline = stability[
        stability[
            "weight_scenario"
        ].eq("baseline")
    ].iloc[0]

    assert baseline[
        "spearman_rank_correlation"
    ] == pytest.approx(1.0)

    assert baseline[
        "top10_jaccard"
    ] == pytest.approx(1.0)


def test_weight_sensitivity_summary_combines_event_and_rank_metrics():
    result = build_weight_sensitivity(
        event_scope=_weight_event_scope(),
        labels=_weight_labels(),
        scores=_weight_scores(),
        weight_scenarios={
            "baseline": {
                "climate": 0.35,
                "hydro": 0.35,
                "wildfire": 0.30,
            },
            "wildfire_heavy": {
                "climate": 0.10,
                "hydro": 0.10,
                "wildfire": 0.80,
            },
        },
    )

    summary = summarize_weight_sensitivity(
        source_event_metrics=result[
            "source_event_metrics"
        ],
        rank_stability_metrics=result[
            "rank_stability_metrics"
        ],
    )

    assert set(
        summary[
            "weight_scenario"
        ]
    ) == {
        "baseline",
        "wildfire_heavy",
    }

    assert set(
        [
            "mean_event_capture_at_10",
            "mean_event_auc",
            "mean_spearman_rank_correlation",
            "mean_top10_jaccard",
        ]
    ).issubset(
        summary.columns
    )