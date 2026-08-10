import pandas as pd
import pytest

from src.backtesting.risk_score import (
    build_event_month_metrics,
    build_source_event_metrics,
    summarize_source_events,
)


def _event_scope() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "disaster_event_reference_key": [
                "event_a",
                "event_a",
            ],
            "source_disaster_event_key": [
                "1135_AB_2021-07",
                "1135_AB_2021-07",
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
                "municipal",
                "municipal",
            ],
            "grid_cell_key": [
                "grid_a",
                "grid_c",
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
            ] * 6,
            "disaster_event_occurred": [
                True,
                True,
                True,
                False,
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
            ] * 6,
            "province_key": [
                "AB",
            ] * 6,
            "ranking_eligible": [
                True,
            ] * 6,
            "composite_risk_score": [
                0.90,
                0.85,
                0.70,
                0.60,
                0.40,
                0.20,
            ],
            "priority_percentile": [
                1.00,
                0.85,
                0.70,
                0.60,
                0.40,
                0.20,
            ],
        }
    )


def test_event_month_metrics_use_clean_controls():
    result = build_event_month_metrics(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
        top_k_fraction=0.10,
    )

    row = result.iloc[0]

    assert row["affected_grid_count"] == 2
    assert row["affected_rankable_count"] == 2

    # grid_b is another positive event grid,
    # so only d/e/f are controls.
    assert row["control_rankable_count"] == 3


def test_event_month_capture_and_lift():
    result = build_event_month_metrics(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
        top_k_fraction=0.10,
    )

    row = result.iloc[0]

    assert row["affected_top10_count"] == 1
    assert row["event_capture_at_10"] == pytest.approx(
        0.5
    )

    assert row["capture_lift_at_10"] == pytest.approx(
        5.0
    )


def test_event_month_auc_uses_affected_vs_controls():
    result = build_event_month_metrics(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    row = result.iloc[0]

    assert row["event_auc"] == pytest.approx(
        1.0
    )

    assert row["score_gap"] > 0


def test_source_event_id_removes_month_suffix():
    result = build_event_month_metrics(
        event_scope=_event_scope(),
        labels=_labels(),
        scores=_scores(),
    )

    assert result.loc[
        0,
        "source_event_id",
    ] == "1135"


def test_source_event_aggregation_treats_months_as_one_event():
    event_month_metrics = pd.DataFrame(
        {
            "source_event_id": [
                "1135",
                "1135",
                "2000",
            ],
            "reference_month": [
                "2021-06",
                "2021-07",
                "2021-08",
            ],
            "province_key": [
                "AB",
                "AB",
                "BC",
            ],
            "disaster_domain": [
                "wildfire",
                "wildfire",
                "flood",
            ],
            "location_text": [
                "Example fire",
                "Example fire",
                "Example flood",
            ],
            "location_tier": [
                "municipal",
                "municipal",
                "regional",
            ],
            "affected_rankable_rate": [
                1.0,
                0.9,
                1.0,
            ],
            "event_capture_at_10": [
                0.4,
                0.2,
                0.3,
            ],
            "capture_lift_at_10": [
                4.0,
                2.0,
                3.0,
            ],
            "precision_at_10": [
                0.2,
                0.1,
                0.15,
            ],
            "event_auc": [
                0.8,
                0.6,
                0.7,
            ],
            "score_gap": [
                0.2,
                0.1,
                0.15,
            ],
            "mean_affected_priority_percentile": [
                0.7,
                0.5,
                0.6,
            ],
        }
    )

    result = build_source_event_metrics(
        event_month_metrics
    )

    source_event = result[
        result["source_event_id"].eq(
            "1135"
        )
    ].iloc[0]

    assert source_event[
        "event_month_count"
    ] == 2

    assert source_event[
        "mean_event_capture_at_10"
    ] == pytest.approx(0.3)

    assert source_event[
        "mean_event_auc"
    ] == pytest.approx(0.7)


def test_source_event_summary_counts_independent_events():
    event_month_metrics = pd.DataFrame(
        {
            "source_event_id": [
                "1135",
                "1135",
                "2000",
            ],
            "reference_month": [
                "2021-06",
                "2021-07",
                "2021-08",
            ],
            "province_key": [
                "AB",
                "AB",
                "BC",
            ],
            "disaster_domain": [
                "wildfire",
                "wildfire",
                "flood",
            ],
            "location_text": [
                "Example fire",
                "Example fire",
                "Example flood",
            ],
            "location_tier": [
                "municipal",
                "municipal",
                "regional",
            ],
            "affected_rankable_rate": [
                1.0,
                1.0,
                1.0,
            ],
            "event_capture_at_10": [
                0.4,
                0.2,
                0.3,
            ],
            "capture_lift_at_10": [
                4.0,
                2.0,
                3.0,
            ],
            "precision_at_10": [
                0.2,
                0.1,
                0.15,
            ],
            "event_auc": [
                0.8,
                0.6,
                0.7,
            ],
            "score_gap": [
                0.2,
                0.1,
                0.15,
            ],
            "mean_affected_priority_percentile": [
                0.7,
                0.5,
                0.6,
            ],
        }
    )

    source_events = build_source_event_metrics(
        event_month_metrics
    )

    summary = summarize_source_events(
        source_events
    )

    assert summary["source_event_count"] == 2
    assert summary["event_month_count"] == 3
    assert summary[
        "source_event_counts_by_domain"
    ] == {
        "wildfire": 1,
        "flood": 1,
    }