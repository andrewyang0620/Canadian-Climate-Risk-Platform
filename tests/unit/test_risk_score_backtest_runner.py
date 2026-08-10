import pandas as pd
from pathlib import Path

from src.backtesting.run_risk_score_backtest import (
    run_risk_score_backtest,
)


def _event_scope() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "disaster_event_reference_key": [
                "event_a",
            ],
            "source_disaster_event_key": [
                "100_AB_2021-07",
            ],
            "reference_month": [
                "2021-07",
            ],
            "province_key": [
                "AB",
            ],
            "disaster_domain": [
                "wildfire",
            ],
            "location_text": [
                "Example fire",
            ],
            "location_tier": [
                "regional",
            ],
            "grid_cell_key": [
                "grid_a",
            ],
            "affected_grid_coverage_ratio": [
                1.0,
            ],
            "is_csd_to_cd_approximation": [
                False,
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
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "disaster_event_occurred": [
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
            ],
            "reference_month": [
                "2021-07",
                "2021-07",
                "2021-07",
            ],
            "province_key": [
                "AB",
                "AB",
                "AB",
            ],
            "boundary_coverage_ratio": [
                1.0,
                1.0,
                1.0,
            ],
            "ranking_eligible": [
                True,
                True,
                True,
            ],
            "composite_risk_score": [
                0.9,
                0.5,
                0.2,
            ],
            "priority_percentile": [
                1.0,
                2.0 / 3.0,
                1.0 / 3.0,
            ],
            "climate_sub_score": [
                0.7,
                0.5,
                0.2,
            ],
            "hydro_sub_score": [
                0.4,
                0.5,
                0.2,
            ],
            "wildfire_sub_score": [
                1.0,
                0.0,
                0.0,
            ],
        }
    )


def _config() -> dict:
    return {
        "composite": {
            "minimum_available_domains": 2,
        },
        "ranking": {
            "minimum_boundary_coverage_ratio": 0.01,
        },
        "backtesting": {
            "top_k_fraction": 0.10,
            "low_overlap_threshold": 0.05,
        },
        "sensitivity": {
            "baseline": {
                "climate": 0.35,
                "hydro": 0.35,
                "wildfire": 0.30,
            },
            "equal": {
                "climate": 1.0 / 3.0,
                "hydro": 1.0 / 3.0,
                "wildfire": 1.0 / 3.0,
            },
        },
    }


def test_runner_writes_backtest_outputs(
    tmp_path,
    monkeypatch,
):
    paths = {
        "gold_disaster_event_grid_scope": (
            tmp_path / "event_scope.parquet"
        ),
        "gold_grid_month_disaster_event_label": (
            tmp_path / "labels.parquet"
        ),
        "gold_grid_month_risk_score": (
            tmp_path / "scores.parquet"
        ),
    }

    _event_scope().to_parquet(
        paths[
            "gold_disaster_event_grid_scope"
        ],
        index=False,
    )

    _labels().to_parquet(
        paths[
            "gold_grid_month_disaster_event_label"
        ],
        index=False,
    )

    _scores().to_parquet(
        paths[
            "gold_grid_month_risk_score"
        ],
        index=False,
    )

    def fake_latest_table_parquet(
        *,
        root,
        table_name,
    ):
        return paths[table_name]

    monkeypatch.setattr(
        "src.backtesting.run_risk_score_backtest.latest_table_parquet",
        fake_latest_table_parquet,
    )

    summary = run_risk_score_backtest(
        gold_root=tmp_path,
        audit_root=tmp_path / "audits",
        config=_config(),
    )

    assert summary[
        "load_status"
    ] == "success"

    assert summary[
        "baseline"
    ][
        "source_event_count"
    ] == 1

    assert summary[
        "baseline"
    ][
        "event_month_count"
    ] == 1

    assert set(
        summary[
            "output_paths"
        ]
    ) == {
        "event_month_metrics",
        "source_event_metrics",
        "domain_event_month_metrics",
        "source_event_domain_metrics",
        "label_sensitivity_metrics",
        "label_domain_sensitivity_metrics",
        "weight_sensitivity_metrics",
        "rank_stability_metrics",
    }

    for path in summary[
        "output_paths"
    ].values():
        assert Path(path).exists()

    assert Path(
        summary[
            "summary_path"
        ]
    ).exists()