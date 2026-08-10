from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.backtesting.risk_score import (
    build_domain_event_month_metrics,
    build_event_month_metrics,
    build_source_event_domain_metrics,
    build_source_event_metrics,
    summarize_domain_source_events,
    summarize_source_events,
)
from src.backtesting.sensitivity import (
    build_label_quality_sensitivity,
    build_weight_sensitivity,
    summarize_domain_label_quality_sensitivity,
    summarize_label_quality_sensitivity,
    summarize_weight_sensitivity,
)
from src.gold.common.io import latest_table_parquet
from src.scoring.builder import load_risk_scoring_config


GOLD_ROOT = Path("lakehouse/gold")
AUDIT_ROOT = Path("lakehouse/audits/risk_score_backtest")

EVENT_SCOPE_TABLE = "gold_disaster_event_grid_scope"
LABEL_TABLE = "gold_grid_month_disaster_event_label"
SCORE_TABLE = "gold_grid_month_risk_score"


def run_risk_score_backtest(
    *,
    gold_root: Path = GOLD_ROOT,
    audit_root: Path = AUDIT_ROOT,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Run the following:
    
    - `build_event_month_metrics()` `build_source_event_metrics()` for testing risk score on disasters
    - `build_domain_event_month_metrics()` `build_source_event_domain_metrics()` for testing the sub-score performance on certain type of disasters
    - `build_label_quality_sensitivity()` + 2 `summarization()` for testing baseline backtesting when get rid of CSD approximation, low over lap
    - `build_weight_sensitivity()` for testing risk score stability when changing the weight distribution
    - `build_rank_stability_metrics()` for testing ranking stability with Spearman and top10 Jaccard, when changing the weights

    The outputs are:
    
    - event_month_metrics.parquet
    - source_event_metrics.parquet
    - domain_event_month_metrics.parquet
    - source_event_domain_metrics.parquet
    - label_sensitivity_metrics.parquet
    - label_domain_sentivity_metrics.parquet
    - weight_sensitivity_metrics.parquet
    - rank_stability_metrics.parquet
    """
    scoring_config = (
        load_risk_scoring_config()
        if config is None
        else config
    )

    backtesting_config = scoring_config["backtesting"]

    input_paths = {
        EVENT_SCOPE_TABLE: latest_table_parquet(
            root=gold_root,
            table_name=EVENT_SCOPE_TABLE,
        ),
        LABEL_TABLE: latest_table_parquet(
            root=gold_root,
            table_name=LABEL_TABLE,
        ),
        SCORE_TABLE: latest_table_parquet(
            root=gold_root,
            table_name=SCORE_TABLE,
        ),
    }

    event_scope = pd.read_parquet(
        input_paths[EVENT_SCOPE_TABLE],
        columns=[
            "disaster_event_reference_key",
            "source_disaster_event_key",
            "reference_month",
            "province_key",
            "disaster_domain",
            "location_text",
            "location_tier",
            "grid_cell_key",
            "affected_grid_coverage_ratio",
            "is_csd_to_cd_approximation",
        ],
    )

    labels = pd.read_parquet(
        input_paths[LABEL_TABLE],
        columns=[
            "grid_cell_key",
            "reference_month",
            "disaster_event_occurred",
        ],
    )

    scores = pd.read_parquet(
        input_paths[SCORE_TABLE],
        columns=[
            "grid_cell_key",
            "reference_month",
            "province_key",
            "boundary_coverage_ratio",
            "ranking_eligible",
            "composite_risk_score",
            "priority_percentile",
            "climate_sub_score",
            "hydro_sub_score",
            "wildfire_sub_score",
        ],
    )

    top_k_fraction = float(backtesting_config["top_k_fraction"])

    low_overlap_threshold = float(backtesting_config["low_overlap_threshold"])

    minimum_available_domains = int(
        scoring_config["composite"][
            "minimum_available_domains"
        ]
    )

    minimum_boundary_coverage_ratio = float(
        scoring_config["ranking"]["minimum_boundary_coverage_ratio"]
    )

    event_month_metrics = build_event_month_metrics(
        event_scope=event_scope,
        labels=labels,
        scores=scores,
        top_k_fraction=top_k_fraction,
    )

    source_event_metrics = build_source_event_metrics(
        event_month_metrics
    )

    domain_event_month_metrics = (
        build_domain_event_month_metrics(
            event_scope=event_scope,
            labels=labels,
            scores=scores,
            minimum_boundary_coverage_ratio=(
                minimum_boundary_coverage_ratio
            ),
        )
    )

    source_event_domain_metrics = (
        build_source_event_domain_metrics(
            domain_event_month_metrics
        )
    )

    label_sensitivity = (
        build_label_quality_sensitivity(
            event_scope=event_scope,
            labels=labels,
            scores=scores,
            top_k_fraction=top_k_fraction,
            minimum_boundary_coverage_ratio=(
                minimum_boundary_coverage_ratio
            ),
            low_overlap_threshold=(
                low_overlap_threshold
            ),
        )
    )

    weight_sensitivity = build_weight_sensitivity(
        event_scope=event_scope,
        labels=labels,
        scores=scores,
        weight_scenarios=scoring_config[
            "sensitivity"
        ],
        top_k_fraction=top_k_fraction,
        minimum_available_domains=(
            minimum_available_domains
        ),
        minimum_boundary_coverage_ratio=(
            minimum_boundary_coverage_ratio
        ),
    )

    baseline_summary = summarize_source_events(
        source_event_metrics
    )

    domain_summary = summarize_domain_source_events(
        source_event_domain_metrics
    )

    label_summary = (
        summarize_label_quality_sensitivity(
            label_sensitivity[
                "source_event_metrics"
            ]
        )
    )

    label_domain_summary = (
        summarize_domain_label_quality_sensitivity(
            label_sensitivity[
                "source_event_domain_metrics"
            ]
        )
    )

    weight_summary = summarize_weight_sensitivity(
        source_event_metrics=weight_sensitivity[
            "source_event_metrics"
        ],
        rank_stability_metrics=weight_sensitivity[
            "rank_stability_metrics"
        ],
    )

    run_id = str(uuid4())
    extract_date = date.today().isoformat()

    output_dir = (
        audit_root
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    outputs = {
        "event_month_metrics": (
            event_month_metrics
        ),
        "source_event_metrics": (
            source_event_metrics
        ),
        "domain_event_month_metrics": (
            domain_event_month_metrics
        ),
        "source_event_domain_metrics": (
            source_event_domain_metrics
        ),
        "label_sensitivity_metrics": (
            label_sensitivity[
                "source_event_metrics"
            ]
        ),
        "label_domain_sensitivity_metrics": (
            label_sensitivity[
                "source_event_domain_metrics"
            ]
        ),
        "weight_sensitivity_metrics": (
            weight_sensitivity[
                "source_event_metrics"
            ]
        ),
        "rank_stability_metrics": (
            weight_sensitivity[
                "rank_stability_metrics"
            ]
        ),
    }

    output_paths = {}

    for name, dataframe in outputs.items():
        path = output_dir / f"{name}.parquet"

        dataframe.to_parquet(
            path,
            index=False,
        )

        output_paths[name] = (
            path.as_posix()
        )

    summary = {
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "baseline": baseline_summary,
        "domain": domain_summary,
        "label_quality_sensitivity": (
            _records(label_summary)
        ),
        "domain_label_quality_sensitivity": (
            _records(label_domain_summary)
        ),
        "weight_sensitivity": (
            _records(weight_summary)
        ),
        "input_paths": {
            table: path.as_posix()
            for table, path
            in input_paths.items()
        },
        "output_paths": output_paths,
    }

    summary_path = (
        output_dir / "summary.json"
    )

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    summary["summary_path"] = (
        summary_path.as_posix()
    )

    return summary


def _records(dataframe: pd.DataFrame,) -> list[dict[str, Any]]:
    # turning DF into list[dict]
    return json.loads(
        dataframe.to_json(
            orient="records"
        )
    )


def main() -> None:
    summary = run_risk_score_backtest()
    baseline = summary["baseline"]
    print(
        "[OK] risk score backtest completed | "
        f"source_events={baseline['source_event_count']} "
        f"event_months={baseline['event_month_count']} "
        f"mean_capture={baseline['mean_event_capture_at_10']:.3f} "
        f"mean_lift={baseline['mean_capture_lift_at_10']:.3f} "
        f"mean_auc={baseline['mean_event_auc']:.3f}"
    )

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()