import json

import pandas as pd

from src.scoring import run_risk_score


def test_runner_writes_score_and_metadata(
    tmp_path,
    monkeypatch,
):
    gold_root = tmp_path / "gold"

    input_path = (
        gold_root
        / "gold_grid_month_risk_feature_mart"
        / "extract_date=2026-08-08"
        / "run_id=input-run"
        / "gold_grid_month_risk_feature_mart.parquet"
    )

    input_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pd.DataFrame(
        {
            "placeholder": [1],
        }
    ).to_parquet(
        input_path,
        index=False,
    )

    expected_score = pd.DataFrame(
        {
            "risk_score_key": [
                "grid_a__2021-07"
            ],
        }
    )

    expected_summary = {
        "row_count": 1,
        "grid_cell_count": 1,
        "month_count": 1,
        "minimum_month": "2021-07",
        "maximum_month": "2021-07",
        "composite_score_eligible_count": 1,
        "ranking_eligible_count": 1,
        "composite_score_null_count": 0,
        "minimum_composite_score": 0.5,
        "maximum_composite_score": 0.5,
        "domain_coverage_counts": {
            "3": 1,
        },
        "priority_tier_counts": {
            "high": 1,
        },
        "ranking_exclusion_reason_counts": {
            "none": 1,
        },
    }

    def fake_builder(dataframe):
        assert len(dataframe) == 1
        return (
            expected_score,
            expected_summary,
        )

    monkeypatch.setattr(
        run_risk_score,
        "build_gold_grid_month_risk_score",
        fake_builder,
    )

    metadata = (
        run_risk_score
        .run_gold_grid_month_risk_score(
            gold_root=gold_root,
            extract_date="2026-08-09",
            run_id="score-run",
        )
    )

    output_path = (
        gold_root
        / "gold_grid_month_risk_score"
        / "extract_date=2026-08-09"
        / "run_id=score-run"
        / "gold_grid_month_risk_score.parquet"
    )

    metadata_path = (
        gold_root
        / "_metadata"
        / "gold_grid_month_risk_score"
        / "extract_date=2026-08-09"
        / "run_id=score-run"
        / "metadata.json"
    )

    assert output_path.exists()
    assert metadata_path.exists()

    written = pd.read_parquet(
        output_path
    )

    assert written.equals(
        expected_score
    )

    stored_metadata = json.loads(
        metadata_path.read_text(
            encoding="utf-8"
        )
    )

    assert stored_metadata[
        "table_name"
    ] == "gold_grid_month_risk_score"

    assert stored_metadata[
        "input_table"
    ] == "gold_grid_month_risk_feature_mart"

    assert stored_metadata[
        "row_count"
    ] == 1

    assert metadata[
        "metadata_path"
    ] == metadata_path.as_posix()