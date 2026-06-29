from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.gold.wildfire.validate_perimeter_grid_features import (
    validate_gold_wildfire_perimeter_grid_features,
)


def write_gold_table(
    root: Path,
    dataframe: pd.DataFrame,
) -> None:
    path = (
        root
        / "gold_grid_month_wildfire_perimeter_feature"
        / "extract_date=2026-06-29"
        / "run_id=test-run"
        / "gold_grid_month_wildfire_perimeter_feature.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)


def valid_dataframe() -> pd.DataFrame:
    rows = []

    for month in ["2016-01", "2016-02"]:
        rows.append(
            {
                "wildfire_grid_month_key": f"ab_10km_test__{month}",
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "reference_month": month,
                "crs_epsg": 3347,
                "grid_analysis_area_sq_km": 100.0,
                "wildfire_perimeter_count": 0,
                "wildfire_intersection_area_sq_km": 0.0,
                "wildfire_intersection_area_ha": 0.0,
                "wildfire_intersection_area_ratio_of_grid": 0.0,
                "wildfire_max_source_size_ha": 0.0,
                "wildfire_max_calculated_size_ha": 0.0,
                "wildfire_cause_n_polygon_count": 0,
                "wildfire_cause_h_polygon_count": 0,
                "wildfire_cause_u_polygon_count": 0,
                "wildfire_cause_prescribed_burn_polygon_count": 0,
                "wildfire_cause_other_polygon_count": 0,
                "wildfire_has_observed_perimeter_overlap": False,
                "wildfire_temporal_assignment_method": "no_observed_perimeter_overlap",
            }
        )

    rows[1]["wildfire_perimeter_count"] = 1
    rows[1]["wildfire_intersection_area_sq_km"] = 1.0
    rows[1]["wildfire_intersection_area_ha"] = 100.0
    rows[1]["wildfire_intersection_area_ratio_of_grid"] = 0.01
    rows[1]["wildfire_max_source_size_ha"] = 100.0
    rows[1]["wildfire_max_calculated_size_ha"] = 100.0
    rows[1]["wildfire_cause_n_polygon_count"] = 1
    rows[1]["wildfire_has_observed_perimeter_overlap"] = True
    rows[1]["wildfire_temporal_assignment_method"] = "polygon_fire_month"

    return pd.DataFrame(rows)


def test_validate_gold_wildfire_perimeter_grid_features_passes(tmp_path: Path):
    dataframe = valid_dataframe()
    write_gold_table(tmp_path, dataframe)

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=tmp_path,
        expected_grid_cell_count=1,
        expected_month_count=2,
        expected_reference_month_start="2016-01",
        expected_reference_month_end="2016-02",
        output_json_path=None,
    )

    assert report.passed
    assert report.summary["row_count"] == 2
    assert report.summary["grid_cell_count"] == 1
    assert report.summary["month_count"] == 2
    assert report.summary["nonzero_grid_month_count"] == 1


def test_validate_gold_wildfire_perimeter_grid_features_fails_duplicate_key(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[1, "wildfire_grid_month_key"] = dataframe.loc[0, "wildfire_grid_month_key"]
    write_gold_table(tmp_path, dataframe)

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=tmp_path,
        expected_grid_cell_count=1,
        expected_month_count=2,
        expected_reference_month_start="2016-01",
        expected_reference_month_end="2016-02",
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_gold_key_quality" in failed


def test_validate_gold_wildfire_perimeter_grid_features_fails_forbidden_total_fields(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe["wildfire_total_source_size_ha"] = 100.0
    write_gold_table(tmp_path, dataframe)

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=tmp_path,
        expected_grid_cell_count=1,
        expected_month_count=2,
        expected_reference_month_start="2016-01",
        expected_reference_month_end="2016-02",
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_gold_required_columns" in failed
    assert (
        "wildfire_total_source_size_ha"
        in failed["wildfire_gold_required_columns"].details["unexpected_forbidden_columns"]
    )


def test_validate_gold_wildfire_perimeter_grid_features_fails_cause_mismatch(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[1, "wildfire_cause_n_polygon_count"] = 0
    write_gold_table(tmp_path, dataframe)

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=tmp_path,
        expected_grid_cell_count=1,
        expected_month_count=2,
        expected_reference_month_start="2016-01",
        expected_reference_month_end="2016-02",
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_gold_cause_count_consistency" in failed


def test_validate_gold_wildfire_perimeter_grid_features_fails_zero_semantics(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[0, "wildfire_intersection_area_ha"] = 1.0
    write_gold_table(tmp_path, dataframe)

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=tmp_path,
        expected_grid_cell_count=1,
        expected_month_count=2,
        expected_reference_month_start="2016-01",
        expected_reference_month_end="2016-02",
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_gold_zero_semantics" in failed
