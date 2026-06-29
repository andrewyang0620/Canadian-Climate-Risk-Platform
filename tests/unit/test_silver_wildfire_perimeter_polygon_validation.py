from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.silver.validate_wildfire_perimeter_polygon import (
    validate_wildfire_perimeter_polygon_silver_outputs,
)


def write_table(
    root: Path,
    *,
    extract_date: str,
    run_id: str,
    dataframe: pd.DataFrame,
) -> None:
    path = (
        root
        / "silver_wildfire_perimeter_polygon"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_wildfire_perimeter_polygon.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)


def valid_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "wildfire_perimeter_key": "nfdb_poly__BC-2016-N10037__NFDB_poly_1972to2020_20250630__record_1",
                "cfs_ref_id": "BC-2016-N10037",
                "source_fire_id": "2016-N10037",
                "source_key": "",
                "source_agency": "BC",
                "province": "BC",
                "fire_name": "TEST FIRE",
                "fire_year": 2016,
                "fire_month": 5,
                "fire_day": 13,
                "report_date": "2016-05-13",
                "out_date": None,
                "polygon_date": "2016-05-14",
                "acquired_date": "2025-06-30",
                "date_type": "REPORT",
                "decade": "2010",
                "source_size_ha": 3.5,
                "calculated_size_ha": 3.6,
                "fire_cause": "N",
                "prescribed": "N",
                "map_source": "unit_test",
                "map_method": "digitized",
                "water_removed": "N",
                "unburned_removed": "N",
                "more_info": "",
                "cfs_note1": "",
                "cfs_note2": "",
                "agency_source_file": "unit_test.shp",
                "geometry_type": "Polygon",
                "geometry_wkt": "POLYGON ((0 0, 0.1 0, 0.1 0.1, 0 0.1, 0 0))",
                "geometry_original_is_valid": True,
                "geometry_was_repaired": False,
                "geometry_is_valid": True,
                "source_crs": "NAD_1983_Lambert_Conformal_Conic",
                "source_name": "wildfire_perimeter_polygons",
                "source_layer": "NFDB_poly",
                "source_file": "NFDB_poly_1972to2020_20250630.shp",
                "source_record_number": 1,
            },
            {
                "wildfire_perimeter_key": "nfdb_poly__AB-2023-GWF018__NFDB_poly_2021to2024_20250630__record_1",
                "cfs_ref_id": "AB-2023-GWF018",
                "source_fire_id": "GWF018",
                "source_key": "",
                "source_agency": "AB",
                "province": "AB",
                "fire_name": "TEST FIRE 2",
                "fire_year": 2023,
                "fire_month": 8,
                "fire_day": 1,
                "report_date": "2023-08-01",
                "out_date": None,
                "polygon_date": "2023-08-02",
                "acquired_date": "2025-06-30",
                "date_type": "REPORT",
                "decade": "2020",
                "source_size_ha": 49362.8,
                "calculated_size_ha": 49380.882141,
                "fire_cause": "N",
                "prescribed": "N",
                "map_source": "unit_test",
                "map_method": "digitized",
                "water_removed": "N",
                "unburned_removed": "N",
                "more_info": "",
                "cfs_note1": "",
                "cfs_note2": "",
                "agency_source_file": "unit_test.shp",
                "geometry_type": "Polygon",
                "geometry_wkt": "POLYGON ((1 0, 1.1 0, 1.1 0.1, 1 0.1, 1 0))",
                "geometry_original_is_valid": False,
                "geometry_was_repaired": True,
                "geometry_is_valid": True,
                "source_crs": "NAD_1983_Lambert_Conformal_Conic",
                "source_name": "wildfire_perimeter_polygons",
                "source_layer": "NFDB_poly",
                "source_file": "NFDB_poly_2021to2024_20250630.shp",
                "source_record_number": 1,
            },
        ]
    )


def test_validate_wildfire_perimeter_polygon_silver_outputs_passes(tmp_path: Path):
    write_table(
        tmp_path,
        extract_date="2026-06-28",
        run_id="test-run",
        dataframe=valid_dataframe(),
    )

    output_json = tmp_path / "_validation" / "wildfire_perimeter_polygon" / "latest_validation.json"

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=output_json,
    )

    assert report.passed
    assert output_json.exists()
    assert report.summary["row_count"] == 2
    assert report.summary["key_unique_count"] == 2
    assert report.summary["province_values"] == ["AB", "BC"]
    assert report.summary["fire_year_min"] == 2016
    assert report.summary["fire_year_max"] == 2023
    assert report.summary["geometry_repaired_count"] == 1


def test_validate_wildfire_perimeter_polygon_silver_outputs_fails_duplicate_key(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[1, "wildfire_perimeter_key"] = dataframe.loc[0, "wildfire_perimeter_key"]

    write_table(
        tmp_path,
        extract_date="2026-06-28",
        run_id="test-run",
        dataframe=dataframe,
    )

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_perimeter_key_quality" in failed
    assert failed["wildfire_perimeter_key_quality"].details["duplicate_key_count"] == 1


def test_validate_wildfire_perimeter_polygon_silver_outputs_fails_invalid_province(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[0, "province"] = "ON"

    write_table(
        tmp_path,
        extract_date="2026-06-28",
        run_id="test-run",
        dataframe=dataframe,
    )

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_perimeter_bc_ab_filter" in failed


def test_validate_wildfire_perimeter_polygon_silver_outputs_fails_null_year(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[1, "fire_year"] = None

    write_table(
        tmp_path,
        extract_date="2026-06-28",
        run_id="test-run",
        dataframe=dataframe,
    )

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_perimeter_year_presence" in failed


def test_validate_wildfire_perimeter_polygon_silver_outputs_fails_invalid_geometry(
    tmp_path: Path,
):
    dataframe = valid_dataframe()
    dataframe.loc[0, "geometry_is_valid"] = False

    write_table(
        tmp_path,
        extract_date="2026-06-28",
        run_id="test-run",
        dataframe=dataframe,
    )

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=None,
    )

    assert not report.passed
    failed = {check.name: check for check in report.checks if not check.passed}
    assert "wildfire_perimeter_geometry_quality" in failed
    assert failed["wildfire_perimeter_geometry_quality"].details["invalid_geometry_count"] == 1
