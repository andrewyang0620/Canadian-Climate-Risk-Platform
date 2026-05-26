import pandas as pd

from src.silver.validation import validate_wildfire_history_silver_outputs


def write_wildfire_table(root, dataframe):
    path = root / "silver_wildfire_event" / "extract_date=2026-05-24" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_wildfire_event.parquet", index=False)


def base_row(event_key, province, year=2020):
    return {
        "wildfire_event_key": event_key,
        "source_event_id": event_key,
        "agency": province,
        "province": province,
        "province_inference_method": "source_agency",
        "fire_year": year,
        "report_date": "2020-07-01",
        "fire_size_ha": 10.0,
        "latitude": 50.0,
        "longitude": -120.0,
        "geometry_wkt": "POINT (-120 50)",
        "source_record_count": 1,
    }


def test_validate_wildfire_history_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("BC_1", "BC", 2020),
            base_row("AB_1", "AB", 2021),
        ]
    )

    write_wildfire_table(silver_root, dataframe)

    report = validate_wildfire_history_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 9


def test_validate_wildfire_history_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("BC_1", "BC", 2020)
    dataframe = pd.DataFrame([row, row])

    write_wildfire_table(silver_root, dataframe)

    report = validate_wildfire_history_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "wildfire_event_key_not_null_and_unique" in failed_checks


def test_validate_wildfire_history_silver_outputs_fails_bad_year(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("BC_1", "BC", 2020),
            base_row("AB_1", "AB", -999),
        ]
    )

    write_wildfire_table(silver_root, dataframe)

    report = validate_wildfire_history_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "wildfire_non_null_fire_years_in_expected_range" in failed_checks
