import pandas as pd

from src.silver.validation import (
    validate_municipal_property_assessment_silver_outputs,
)


def write_property_assessment_table(root, dataframe):
    path = root / "silver_property_assessment" / "extract_date=2026-06-01" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_property_assessment.parquet", index=False)


def base_row(key):
    return {
        "property_assessment_key": key,
        "city": "calgary",
        "source_property_id": "150104206",
        "source_parcel_id": "100007",
        "source_unique_key": key.replace("calgary_", ""),
        "assessment_year": 2026,
        "address_text": "15 DEERMEADE PL SE",
        "assessed_value_total": 729000.0,
        "assessment_class_description": "Residential",
        "land_size_sm": 610.1,
        "land_size_sf": 6567.0,
        "land_size_ac": 0.15,
        "geometry_wkt": "MULTIPOLYGON (((-114.0 51.0, -114.1 51.0, -114.0 51.0)))",
        "latitude": 51.0,
        "longitude": -114.0,
        "source_record_count": 1,
    }


def test_validate_municipal_property_assessment_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("calgary_1"),
            base_row("calgary_2"),
        ]
    )

    write_property_assessment_table(silver_root, dataframe)

    report = validate_municipal_property_assessment_silver_outputs(
        silver_root=silver_root,
        expected_assessment_year=2026,
    )

    assert report.passed is True
    assert len(report.checks) == 13


def test_validate_municipal_property_assessment_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1")
    dataframe = pd.DataFrame([row, row])

    write_property_assessment_table(silver_root, dataframe)

    report = validate_municipal_property_assessment_silver_outputs(
        silver_root=silver_root,
        expected_assessment_year=2026,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_assessment_key_not_null_and_unique" in failed_checks


def test_validate_municipal_property_assessment_silver_outputs_fails_negative_value(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1")
    row["assessed_value_total"] = -1.0
    dataframe = pd.DataFrame([row])

    write_property_assessment_table(silver_root, dataframe)

    report = validate_municipal_property_assessment_silver_outputs(
        silver_root=silver_root,
        expected_assessment_year=2026,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_assessment_value_present_and_non_negative" in failed_checks
