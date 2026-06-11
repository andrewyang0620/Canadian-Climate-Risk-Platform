import pandas as pd

from src.silver.validation import (
    validate_municipal_property_tax_assessment_silver_outputs,
)


def write_tax_table(root, dataframe):
    path = root / "silver_property_tax_assessment" / "extract_date=2026-06-09" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_property_tax_assessment.parquet", index=False)


def write_parcel_table(root):
    path = root / "silver_property_parcel" / "extract_date=2026-06-09" / "run_id=test"
    path.mkdir(parents=True)

    dataframe = pd.DataFrame(
        [
            {
                "property_parcel_key": "vancouver_parcel_1",
                "city": "vancouver",
                "province": "BC",
                "source_name": "vancouver_property_parcels",
                "source_tax_coord": "12345678",
            },
            {
                "property_parcel_key": "vancouver_parcel_2",
                "city": "vancouver",
                "province": "BC",
                "source_name": "vancouver_property_parcels",
                "source_tax_coord": "87654321",
            },
        ]
    )

    dataframe.to_parquet(path / "silver_property_parcel.parquet", index=False)


def base_row(key, row_number, land_coordinate="12345678"):
    return {
        "property_tax_assessment_key": key,
        "city": "vancouver",
        "province": "BC",
        "source_name": "vancouver_property_tax",
        "source_pid": "001-001-001",
        "source_folio": "123",
        "source_land_coordinate": land_coordinate,
        "current_land_value": 1000.0,
        "current_improvement_value": 500.0,
        "current_total_assessed_value": 1500.0,
        "previous_land_value": 900.0,
        "previous_improvement_value": 400.0,
        "previous_total_assessed_value": 1300.0,
        "tax_levy": 10.0,
        "tax_assessment_year": 2026,
        "report_year": 2026,
        "source_row_number": row_number,
        "source_record_count": 1,
    }


def test_validate_municipal_property_tax_assessment_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("vancouver_1", 1, "12345678"),
            base_row("vancouver_2", 2, "87654321"),
        ]
    )

    write_tax_table(silver_root, dataframe)
    write_parcel_table(silver_root)

    report = validate_municipal_property_tax_assessment_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 15


def test_validate_municipal_property_tax_assessment_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("vancouver_1", 1, "12345678"),
            base_row("vancouver_1", 2, "87654321"),
        ]
    )

    write_tax_table(silver_root, dataframe)
    write_parcel_table(silver_root)

    report = validate_municipal_property_tax_assessment_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_tax_assessment_key_not_null_and_unique" in failed_checks


def test_validate_municipal_property_tax_assessment_silver_outputs_fails_negative_value(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("vancouver_1", 1, "12345678")
    row["tax_levy"] = -1.0

    dataframe = pd.DataFrame([row, base_row("vancouver_2", 2, "87654321")])

    write_tax_table(silver_root, dataframe)
    write_parcel_table(silver_root)

    report = validate_municipal_property_tax_assessment_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_tax_assessment_values_non_negative" in failed_checks
