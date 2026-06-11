import pandas as pd

from src.silver.validation import validate_municipal_building_permit_silver_outputs


def write_building_permit_table(root, dataframe):
    path = root / "silver_building_permit" / "extract_date=2026-06-01" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_building_permit.parquet", index=False)


def base_row(key, city, source_name):
    latitude = 51.0 if city == "calgary" else 49.25
    longitude = -114.0 if city == "calgary" else -123.1

    return {
        "building_permit_key": key,
        "city": city,
        "source_name": source_name,
        "source_permit_id": key,
        "permit_number": key,
        "issue_year": 2026,
        "issue_date": "2026-01-01",
        "address_text": "1 TEST ST",
        "estimated_project_cost": 1000.0,
        "latitude": latitude,
        "longitude": longitude,
        "geometry_wkt": f"POINT ({longitude} {latitude})",
        "source_record_count": 1,
    }


def test_validate_municipal_building_permit_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row(
                "calgary_1",
                "calgary",
                "calgary_building_permits",
            ),
            base_row(
                "vancouver_1",
                "vancouver",
                "vancouver_building_permits",
            ),
        ]
    )

    write_building_permit_table(silver_root, dataframe)

    report = validate_municipal_building_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 13


def test_validate_municipal_building_permit_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1", "calgary", "calgary_building_permits")
    dataframe = pd.DataFrame([row, row])

    write_building_permit_table(silver_root, dataframe)

    report = validate_municipal_building_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "building_permit_key_not_null_and_unique" in failed_checks


def test_validate_municipal_building_permit_silver_outputs_fails_negative_cost(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1", "calgary", "calgary_building_permits")
    row["estimated_project_cost"] = -1.0
    dataframe = pd.DataFrame(
        [
            row,
            base_row("vancouver_1", "vancouver", "vancouver_building_permits"),
        ]
    )

    write_building_permit_table(silver_root, dataframe)

    report = validate_municipal_building_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "building_permit_estimated_cost_non_negative" in failed_checks
