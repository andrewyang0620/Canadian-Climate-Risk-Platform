import pandas as pd

from src.silver.validation import validate_municipal_development_permit_silver_outputs


def write_development_permit_table(root, dataframe):
    path = root / "silver_development_permit" / "extract_date=2026-06-09" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_development_permit.parquet", index=False)


def base_row(key):
    return {
        "development_permit_key": key,
        "city": "calgary",
        "province": "AB",
        "source_name": "calgary_development_permits",
        "source_permit_id": key.replace("calgary_", ""),
        "applied_date": pd.Timestamp("2026-01-01"),
        "applied_year": 2026,
        "decision_date": pd.Timestamp("2026-02-01"),
        "decision_year": 2026,
        "address_text": "100 TEST ST NW",
        "status_current": "Released",
        "permitted_discretionary": "Permitted",
        "land_use_district": "R-G",
        "proposed_use_code": "C2626",
        "proposed_use_description": "SECONDARY SUITE",
        "community_code": "TAR",
        "community_name": "TARADALE",
        "latitude": 51.0,
        "longitude": -114.0,
        "geometry_wkt": "POINT (-114 51)",
        "source_record_count": 1,
    }


def test_validate_municipal_development_permit_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame([base_row("calgary_1"), base_row("calgary_2")])
    write_development_permit_table(silver_root, dataframe)

    report = validate_municipal_development_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 15


def test_validate_municipal_development_permit_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame([base_row("calgary_1"), base_row("calgary_1")])
    write_development_permit_table(silver_root, dataframe)

    report = validate_municipal_development_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "development_permit_key_not_null_and_unique" in failed_checks


def test_validate_municipal_development_permit_silver_outputs_fails_bad_coordinates(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1")
    row["latitude"] = 0.0
    row["longitude"] = 0.0

    dataframe = pd.DataFrame([row, base_row("calgary_2")])
    write_development_permit_table(silver_root, dataframe)

    report = validate_municipal_development_permit_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "development_permit_coordinates_in_calgary_range" in failed_checks
