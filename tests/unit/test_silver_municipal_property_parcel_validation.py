import pandas as pd

from src.silver.validation import validate_municipal_property_parcel_silver_outputs


def write_property_parcel_table(root, dataframe):
    path = root / "silver_property_parcel" / "extract_date=2026-06-09" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_property_parcel.parquet", index=False)


def base_row(key):
    return {
        "property_parcel_key": key,
        "city": "vancouver",
        "province": "BC",
        "source_name": "vancouver_property_parcels",
        "source_parcel_id": "SITE1",
        "source_tax_coord": "12345678",
        "civic_number": "100",
        "street_name": "TEST ST",
        "address_text": "100 TEST ST",
        "latitude": 49.25,
        "longitude": -123.1,
        "geometry_type": "Polygon",
        "geometry_wkt": "POLYGON ((-123.1 49.25, -123.0 49.25, -123.1 49.25))",
        "source_record_count": 1,
    }


def test_validate_municipal_property_parcel_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame([base_row("vancouver_1"), base_row("vancouver_2")])
    write_property_parcel_table(silver_root, dataframe)

    report = validate_municipal_property_parcel_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 12


def test_validate_municipal_property_parcel_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame([base_row("vancouver_1"), base_row("vancouver_1")])
    write_property_parcel_table(silver_root, dataframe)

    report = validate_municipal_property_parcel_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_parcel_key_not_null_and_unique" in failed_checks


def test_validate_municipal_property_parcel_silver_outputs_fails_bad_coordinates(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("vancouver_1")
    row["latitude"] = 0.0
    row["longitude"] = 0.0

    dataframe = pd.DataFrame([row, base_row("vancouver_2")])
    write_property_parcel_table(silver_root, dataframe)

    report = validate_municipal_property_parcel_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "property_parcel_coordinates_in_vancouver_range" in failed_checks
