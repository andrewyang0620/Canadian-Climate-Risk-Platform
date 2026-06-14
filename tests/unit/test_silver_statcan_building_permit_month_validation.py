import pandas as pd

from src.silver.validation import validate_statcan_building_permit_month_silver_outputs


def write_statcan_permit_table(root, dataframe):
    path = root / "silver_permit_monthly" / "extract_date=2026-06-11" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_permit_monthly.parquet", index=False)


def base_row(key):
    return {
        "statcan_building_permit_month_key": key,
        "source_name": "statcan_building_permits",
        "statcan_table_id": "34-10-0292-01",
        "statcan_product_id": "3410029201",
        "reference_month": pd.Timestamp("2026-01-01"),
        "reference_year": 2026,
        "reference_month_number": 1,
        "geo_name": "Alberta",
        "dguid": "2021A000248",
        "geo_level": "province",
        "province_code": "AB",
        "type_of_building": "Total residential and non-residential",
        "type_of_work": "Types of work, total",
        "measure_name": "Value of permits",
        "seasonal_adjustment_value_type": "Unadjusted, current",
        "value": 100.0,
        "value_scaled": 100000.0,
        "unit_of_measure": "Dollars",
        "unit_of_measure_id": 81,
        "scalar_factor": "thousands",
        "scalar_factor_id": 3,
        "vector": key,
        "coordinate": "1.1.1.1.1",
        "status": None,
        "symbol": None,
        "terminated": None,
        "decimals": 0,
        "source_record_number": 1,
        "source_record_count": 1,
    }


def test_validate_statcan_building_permit_month_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    rows = []
    geos = [
        ("Canada", "country", None),
        ("Alberta", "province", "AB"),
        ("British Columbia", "province", "BC"),
        ("Calgary, Alberta", "cma", "AB"),
        ("Vancouver, British Columbia", "cma", "BC"),
    ]

    measures = [
        "Value of permits",
        "Number of permits",
        "Number of dwelling-units lost",
        "Number of dwelling-units created",
        "Number of dwelling-units demolished",
    ]

    counter = 0
    for geo, geo_level, province_code in geos:
        for measure in measures:
            counter += 1
            row = base_row(f"v{counter}")
            row["statcan_building_permit_month_key"] = f"key_{counter}"
            row["geo_name"] = geo
            row["geo_level"] = geo_level
            row["province_code"] = province_code
            row["measure_name"] = measure
            row["vector"] = f"v{counter}"
            rows.append(row)

    dataframe = pd.DataFrame(rows)
    write_statcan_permit_table(silver_root, dataframe)

    report = validate_statcan_building_permit_month_silver_outputs(
        silver_root=silver_root,
        min_expected_row_count=1,
    )

    assert report.passed is True
    assert len(report.checks) == 12


def test_validate_statcan_building_permit_month_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row_1 = base_row("v1")
    row_2 = base_row("v2")
    row_2["statcan_building_permit_month_key"] = row_1["statcan_building_permit_month_key"]

    dataframe = pd.DataFrame([row_1, row_2])
    write_statcan_permit_table(silver_root, dataframe)

    report = validate_statcan_building_permit_month_silver_outputs(
        silver_root=silver_root,
        min_expected_row_count=1,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "statcan_building_permit_month_key_not_null_and_unique" in failed_checks
