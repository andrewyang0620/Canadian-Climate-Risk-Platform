import pandas as pd

from src.silver.validation import validate_canadian_disaster_database_silver_outputs


def write_disaster_table(root, dataframe):
    path = root / "silver_disaster_event_month" / "extract_date=2026-05-25" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_disaster_event_month.parquet", index=False)


def base_row(key, province, event_month):
    return {
        "disaster_event_month_key": key,
        "source_event_id": "1",
        "province": province,
        "event_month": event_month,
        "event_year": int(event_month[:4]),
        "event_month_number": int(event_month[5:7]),
        "event_start_date": "2020-01-15",
        "event_end_date": "2020-03-02",
        "disaster_type": "Flood",
        "fatalities": 0,
        "injured": 0,
        "evacuated": 10,
        "estimated_total_cost_cad": 1000.0,
        "normalized_total_cost_cad": None,
        "source_record_count": 1,
    }


def test_validate_canadian_disaster_database_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row("1_BC_2020-01", "BC", "2020-01-01"),
            base_row("1_BC_2020-02", "BC", "2020-02-01"),
            base_row("1_AB_2020-03", "AB", "2020-03-01"),
        ]
    )

    write_disaster_table(silver_root, dataframe)

    report = validate_canadian_disaster_database_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 9


def test_validate_canadian_disaster_database_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("1_BC_2020-01", "BC", "2020-01-01")
    dataframe = pd.DataFrame([row, row])

    write_disaster_table(silver_root, dataframe)

    report = validate_canadian_disaster_database_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "disaster_event_month_key_not_null_and_unique" in failed_checks


def test_validate_canadian_disaster_database_silver_outputs_fails_negative_impact(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("1_BC_2020-01", "BC", "2020-01-01")
    row["fatalities"] = -1
    dataframe = pd.DataFrame([row])

    write_disaster_table(silver_root, dataframe)

    report = validate_canadian_disaster_database_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "disaster_impact_counts_non_negative" in failed_checks


def test_validate_canadian_disaster_database_silver_outputs_fails_month_outside_range(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("1_BC_2020-05", "BC", "2020-05-01")
    dataframe = pd.DataFrame([row])

    write_disaster_table(silver_root, dataframe)

    report = validate_canadian_disaster_database_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "disaster_event_month_between_start_and_end" in failed_checks
