import pandas as pd

from src.silver.validation import validate_hydat_archive_silver_outputs


def write_station_table(root, dataframe):
    path = root / "silver_hydro_station" / "extract_date=2026-05-24" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_hydro_station.parquet", index=False)


def write_daily_partition(root, year, dataframe):
    path = (
        root
        / "silver_hydro_daily"
        / "extract_date=2026-05-24"
        / "run_id=test"
        / f"observation_year={year}"
    )
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_hydro_daily.parquet", index=False)


def station_row(station_id, province):
    return {
        "station_id": station_id,
        "station_name": "TEST",
        "province": province,
        "latitude": 50.0,
        "longitude": -120.0,
        "geometry_wkt": "POINT (-120 50)",
    }


def daily_row(station_id, date, year, measurement_type, value):
    return {
        "hydro_daily_key": f"{station_id}_{date}_{measurement_type}",
        "station_id": station_id,
        "observation_date": date,
        "observation_year": year,
        "observation_month": 1,
        "observation_day": 1,
        "measurement_type": measurement_type,
        "measurement_value": value,
        "source_record_count": 1,
    }


def test_validate_hydat_archive_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    station_df = pd.DataFrame(
        [
            station_row("01AA001", "BC"),
            station_row("05AB001", "AB"),
        ]
    )

    write_station_table(silver_root, station_df)

    daily_df_2020 = pd.DataFrame(
        [
            daily_row("01AA001", "2020-01-01", 2020, "flow", 1.2),
            daily_row("05AB001", "2020-01-01", 2020, "level", 2.3),
        ]
    )

    daily_df_2021 = pd.DataFrame(
        [
            daily_row("01AA001", "2021-01-01", 2021, "flow", 1.5),
            daily_row("05AB001", "2021-01-01", 2021, "level", 2.5),
        ]
    )

    write_daily_partition(silver_root, 2020, daily_df_2020)
    write_daily_partition(silver_root, 2021, daily_df_2021)

    report = validate_hydat_archive_silver_outputs(
        silver_root=silver_root,
        expected_start_year=2020,
        expected_end_year=2021,
    )

    assert report.passed is True
    assert len(report.checks) == 12


def test_validate_hydat_archive_silver_outputs_fails_duplicate_daily_key(tmp_path):
    silver_root = tmp_path / "silver"

    station_df = pd.DataFrame(
        [
            station_row("01AA001", "BC"),
            station_row("05AB001", "AB"),
        ]
    )

    write_station_table(silver_root, station_df)

    row = daily_row("01AA001", "2020-01-01", 2020, "flow", 1.2)
    write_daily_partition(silver_root, 2020, pd.DataFrame([row, row]))

    report = validate_hydat_archive_silver_outputs(
        silver_root=silver_root,
        expected_start_year=2020,
        expected_end_year=2020,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "hydro_daily_key_not_null_and_unique" in failed_checks


def test_validate_hydat_archive_silver_outputs_fails_unknown_station(tmp_path):
    silver_root = tmp_path / "silver"

    station_df = pd.DataFrame(
        [
            station_row("01AA001", "BC"),
            station_row("05AB001", "AB"),
        ]
    )

    write_station_table(silver_root, station_df)

    daily_df = pd.DataFrame(
        [
            daily_row("UNKNOWN", "2020-01-01", 2020, "flow", 1.2),
            daily_row("05AB001", "2020-01-01", 2020, "level", 2.3),
        ]
    )

    write_daily_partition(silver_root, 2020, daily_df)

    report = validate_hydat_archive_silver_outputs(
        silver_root=silver_root,
        expected_start_year=2020,
        expected_end_year=2020,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "hydro_daily_station_ids_exist_in_station_table" in failed_checks
