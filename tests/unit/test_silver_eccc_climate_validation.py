import pandas as pd

from src.silver.validation import validate_eccc_climate_daily_silver_outputs


def write_climate_partition(root, year, dataframe):
    path = (
        root
        / "silver_climate_daily"
        / "extract_date=2026-05-22"
        / "run_id=test"
        / f"observation_year={year}"
    )
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_climate_daily.parquet", index=False)


def base_row(station_id, province, date, year):
    return {
        "climate_daily_key": f"{station_id}_{date}",
        "station_id": station_id,
        "station_name": "TEST",
        "province": province,
        "observation_date": date,
        "observation_year": year,
        "observation_month": 1,
        "observation_day": 1,
        "latitude": 50.0,
        "longitude": -120.0,
        "geometry_type": "Point",
        "mean_temp_c": 1.0,
        "min_temp_c": 0.0,
        "max_temp_c": 2.0,
        "total_precip_mm": None,
        "total_rain_mm": None,
        "total_snow": None,
        "snow_on_ground": None,
        "speed_max_gust": None,
        "direction_max_gust": None,
        "cooling_degree_days": None,
        "heating_degree_days": None,
        "min_relative_humidity": None,
        "max_relative_humidity": None,
        "source_name": "eccc_historical_climate",
    }


def test_validate_eccc_climate_daily_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    write_climate_partition(
        silver_root,
        2016,
        pd.DataFrame(
            [
                base_row("101", "BC", "2016-01-01", 2016),
                base_row("202", "AB", "2016-01-01", 2016),
            ]
        ),
    )
    write_climate_partition(
        silver_root,
        2017,
        pd.DataFrame(
            [
                base_row("101", "BC", "2017-01-01", 2017),
                base_row("202", "AB", "2017-01-01", 2017),
            ]
        ),
    )

    report = validate_eccc_climate_daily_silver_outputs(
        silver_root=silver_root,
        expected_years=[2016, 2017],
    )

    assert report.passed is True
    assert len(report.checks) == 10


def test_validate_eccc_climate_daily_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    duplicate = base_row("101", "BC", "2016-01-01", 2016)

    write_climate_partition(
        silver_root,
        2016,
        pd.DataFrame([duplicate, duplicate]),
    )

    report = validate_eccc_climate_daily_silver_outputs(
        silver_root=silver_root,
        expected_years=[2016],
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "climate_daily_key_not_null_and_unique" in failed_checks
