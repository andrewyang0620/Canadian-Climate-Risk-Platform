import pandas as pd

from src.silver.validation import (
    validate_eccc_hydro_realtime_observation_silver_outputs,
)


def write_realtime_table(root, dataframe):
    path = root / "silver_hydro_realtime_observation" / "extract_date=2026-06-12" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(
        path / "silver_hydro_realtime_observation.parquet",
        index=False,
    )


def base_row(key, station_id, observed_at, province_code):
    return {
        "hydro_realtime_observation_key": key,
        "source_name": "eccc_hydrometric_realtime",
        "station_id": station_id,
        "province_code": province_code,
        "observed_at_utc": pd.Timestamp(observed_at),
        "latitude": 51.0 if province_code == "AB" else 49.2,
        "longitude": -114.0 if province_code == "AB" else -123.1,
        "geometry_wkt": "POINT (-114 51)",
        "water_level_m": 1.2,
        "discharge_cms": 10.0,
        "raw_discharge_cms": 10.0,
        "negative_discharge_flag": False,
        "source_record_count": 1,
    }


def test_validate_eccc_hydro_realtime_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row(
                "key_1",
                "05AA001",
                "2026-06-11T09:00:00Z",
                "AB",
            ),
            base_row(
                "key_2",
                "08AA001",
                "2026-06-12T21:00:00Z",
                "BC",
            ),
        ]
    )

    write_realtime_table(silver_root, dataframe)

    report = validate_eccc_hydro_realtime_observation_silver_outputs(
        silver_root=silver_root,
        reference_time_utc="2026-06-12T22:00:00Z",
    )

    assert report.passed is True
    assert len(report.checks) == 13


def test_validate_eccc_hydro_realtime_fails_duplicate_station_timestamp(
    tmp_path,
):
    silver_root = tmp_path / "silver"

    row_1 = base_row(
        "key_1",
        "05AA001",
        "2026-06-12T20:00:00Z",
        "AB",
    )
    row_2 = base_row(
        "key_2",
        "05AA001",
        "2026-06-12T20:00:00Z",
        "AB",
    )

    dataframe = pd.DataFrame([row_1, row_2])
    write_realtime_table(silver_root, dataframe)

    report = validate_eccc_hydro_realtime_observation_silver_outputs(
        silver_root=silver_root,
        reference_time_utc="2026-06-12T22:00:00Z",
        min_window_hours=0,
    )

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "hydro_realtime_station_timestamp_unique" in failed_checks
