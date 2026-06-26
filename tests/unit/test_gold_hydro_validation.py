import pandas as pd

from src.gold.hydro.validation import validate_hydro_monthly_feature_dataframes


def make_station_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        days = pd.Period(month, freq="M").days_in_month

        for province_key, station_id, latitude, longitude in [
            ("AB", "05AA001", 49.6, -114.0),
            ("BC", "08MH011", 49.2, -122.3),
        ]:
            for measurement_type in ["flow", "level"]:
                is_flow = measurement_type == "flow"

                rows.append(
                    {
                        "hydro_station_month_key": (
                            f"{province_key}__{station_id}__" f"{measurement_type}__{month}"
                        ),
                        "province_key": province_key,
                        "station_id": station_id,
                        "station_name": f"Station {station_id}",
                        "measurement_type": measurement_type,
                        "reference_month": month,
                        "latitude": latitude,
                        "longitude": longitude,
                        "drainage_area_gross": 100.0,
                        "drainage_area_effect": None,
                        "rhbn": "0",
                        "real_time": "1",
                        "daily_record_count": days,
                        "observation_day_count": days,
                        "measurement_observation_count": days,
                        "days_in_month": days,
                        "measurement_completeness_ratio": 1.0,
                        "mean_measurement_value": 1.0 if is_flow else 0.5,
                        "min_measurement_value": 0.0 if is_flow else -0.5,
                        "max_measurement_value": 2.0 if is_flow else 1.5,
                        "median_measurement_value": 1.0 if is_flow else 0.5,
                        "p95_measurement_value": 1.9 if is_flow else 1.4,
                        "measurement_symbol_count": 0,
                        "estimated_symbol_count": 0,
                        "approved_symbol_count": 0,
                        "flow_zero_day_count": 1 if is_flow else 0,
                        "negative_value_count": 0 if is_flow else 1,
                    }
                )

    return pd.DataFrame(rows)


def test_hydro_monthly_validation_passes_valid_station_month():
    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
    )

    assert report.passed is True


def test_hydro_monthly_validation_detects_duplicate_station_month_key():
    station_month = make_station_month()
    station_month.loc[1, "hydro_station_month_key"] = station_month.loc[
        0,
        "hydro_station_month_key",
    ]

    report = validate_hydro_monthly_feature_dataframes(
        station_month=station_month,
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_hydro_station_month_key_valid" in failed_names


def test_hydro_monthly_validation_rejects_negative_flow():
    station_month = make_station_month()

    flow_index = station_month[station_month["measurement_type"] == "flow"].index[0]

    station_month.loc[flow_index, "negative_value_count"] = 1
    station_month.loc[flow_index, "min_measurement_value"] = -1.0

    report = validate_hydro_monthly_feature_dataframes(
        station_month=station_month,
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_hydro_station_month_flow_nonnegative" in failed_names


def test_hydro_monthly_validation_allows_negative_level():
    station_month = make_station_month()

    level_negative_total = int(
        station_month.loc[
            station_month["measurement_type"] == "level",
            "negative_value_count",
        ].sum()
    )

    assert level_negative_total > 0

    report = validate_hydro_monthly_feature_dataframes(
        station_month=station_month,
    )

    assert report.passed is True


def test_hydro_monthly_validation_detects_unexpected_measurement_type():
    station_month = make_station_month()
    station_month.loc[0, "measurement_type"] = "temperature"

    report = validate_hydro_monthly_feature_dataframes(
        station_month=station_month,
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_hydro_station_month_measurement_types_valid" in failed_names


def make_grid_cell():
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "province_key": "AB",
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "bc_10km_test",
                "grid_system": "bc_10km",
                "province_key": "BC",
                "crs_epsg": 3347,
            },
        ]
    )


def make_grid_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        days = pd.Period(month, freq="M").days_in_month

        for province_key, grid_cell_key, grid_system in [
            ("AB", "ab_10km_test", "ab_10km"),
            ("BC", "bc_10km_test", "bc_10km"),
        ]:
            for measurement_type in ["flow", "level"]:
                is_flow = measurement_type == "flow"

                rows.append(
                    {
                        "grid_month_hydro_feature_key": (
                            f"{grid_cell_key}__{measurement_type}__{month}"
                        ),
                        "grid_cell_key": grid_cell_key,
                        "grid_system": grid_system,
                        "grid_level": "province",
                        "grid_version": "v1",
                        "province_key": province_key,
                        "measurement_type": measurement_type,
                        "reference_month": month,
                        "station_count": 1,
                        "daily_record_count": days,
                        "observation_day_count": days,
                        "measurement_observation_count": days,
                        "mean_measurement_value": 1.0 if is_flow else 0.5,
                        "min_measurement_value": 0.0 if is_flow else -0.5,
                        "max_measurement_value": 2.0 if is_flow else 1.5,
                        "median_measurement_value": 1.0 if is_flow else 0.5,
                        "p95_measurement_value": 1.9 if is_flow else 1.4,
                        "mean_measurement_completeness_ratio": 1.0,
                        "flow_zero_day_count": 1 if is_flow else 0,
                        "negative_value_count": 0 if is_flow else 1,
                        "nearest_station_distance_km": 0.0,
                        "mean_station_distance_km": 0.0,
                        "hydro_feature_quality_flag": "high",
                    }
                )

    return pd.DataFrame(rows)


def test_hydro_monthly_validation_passes_valid_station_and_grid_month():
    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=make_grid_month(),
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is True


def test_hydro_monthly_validation_detects_unknown_grid_key():
    grid_month = make_grid_month()
    grid_month.loc[0, "grid_cell_key"] = "unknown_grid"

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_month_hydro_grid_keys_known" in failed_names


def test_hydro_monthly_validation_detects_duplicate_grid_month_key():
    grid_month = make_grid_month()
    grid_month.loc[1, "grid_month_hydro_feature_key"] = grid_month.loc[
        0,
        "grid_month_hydro_feature_key",
    ]

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_month_hydro_key_valid" in failed_names


def test_hydro_monthly_validation_detects_invalid_grid_distance():
    grid_month = make_grid_month()
    grid_month.loc[0, "nearest_station_distance_km"] = 99.0
    grid_month.loc[0, "mean_station_distance_km"] = 99.0

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_month_hydro_distance_valid" in failed_names


def test_hydro_monthly_validation_detects_invalid_quality_flag():
    grid_month = make_grid_month()
    grid_month.loc[0, "hydro_feature_quality_flag"] = "bad_quality"

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_month_hydro_quality_flags_valid" in failed_names
