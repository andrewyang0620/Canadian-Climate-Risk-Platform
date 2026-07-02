from __future__ import annotations

import pandas as pd

from src.gold.climate.validation import (
    validate_climate_monthly_feature_dataframes,
)


TEST_MONTH_MIN = "2020-01"
TEST_MONTH_MAX = "2020-02"
TEST_MONTH_COUNT = 2


def validate_test_data(
    *,
    station_month: pd.DataFrame | None = None,
    grid_month: pd.DataFrame | None = None,
    grid_cell: pd.DataFrame | None = None,
):
    if station_month is None:
        station_month = make_station_month()

    if grid_month is None:
        grid_month = make_grid_month()

    if grid_cell is None:
        grid_cell = make_grid_cell()

    return validate_climate_monthly_feature_dataframes(
        station_month=station_month,
        grid_month=grid_month,
        gold_grid_cell=grid_cell,
        expected_month_min=TEST_MONTH_MIN,
        expected_month_max=TEST_MONTH_MAX,
        expected_month_count=TEST_MONTH_COUNT,
    )


def make_station_month() -> pd.DataFrame:
    rows = []

    for month in [TEST_MONTH_MIN, TEST_MONTH_MAX]:
        rows.append(
            {
                "climate_station_month_key": f"AB__S1__{month}",
                "station_id": "S1",
                "station_name": "Station 1",
                "province_key": "AB",
                "reference_month": month,
                "latitude": 52.0,
                "longitude": -114.0,
                "daily_record_count": 31,
                "temperature_observation_count": 31,
                "precipitation_observation_count": 31,
                "total_precip_mm": 20.0,
                "total_rain_mm": 15.0,
                "total_snow": 5.0,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
            }
        )

    return pd.DataFrame(rows)


def make_grid_cell() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_grid",
                "grid_system": "ab_10km",
                "province_key": "AB",
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "bc_grid",
                "grid_system": "bc_10km",
                "province_key": "BC",
                "crs_epsg": 3347,
            },
        ]
    )


def base_grid_month_row(
    *,
    grid_cell_key: str,
    grid_system: str,
    province_key: str,
    reference_month: str,
    method: str,
) -> dict[str, object]:
    row = {
        "grid_month_climate_feature_key": f"{grid_cell_key}__{reference_month}",
        "grid_cell_key": grid_cell_key,
        "grid_system": grid_system,
        "grid_level": "province",
        "grid_version": "v1",
        "province_key": province_key,
        "reference_month": reference_month,
        "climate_mapping_method": method,
        "climate_station_count": 1,
        "climate_nearest_station_distance_km": 0.0,
        "climate_mean_station_distance_km": 0.0,
        "climate_max_station_distance_km": 0.0,
        "climate_idw_confidence_score": 1.0,
        "daily_record_count": 31.0,
        "temperature_observation_count": 31.0,
        "precipitation_observation_count": 31.0,
        "mean_temp_c": 10.0,
        "min_temp_c": 5.0,
        "max_temp_c": 15.0,
        "observed_min_temp_c": 3.0,
        "observed_max_temp_c": 17.0,
        "total_precip_mm": 20.0,
        "total_rain_mm": 15.0,
        "total_snow": 5.0,
        "precipitation_days": 8.0,
        "heavy_precipitation_days": 2.0,
        "extreme_heat_days": 0.0,
        "extreme_cold_days": 0.0,
        "freeze_thaw_days": 4.0,
        "temperature_completeness_ratio": 1.0,
        "precipitation_completeness_ratio": 1.0,
        "climate_data_completeness_score": 1.0,
        "climate_feature_quality_flag": "direct",
    }

    if method == "idw_interpolated":
        row.update(
            {
                "climate_station_count": 3,
                "climate_nearest_station_distance_km": 25.0,
                "climate_mean_station_distance_km": 60.0,
                "climate_max_station_distance_km": 120.0,
                "climate_idw_confidence_score": 0.7,
                "climate_feature_quality_flag": "medium",
            }
        )

    if method == "no_station_within_radius":
        row.update(
            {
                "climate_station_count": 0,
                "climate_nearest_station_distance_km": pd.NA,
                "climate_mean_station_distance_km": pd.NA,
                "climate_max_station_distance_km": pd.NA,
                "climate_idw_confidence_score": 0.0,
                "climate_feature_quality_flag": pd.NA,
                "climate_data_completeness_score": pd.NA,
            }
        )

        for column in [
            "daily_record_count",
            "temperature_observation_count",
            "precipitation_observation_count",
            "mean_temp_c",
            "min_temp_c",
            "max_temp_c",
            "observed_min_temp_c",
            "observed_max_temp_c",
            "total_precip_mm",
            "total_rain_mm",
            "total_snow",
            "precipitation_days",
            "heavy_precipitation_days",
            "extreme_heat_days",
            "extreme_cold_days",
            "freeze_thaw_days",
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
        ]:
            row[column] = pd.NA

    return row


def make_grid_month() -> pd.DataFrame:
    rows = [
        base_grid_month_row(
            grid_cell_key="ab_grid",
            grid_system="ab_10km",
            province_key="AB",
            reference_month="2020-01",
            method="direct_station_in_cell",
        ),
        base_grid_month_row(
            grid_cell_key="bc_grid",
            grid_system="bc_10km",
            province_key="BC",
            reference_month="2020-01",
            method="idw_interpolated",
        ),
        base_grid_month_row(
            grid_cell_key="ab_grid",
            grid_system="ab_10km",
            province_key="AB",
            reference_month="2020-02",
            method="no_station_within_radius",
        ),
        base_grid_month_row(
            grid_cell_key="bc_grid",
            grid_system="bc_10km",
            province_key="BC",
            reference_month="2020-02",
            method="direct_station_average_in_cell",
        ),
    ]

    rows[-1]["climate_station_count"] = 2

    return pd.DataFrame(rows)


def failed_check_names(report) -> set[str]:
    return {check.name for check in report.checks if not check.passed}


def test_climate_monthly_validation_passes_with_minimal_valid_data():
    report = validate_test_data()

    assert report.passed


def test_climate_monthly_validation_detects_negative_precipitation():
    grid_month = make_grid_month()
    grid_month.loc[0, "total_precip_mm"] = -1.0

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_precipitation_nonnegative" in failed_check_names(report)


def test_climate_monthly_validation_accepts_silver_coordinate_bounds():
    station_month = make_station_month()

    station_month.loc[:, "latitude"] = 60.8
    station_month.loc[:, "longitude"] = -109.5

    report = validate_test_data(station_month=station_month)

    assert report.passed


def test_climate_monthly_validation_detects_incomplete_skeleton():
    grid_month = make_grid_month().iloc[:-1].copy()

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_complete_skeleton" in failed_check_names(report)


def test_climate_monthly_validation_detects_invalid_idw_distance():
    grid_month = make_grid_month()
    idw_index = grid_month.index[grid_month["climate_mapping_method"] == "idw_interpolated"][0]

    grid_month.loc[idw_index, "climate_max_station_distance_km"] = 151.0

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_distance_valid" in failed_check_names(report)


def test_climate_monthly_validation_detects_invalid_mapping_method():
    grid_month = make_grid_month()
    grid_month.loc[0, "climate_mapping_method"] = "nearest_grid_cell"

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_mapping_methods_valid" in failed_check_names(report)


def test_climate_monthly_validation_detects_no_station_value_leakage():
    grid_month = make_grid_month()
    no_station_index = grid_month.index[
        grid_month["climate_mapping_method"] == "no_station_within_radius"
    ][0]

    grid_month.loc[no_station_index, "mean_temp_c"] = 10.0

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_no_station_null_semantics" in failed_check_names(report)


def test_climate_monthly_validation_detects_invalid_quality_flag_for_direct():
    grid_month = make_grid_month()
    grid_month.loc[0, "climate_feature_quality_flag"] = "high"

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_quality_flags_valid" in failed_check_names(report)


def test_climate_monthly_validation_detects_quality_flag_on_no_station_row():
    grid_month = make_grid_month()
    no_station_index = grid_month.index[
        grid_month["climate_mapping_method"] == "no_station_within_radius"
    ][0]

    grid_month.loc[no_station_index, "climate_feature_quality_flag"] = "very_low"

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_quality_flags_valid" in failed_check_names(report)


def test_climate_monthly_validation_detects_missing_mapped_completeness():
    grid_month = make_grid_month()
    idw_index = grid_month.index[grid_month["climate_mapping_method"] == "idw_interpolated"][0]

    grid_month.loc[idw_index, "climate_data_completeness_score"] = pd.NA

    report = validate_test_data(grid_month=grid_month)

    assert not report.passed
    assert "gold_grid_month_climate_mapped_completeness_present" in failed_check_names(report)
