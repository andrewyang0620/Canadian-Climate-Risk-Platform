import pandas as pd

from src.gold.climate_validation import (
    validate_climate_monthly_feature_dataframes,
)


def test_climate_monthly_validation_passes_with_minimal_valid_data():
    station_month = make_station_month()
    grid_month = make_grid_month()
    grid_cell = make_grid_cell()

    report = validate_climate_monthly_feature_dataframes(
        station_month=station_month,
        grid_month=grid_month,
        gold_grid_cell=grid_cell,
    )

    assert report.passed is True


def test_climate_monthly_validation_detects_negative_precipitation():
    station_month = make_station_month()
    grid_month = make_grid_month()
    grid_cell = make_grid_cell()

    grid_month.loc[0, "total_precip_mm"] = -1.0

    report = validate_climate_monthly_feature_dataframes(
        station_month=station_month,
        grid_month=grid_month,
        gold_grid_cell=grid_cell,
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_month_climate_precipitation_nonnegative" in failed_names


def make_station_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M"):
        rows.append(
            {
                "climate_station_month_key": f"station_ab__{month}",
                "station_id": "station_ab",
                "station_name": "Station AB",
                "province_key": "AB",
                "reference_month": str(month),
                "latitude": 53.5,
                "longitude": -113.5,
                "daily_record_count": 30,
                "temperature_observation_count": 30,
                "precipitation_observation_count": 30,
                "total_precip_mm": 10.0,
                "total_rain_mm": None,
                "total_snow": None,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
            }
        )

        rows.append(
            {
                "climate_station_month_key": f"station_bc__{month}",
                "station_id": "station_bc",
                "station_name": "Station BC",
                "province_key": "BC",
                "reference_month": str(month),
                "latitude": 49.25,
                "longitude": -123.1,
                "daily_record_count": 30,
                "temperature_observation_count": 30,
                "precipitation_observation_count": 30,
                "total_precip_mm": 20.0,
                "total_rain_mm": None,
                "total_snow": None,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
            }
        )

    return pd.DataFrame(rows)


def make_grid_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M"):
        rows.append(
            {
                "grid_month_climate_feature_key": f"ab_grid__{month}",
                "grid_cell_key": "ab_grid",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "reference_month": str(month),
                "station_count": 1,
                "nearest_station_distance_km": 0.0,
                "mean_station_distance_km": 0.0,
                "total_precip_mm": 10.0,
                "total_rain_mm": None,
                "total_snow": None,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
                "climate_data_completeness_score": 1.0,
                "climate_feature_quality_flag": "high",
            }
        )

        rows.append(
            {
                "grid_month_climate_feature_key": f"bc_grid__{month}",
                "grid_cell_key": "bc_grid",
                "grid_system": "bc_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "BC",
                "reference_month": str(month),
                "station_count": 1,
                "nearest_station_distance_km": 0.0,
                "mean_station_distance_km": 0.0,
                "total_precip_mm": 20.0,
                "total_rain_mm": None,
                "total_snow": None,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
                "climate_data_completeness_score": 1.0,
                "climate_feature_quality_flag": "high",
            }
        )

    return pd.DataFrame(rows)


def make_grid_cell():
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
