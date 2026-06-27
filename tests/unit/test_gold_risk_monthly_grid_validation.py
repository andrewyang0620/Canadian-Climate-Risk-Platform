import pandas as pd

from src.gold.mart.risk_monthly_grid import build_gold_grid_month_risk_feature_mart
from src.gold.mart.validation import validate_risk_monthly_grid_dataframes


def make_grid():
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_x1_y1",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "province_code": "48",
                "province_name": "Alberta",
                "boundary_year": 2021,
                "cell_size_m": 10000,
                "grid_x_index": 1,
                "grid_y_index": 1,
                "centroid_longitude": -114.0,
                "centroid_latitude": 51.0,
                "full_cell_area_sq_km": 100.0,
                "analysis_area_sq_km": 100.0,
                "boundary_coverage_ratio": 1.0,
                "is_boundary_edge_cell": False,
            },
            {
                "grid_cell_key": "bc_10km_x2_y2",
                "grid_system": "bc_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "BC",
                "province_code": "59",
                "province_name": "British Columbia",
                "boundary_year": 2021,
                "cell_size_m": 10000,
                "grid_x_index": 2,
                "grid_y_index": 2,
                "centroid_longitude": -123.0,
                "centroid_latitude": 49.0,
                "full_cell_area_sq_km": 100.0,
                "analysis_area_sq_km": 100.0,
                "boundary_coverage_ratio": 1.0,
                "is_boundary_edge_cell": False,
            },
        ]
    )


def make_municipality_bridge():
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_x1_y1",
                "municipality_key": "4806016",
                "municipality_name": "Calgary",
                "municipality_type": "CY",
                "grid_coverage_ratio": 1.0,
                "municipality_coverage_ratio": 0.1,
                "is_primary_municipality": True,
                "municipality_match_count": 1,
            },
            {
                "grid_cell_key": "bc_10km_x2_y2",
                "municipality_key": "5915022",
                "municipality_name": "Vancouver",
                "municipality_type": "CY",
                "grid_coverage_ratio": 1.0,
                "municipality_coverage_ratio": 0.2,
                "is_primary_municipality": True,
                "municipality_match_count": 1,
            },
        ]
    )


def make_climate_grid_month():
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_x1_y1",
                "reference_month": "2016-01",
                "station_count": 1,
                "daily_record_count": 31,
                "temperature_observation_count": 31,
                "precipitation_observation_count": 31,
                "mean_temp_c": -5.0,
                "min_temp_c": -10.0,
                "max_temp_c": 2.0,
                "observed_min_temp_c": -20.0,
                "observed_max_temp_c": 8.0,
                "total_precip_mm": 20.0,
                "total_rain_mm": 5.0,
                "total_snow": 15.0,
                "precipitation_days": 10.0,
                "heavy_precipitation_days": 1.0,
                "extreme_heat_days": 0.0,
                "extreme_cold_days": 2.0,
                "freeze_thaw_days": 12.0,
                "nearest_station_distance_km": 0.0,
                "mean_station_distance_km": 0.0,
                "temperature_completeness_ratio": 1.0,
                "precipitation_completeness_ratio": 1.0,
                "climate_data_completeness_score": 1.0,
                "climate_feature_quality_flag": "high",
                "grid_month_climate_feature_key": ("ab_10km_x1_y1__2016-01"),
            }
        ]
    )


def make_hydro_grid_month():
    base = {
        "grid_cell_key": "ab_10km_x1_y1",
        "reference_month": "2016-01",
        "station_count": 1,
        "daily_record_count": 31,
        "observation_day_count": 31,
        "measurement_observation_count": 31,
        "mean_measurement_completeness_ratio": 1.0,
        "flow_zero_day_count": 0,
        "negative_value_count": 0,
        "nearest_station_distance_km": 0.0,
        "mean_station_distance_km": 0.0,
        "hydro_feature_quality_flag": "high",
    }

    return pd.DataFrame(
        [
            {
                **base,
                "measurement_type": "flow",
                "mean_measurement_value": 5.0,
                "min_measurement_value": 1.0,
                "max_measurement_value": 10.0,
                "median_measurement_value": 4.0,
                "p95_measurement_value": 9.0,
            },
            {
                **base,
                "measurement_type": "level",
                "mean_measurement_value": 0.8,
                "min_measurement_value": 0.2,
                "max_measurement_value": 1.4,
                "median_measurement_value": 0.7,
                "p95_measurement_value": 1.3,
            },
        ]
    )


def make_valid_validation_inputs():
    grid = make_grid()
    climate = make_climate_grid_month()
    hydro = make_hydro_grid_month()

    mart, _ = build_gold_grid_month_risk_feature_mart(
        grid=grid,
        municipality_bridge=make_municipality_bridge(),
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    return mart, grid, climate, hydro


def failed_check_names(report):
    return {check.name for check in report.checks if not check.passed}


def test_risk_monthly_grid_validation_passes_valid_mart():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is True


def test_risk_monthly_grid_validation_detects_duplicate_key():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    mart.loc[1, "grid_month_risk_feature_key"] = mart.loc[
        0,
        "grid_month_risk_feature_key",
    ]

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_key_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_bad_grid_system():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    mart.loc[0, "grid_system"] = "calgary_1km"

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_grid_systems_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_climate_flag_mismatch():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    target = (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    mart.loc[target, "has_climate_feature"] = False

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_climate_coverage_flags_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_hydro_flow_flag_mismatch():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    target = (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    mart.loc[target, "has_hydro_flow_feature"] = False

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_hydro_flow_coverage_flags_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_invalid_quality_flag():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    target = (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    mart.loc[target, "flow_feature_quality_flag"] = "bad_quality"

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_flow_feature_quality_flag_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_invalid_completeness_ratio():
    mart, grid, climate, hydro = make_valid_validation_inputs()

    target = (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    mart.loc[target, "climate_data_completeness_score"] = 1.5

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
    )

    assert report.passed is False
    assert "gold_risk_mart_climate_data_completeness_score_valid" in failed_check_names(report)
