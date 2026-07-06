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
                "grid_coverage_ratio": 0.8,
                "municipality_coverage_ratio": 0.1,
                "is_primary_municipality": True,
                "municipality_match_count": 2,
            },
            {
                "grid_cell_key": "ab_10km_x1_y1",
                "municipality_key": "4800001",
                "municipality_name": "Secondary",
                "municipality_type": "MD",
                "grid_coverage_ratio": 0.2,
                "municipality_coverage_ratio": 0.01,
                "is_primary_municipality": False,
                "municipality_match_count": 2,
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


def _climate_row(
    *,
    grid_cell_key,
    reference_month,
    method,
    station_count,
    quality,
):
    has_values = method != "no_station_within_radius"

    return {
        "grid_month_climate_feature_key": f"{grid_cell_key}__{reference_month}",
        "grid_cell_key": grid_cell_key,
        "reference_month": reference_month,
        "climate_mapping_method": method,
        "climate_station_count": station_count,
        "climate_nearest_station_distance_km": 0.0 if has_values else None,
        "climate_mean_station_distance_km": 0.0 if has_values else None,
        "climate_max_station_distance_km": 0.0 if has_values else None,
        "climate_idw_confidence_score": 1.0 if has_values else 0.0,
        "daily_record_count": 31 if has_values else 0,
        "temperature_observation_count": 31 if has_values else 0,
        "precipitation_observation_count": 31 if has_values else 0,
        "mean_temp_c": -5.0 if has_values else None,
        "min_temp_c": -10.0 if has_values else None,
        "max_temp_c": 2.0 if has_values else None,
        "observed_min_temp_c": -20.0 if has_values else None,
        "observed_max_temp_c": 8.0 if has_values else None,
        "total_precip_mm": 20.0 if has_values else None,
        "total_rain_mm": 5.0 if has_values else None,
        "total_snow": 15.0 if has_values else None,
        "precipitation_days": 10.0 if has_values else None,
        "heavy_precipitation_days": 1.0 if has_values else None,
        "extreme_heat_days": 0.0 if has_values else None,
        "extreme_cold_days": 2.0 if has_values else None,
        "freeze_thaw_days": 12.0 if has_values else None,
        "temperature_completeness_ratio": 1.0 if has_values else None,
        "precipitation_completeness_ratio": 1.0 if has_values else None,
        "climate_data_completeness_score": 1.0 if has_values else None,
        "climate_feature_quality_flag": quality,
    }


def make_climate_grid_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        rows.append(
            _climate_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month=month,
                method="direct_station_in_cell",
                station_count=1,
                quality="direct",
            )
        )
        rows.append(
            _climate_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month=month,
                method="no_station_within_radius",
                station_count=0,
                quality=None,
            )
        )

    return pd.DataFrame(rows)


def _hydro_row(
    *,
    grid_cell_key,
    reference_month,
    method,
):
    has_values = method != "no_hydro_coverage"

    return {
        "grid_month_hydro_feature_key": f"{grid_cell_key}__{reference_month}",
        "grid_cell_key": grid_cell_key,
        "reference_month": reference_month,
        "hydro_spatial_assignment_method": method,
        "hydro_station_count": 1 if has_values else 0,
        "hydro_basin_station_count": 1 if method == "basin_polygon_intersection" else 0,
        "hydro_point_station_count": 1 if method == "station_point_in_cell" else 0,
        "hydro_basin_intersection_area_sq_km": (
            50.0 if method == "basin_polygon_intersection" else None
        ),
        "hydro_basin_grid_coverage_ratio": (
            0.5 if method == "basin_polygon_intersection" else None
        ),
        "flow_station_count": 1 if has_values else 0,
        "flow_daily_record_count": 31 if has_values else 0,
        "flow_observation_day_count": 31 if has_values else 0,
        "flow_measurement_observation_count": 31 if has_values else 0,
        "flow_mean_measurement_value": 5.0 if has_values else None,
        "flow_min_measurement_value": 1.0 if has_values else None,
        "flow_max_measurement_value": 10.0 if has_values else None,
        "flow_median_measurement_value": 4.0 if has_values else None,
        "flow_p95_measurement_value": 9.0 if has_values else None,
        "flow_measurement_completeness_ratio": 1.0 if has_values else None,
        "flow_zero_day_count": 0,
        "flow_negative_value_count": 0,
        "level_station_count": 1 if has_values else 0,
        "level_daily_record_count": 31 if has_values else 0,
        "level_observation_day_count": 31 if has_values else 0,
        "level_measurement_observation_count": 31 if has_values else 0,
        "level_mean_measurement_value": 0.8 if has_values else None,
        "level_min_measurement_value": 0.2 if has_values else None,
        "level_max_measurement_value": 1.4 if has_values else None,
        "level_median_measurement_value": 0.7 if has_values else None,
        "level_p95_measurement_value": 1.3 if has_values else None,
        "level_measurement_completeness_ratio": 1.0 if has_values else None,
        "level_negative_value_count": 0,
        "hydro_data_completeness_score": 1.0 if has_values else None,
        "hydro_feature_quality_flag": "high" if has_values else None,
    }


def make_hydro_grid_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        rows.append(
            _hydro_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month=month,
                method="basin_polygon_intersection",
            )
        )
        rows.append(
            _hydro_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month=month,
                method="no_hydro_coverage",
            )
        )

    return pd.DataFrame(rows)


def _wildfire_row(
    *,
    grid_cell_key,
    reference_month,
    has_overlap,
):
    return {
        "wildfire_grid_month_key": f"{grid_cell_key}__{reference_month}",
        "grid_cell_key": grid_cell_key,
        "reference_month": reference_month,
        "crs_epsg": 3347,
        "grid_analysis_area_sq_km": 100.0,
        "wildfire_perimeter_count": 1 if has_overlap else 0,
        "wildfire_intersection_area_sq_km": 2.0 if has_overlap else 0.0,
        "wildfire_intersection_area_ha": 200.0 if has_overlap else 0.0,
        "wildfire_intersection_area_ratio_of_grid": 0.02 if has_overlap else 0.0,
        "wildfire_max_source_size_ha": 500.0 if has_overlap else 0.0,
        "wildfire_max_calculated_size_ha": 450.0 if has_overlap else 0.0,
        "wildfire_cause_n_polygon_count": 1 if has_overlap else 0,
        "wildfire_cause_h_polygon_count": 0,
        "wildfire_cause_u_polygon_count": 0,
        "wildfire_cause_prescribed_burn_polygon_count": 0,
        "wildfire_cause_other_polygon_count": 0,
        "wildfire_has_observed_perimeter_overlap": has_overlap,
        "wildfire_temporal_assignment_method": (
            "polygon_fire_month" if has_overlap else "no_observed_perimeter_overlap"
        ),
    }


def make_wildfire_grid_month():
    rows = []

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        rows.append(
            _wildfire_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month=month,
                has_overlap=True,
            )
        )
        rows.append(
            _wildfire_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month=month,
                has_overlap=False,
            )
        )

    return pd.DataFrame(rows)


def make_valid_validation_inputs():
    grid = make_grid()
    climate = make_climate_grid_month()
    hydro = make_hydro_grid_month()
    wildfire = make_wildfire_grid_month()

    mart, _ = build_gold_grid_month_risk_feature_mart(
        grid=grid,
        municipality_bridge=make_municipality_bridge(),
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    return mart, grid, climate, hydro, wildfire


def failed_check_names(report):
    return {check.name for check in report.checks if not check.passed}


def test_risk_monthly_grid_validation_passes_valid_mart():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is True


def test_risk_monthly_grid_validation_detects_duplicate_key():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    mart.loc[1, "grid_month_risk_feature_key"] = mart.loc[
        0,
        "grid_month_risk_feature_key",
    ]

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_key_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_bad_grid_system():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    mart.loc[0, "grid_system"] = "bad_grid"

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_grid_systems_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_climate_flag_mismatch():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    target_index = mart[
        (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    ].index[0]

    mart.loc[target_index, "has_climate_feature"] = False

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_climate_methods_and_semantics_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_hydro_flow_flag_mismatch():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    target_index = mart[
        (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    ].index[0]

    mart.loc[target_index, "has_hydro_flow_feature"] = False

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_hydro_methods_and_semantics_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_wildfire_overlap_flag_mismatch():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    target_index = mart[
        (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    ].index[0]

    mart.loc[target_index, "has_wildfire_observed_perimeter_overlap"] = False

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_wildfire_methods_and_semantics_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_invalid_quality_flag():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    mart.loc[0, "hydro_feature_quality_flag"] = "bad_quality"

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_quality_flags_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_invalid_ratio():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    mart.loc[0, "hydro_basin_grid_coverage_ratio"] = 1.5

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_ratio_fields_valid" in failed_check_names(report)


def test_risk_monthly_grid_validation_detects_no_hydro_value_leakage():
    mart, grid, climate, hydro, wildfire = make_valid_validation_inputs()
    no_hydro_index = mart[
        (mart["grid_cell_key"] == "bc_10km_x2_y2") & (mart["reference_month"] == "2016-01")
    ].index[0]

    mart.loc[no_hydro_index, "flow_mean_measurement_value"] = 1.0

    report = validate_risk_monthly_grid_dataframes(
        mart=mart,
        gold_grid_cell=grid,
        climate_grid_month=climate,
        hydro_grid_month=hydro,
        wildfire_grid_month=wildfire,
    )

    assert report.passed is False
    assert "gold_risk_mart_hydro_methods_and_semantics_valid" in failed_check_names(report)
