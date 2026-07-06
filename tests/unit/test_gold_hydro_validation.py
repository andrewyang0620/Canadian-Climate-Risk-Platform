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


def make_grid_cell():
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_basin",
                "grid_system": "ab_10km",
                "province_key": "AB",
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "bc_10km_point",
                "grid_system": "bc_10km",
                "province_key": "BC",
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "bc_10km_none",
                "grid_system": "bc_10km",
                "province_key": "BC",
                "crs_epsg": 3347,
            },
        ]
    )


def _grid_month_row(
    *,
    month,
    grid_cell_key,
    grid_system,
    province_key,
    method,
):
    days = pd.Period(month, freq="M").days_in_month

    if method == "basin_polygon_intersection":
        hydro_station_count = 1
        hydro_basin_station_count = 1
        hydro_point_station_count = 0
        basin_area = 50.0
        basin_ratio = 0.5
        quality = "high"
    elif method == "station_point_in_cell":
        hydro_station_count = 1
        hydro_basin_station_count = 0
        hydro_point_station_count = 1
        basin_area = None
        basin_ratio = None
        quality = "medium"
    else:
        hydro_station_count = 0
        hydro_basin_station_count = 0
        hydro_point_station_count = 0
        basin_area = None
        basin_ratio = None
        quality = None

    has_values = method != "no_hydro_coverage"

    return {
        "grid_month_hydro_feature_key": f"{grid_cell_key}__{month}",
        "grid_cell_key": grid_cell_key,
        "grid_system": grid_system,
        "grid_level": "province",
        "grid_version": "v1",
        "province_key": province_key,
        "reference_month": month,
        "hydro_spatial_assignment_method": method,
        "hydro_station_count": hydro_station_count,
        "hydro_basin_station_count": hydro_basin_station_count,
        "hydro_point_station_count": hydro_point_station_count,
        "hydro_basin_intersection_area_sq_km": basin_area,
        "hydro_basin_grid_coverage_ratio": basin_ratio,
        "flow_station_count": 1 if has_values else 0,
        "flow_daily_record_count": days if has_values else 0,
        "flow_observation_day_count": days if has_values else 0,
        "flow_measurement_observation_count": days if has_values else 0,
        "flow_mean_measurement_value": 1.0 if has_values else None,
        "flow_min_measurement_value": 0.0 if has_values else None,
        "flow_max_measurement_value": 2.0 if has_values else None,
        "flow_median_measurement_value": 1.0 if has_values else None,
        "flow_p95_measurement_value": 1.9 if has_values else None,
        "flow_measurement_completeness_ratio": 1.0 if has_values else None,
        "flow_zero_day_count": 1 if has_values else 0,
        "flow_negative_value_count": 0,
        "level_station_count": 1 if has_values else 0,
        "level_daily_record_count": days if has_values else 0,
        "level_observation_day_count": days if has_values else 0,
        "level_measurement_observation_count": days if has_values else 0,
        "level_mean_measurement_value": 0.5 if has_values else None,
        "level_min_measurement_value": -0.5 if has_values else None,
        "level_max_measurement_value": 1.5 if has_values else None,
        "level_median_measurement_value": 0.5 if has_values else None,
        "level_p95_measurement_value": 1.4 if has_values else None,
        "level_measurement_completeness_ratio": 1.0 if has_values else None,
        "level_negative_value_count": 1 if has_values else 0,
        "hydro_data_completeness_score": 1.0 if has_values else None,
        "hydro_feature_quality_flag": quality,
    }


def make_grid_month():
    rows = []

    grid_specs = [
        (
            "AB",
            "ab_10km_basin",
            "ab_10km",
            "basin_polygon_intersection",
        ),
        (
            "BC",
            "bc_10km_point",
            "bc_10km",
            "station_point_in_cell",
        ),
        (
            "BC",
            "bc_10km_none",
            "bc_10km",
            "no_hydro_coverage",
        ),
    ]

    for month in pd.period_range("2016-01", "2025-12", freq="M").astype(str):
        for province_key, grid_cell_key, grid_system, method in grid_specs:
            rows.append(
                _grid_month_row(
                    month=month,
                    grid_cell_key=grid_cell_key,
                    grid_system=grid_system,
                    province_key=province_key,
                    method=method,
                )
            )

    return pd.DataFrame(rows)


def failed_check_names(report):
    return {check.name for check in report.checks if not check.passed}


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
    assert "gold_hydro_station_month_key_valid" in failed_check_names(report)


def test_hydro_monthly_validation_rejects_negative_flow():
    station_month = make_station_month()
    flow_index = station_month[station_month["measurement_type"] == "flow"].index[0]

    station_month.loc[flow_index, "negative_value_count"] = 1
    station_month.loc[flow_index, "min_measurement_value"] = -1.0

    report = validate_hydro_monthly_feature_dataframes(
        station_month=station_month,
    )

    assert report.passed is False
    assert "gold_hydro_station_month_flow_nonnegative" in failed_check_names(report)


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
    assert "gold_hydro_station_month_measurement_types_valid" in failed_check_names(report)


def test_hydro_monthly_validation_passes_valid_station_and_grid_month_v2():
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
    assert "gold_grid_month_hydro_grid_keys_and_skeleton_valid" in failed_check_names(report)


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
    assert "gold_grid_month_hydro_key_valid" in failed_check_names(report)


def test_hydro_monthly_validation_detects_no_coverage_values():
    grid_month = make_grid_month()
    no_coverage_index = grid_month[
        grid_month["hydro_spatial_assignment_method"] == "no_hydro_coverage"
    ].index[0]

    grid_month.loc[no_coverage_index, "flow_mean_measurement_value"] = 1.0

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False
    assert "gold_grid_month_hydro_no_coverage_semantics_valid" in failed_check_names(report)


def test_hydro_monthly_validation_detects_point_high_quality():
    grid_month = make_grid_month()
    point_index = grid_month[
        grid_month["hydro_spatial_assignment_method"] == "station_point_in_cell"
    ].index[0]

    grid_month.loc[point_index, "hydro_feature_quality_flag"] = "high"

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False
    assert "gold_grid_month_hydro_point_in_cell_semantics_valid" in failed_check_names(report)


def test_hydro_monthly_validation_detects_invalid_basin_ratio():
    grid_month = make_grid_month()
    basin_index = grid_month[
        grid_month["hydro_spatial_assignment_method"] == "basin_polygon_intersection"
    ].index[0]

    grid_month.loc[basin_index, "hydro_basin_grid_coverage_ratio"] = 1.5

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False
    assert "gold_grid_month_hydro_basin_semantics_valid" in failed_check_names(report)


def test_hydro_monthly_validation_detects_invalid_quality_flag():
    grid_month = make_grid_month()
    grid_month.loc[0, "hydro_feature_quality_flag"] = "bad_quality"

    report = validate_hydro_monthly_feature_dataframes(
        station_month=make_station_month(),
        grid_month=grid_month,
        gold_grid_cell=make_grid_cell(),
    )

    assert report.passed is False
    assert "gold_grid_month_hydro_quality_flags_valid" in failed_check_names(report)
