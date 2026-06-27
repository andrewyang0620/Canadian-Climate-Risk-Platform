import pandas as pd
import pytest

from src.gold.mart.risk_monthly_grid import (
    GoldRiskMartError,
    build_gold_grid_month_risk_feature_mart,
    build_grid_month_skeleton,
    pivot_hydro_grid_month_features,
)


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
            {
                "grid_cell_key": "calgary_1km_x1_y1",
                "grid_system": "calgary_1km",
                "grid_level": "city",
                "grid_version": "v1",
                "province_key": "AB",
                "province_code": "48",
                "province_name": "Alberta",
                "boundary_year": 2021,
                "cell_size_m": 1000,
                "grid_x_index": 1,
                "grid_y_index": 1,
                "centroid_longitude": -114.1,
                "centroid_latitude": 51.1,
                "full_cell_area_sq_km": 1.0,
                "analysis_area_sq_km": 1.0,
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


def test_build_grid_month_skeleton_uses_only_10km_grids():
    result = build_grid_month_skeleton(make_grid())

    assert len(result) == 240
    assert result["grid_cell_key"].nunique() == 2
    assert result["reference_month"].nunique() == 120
    assert set(result["grid_system"]) == {"ab_10km", "bc_10km"}
    assert result["grid_month_risk_feature_key"].is_unique


def test_pivot_hydro_grid_month_features_widens_flow_and_level():
    result = pivot_hydro_grid_month_features(make_hydro_grid_month())

    assert len(result) == 1
    assert result.loc[0, "flow_mean_measurement_value"] == 5.0
    assert result.loc[0, "level_mean_measurement_value"] == 0.8
    assert result.loc[0, "flow_feature_quality_flag"] == "high"
    assert result.loc[0, "level_feature_quality_flag"] == "high"


def test_pivot_hydro_grid_month_features_rejects_duplicate_type_rows():
    hydro = pd.concat(
        [make_hydro_grid_month(), make_hydro_grid_month().iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(
        GoldRiskMartError,
        match="duplicate grid_cell_key",
    ):
        pivot_hydro_grid_month_features(hydro)


def test_build_gold_grid_month_risk_feature_mart_preserves_skeleton_rows():
    mart, summary = build_gold_grid_month_risk_feature_mart(
        grid=make_grid(),
        municipality_bridge=make_municipality_bridge(),
        climate_grid_month=make_climate_grid_month(),
        hydro_grid_month=make_hydro_grid_month(),
    )

    assert len(mart) == 240
    assert summary["row_count"] == 240
    assert summary["grid_cell_count"] == 2
    assert summary["month_count"] == 120

    target = mart[
        (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    ].iloc[0]

    assert target["primary_municipality_name"] == "Calgary"
    assert target["has_climate_feature"] == True
    assert target["has_hydro_flow_feature"] == True
    assert target["has_hydro_level_feature"] == True
    assert target["climate_station_count"] == 1
    assert target["flow_mean_measurement_value"] == 5.0
    assert target["level_mean_measurement_value"] == 0.8

    missing = mart[
        (mart["grid_cell_key"] == "bc_10km_x2_y2") & (mart["reference_month"] == "2016-01")
    ].iloc[0]

    assert missing["has_climate_feature"] == False
    assert missing["has_hydro_flow_feature"] == False
    assert missing["has_hydro_level_feature"] == False


def test_build_gold_grid_month_risk_feature_mart_rejects_duplicate_climate_rows():
    climate = pd.concat(
        [make_climate_grid_month(), make_climate_grid_month()],
        ignore_index=True,
    )

    with pytest.raises(
        GoldRiskMartError,
        match="duplicate grid_cell_key",
    ):
        build_gold_grid_month_risk_feature_mart(
            grid=make_grid(),
            municipality_bridge=make_municipality_bridge(),
            climate_grid_month=climate,
            hydro_grid_month=make_hydro_grid_month(),
        )
