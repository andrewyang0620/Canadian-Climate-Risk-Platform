import pandas as pd
import pytest

from src.gold.mart.risk_monthly_grid import (
    GoldRiskMartError,
    build_gold_grid_month_risk_feature_mart,
    build_grid_month_skeleton,
    prepare_climate_grid_month_features,
    prepare_hydro_grid_month_features,
    prepare_wildfire_grid_month_features,
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


def _climate_row(
    *,
    grid_cell_key,
    reference_month,
    method,
    station_count,
    mean_temp,
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
        "mean_temp_c": mean_temp if has_values else None,
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
    return pd.DataFrame(
        [
            _climate_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month="2016-01",
                method="direct_station_in_cell",
                station_count=1,
                mean_temp=-5.0,
                quality="direct",
            ),
            _climate_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month="2016-01",
                method="no_station_within_radius",
                station_count=0,
                mean_temp=None,
                quality=None,
            ),
        ]
    )


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
    return pd.DataFrame(
        [
            _hydro_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month="2016-01",
                method="basin_polygon_intersection",
            ),
            _hydro_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month="2016-01",
                method="no_hydro_coverage",
            ),
        ]
    )


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
    return pd.DataFrame(
        [
            _wildfire_row(
                grid_cell_key="ab_10km_x1_y1",
                reference_month="2016-01",
                has_overlap=True,
            ),
            _wildfire_row(
                grid_cell_key="bc_10km_x2_y2",
                reference_month="2016-01",
                has_overlap=False,
            ),
        ]
    )


def test_build_grid_month_skeleton_uses_only_10km_grids():
    result = build_grid_month_skeleton(make_grid())

    assert len(result) == 240
    assert result["grid_cell_key"].nunique() == 2
    assert result["reference_month"].nunique() == 120
    assert set(result["grid_system"]) == {"ab_10km", "bc_10km"}
    assert result["grid_month_risk_feature_key"].is_unique


def test_prepare_climate_grid_month_features_keeps_v2_methods_and_prefixes_values():
    result = prepare_climate_grid_month_features(make_climate_grid_month())

    assert len(result) == 2
    assert "climate_mean_temp_c" in result.columns
    assert "climate_daily_record_count" in result.columns
    assert result.loc[0, "climate_mapping_method"] == "direct_station_in_cell"
    assert result.loc[0, "climate_mean_temp_c"] == -5.0
    assert result.loc[1, "climate_mapping_method"] == "no_station_within_radius"
    assert pd.isna(result.loc[1, "climate_mean_temp_c"])


def test_prepare_hydro_grid_month_features_accepts_wide_v2_schema():
    result = prepare_hydro_grid_month_features(make_hydro_grid_month())

    assert len(result) == 2
    assert result.loc[0, "hydro_spatial_assignment_method"] == ("basin_polygon_intersection")
    assert result.loc[0, "flow_mean_measurement_value"] == 5.0
    assert result.loc[0, "level_mean_measurement_value"] == 0.8
    assert result.loc[0, "hydro_feature_quality_flag"] == "high"

    assert result.loc[1, "hydro_spatial_assignment_method"] == "no_hydro_coverage"
    assert result.loc[1, "flow_station_count"] == 0
    assert pd.isna(result.loc[1, "flow_mean_measurement_value"])
    assert pd.isna(result.loc[1, "hydro_feature_quality_flag"])


def test_prepare_wildfire_grid_month_features_keeps_zero_semantics():
    result = prepare_wildfire_grid_month_features(make_wildfire_grid_month())

    assert len(result) == 2
    assert "wildfire_crs_epsg" in result.columns
    assert "wildfire_grid_analysis_area_sq_km" in result.columns

    assert result.loc[0, "wildfire_temporal_assignment_method"] == "polygon_fire_month"
    assert result.loc[0, "wildfire_perimeter_count"] == 1
    assert result.loc[0, "wildfire_intersection_area_ha"] == 200.0

    assert result.loc[1, "wildfire_temporal_assignment_method"] == ("no_observed_perimeter_overlap")
    assert result.loc[1, "wildfire_perimeter_count"] == 0
    assert result.loc[1, "wildfire_intersection_area_ha"] == 0.0


def test_prepare_hydro_grid_month_features_rejects_duplicate_grid_month_rows():
    hydro = pd.concat(
        [make_hydro_grid_month(), make_hydro_grid_month().iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(
        GoldRiskMartError,
        match="duplicate grid_cell_key",
    ):
        prepare_hydro_grid_month_features(hydro)


def test_build_gold_grid_month_risk_feature_mart_preserves_skeleton_rows():
    mart, summary = build_gold_grid_month_risk_feature_mart(
        grid=make_grid(),
        municipality_bridge=make_municipality_bridge(),
        climate_grid_month=make_climate_grid_month(),
        hydro_grid_month=make_hydro_grid_month(),
        wildfire_grid_month=make_wildfire_grid_month(),
    )

    assert len(mart) == 240
    assert summary["row_count"] == 240
    assert summary["grid_cell_count"] == 2
    assert summary["month_count"] == 120

    target = mart[
        (mart["grid_cell_key"] == "ab_10km_x1_y1") & (mart["reference_month"] == "2016-01")
    ].iloc[0]

    assert target["primary_municipality_name"] == "Calgary"
    assert bool(target["has_climate_feature"]) is True
    assert bool(target["has_hydro_spatial_coverage"]) is True
    assert bool(target["has_hydro_flow_feature"]) is True
    assert bool(target["has_hydro_level_feature"]) is True
    assert bool(target["has_hydro_feature"]) is True
    assert bool(target["has_wildfire_perimeter_feature"]) is True
    assert bool(target["has_wildfire_observed_perimeter_overlap"]) is True

    assert target["climate_station_count"] == 1
    assert target["climate_mean_temp_c"] == -5.0
    assert target["hydro_spatial_assignment_method"] == "basin_polygon_intersection"
    assert target["flow_mean_measurement_value"] == 5.0
    assert target["level_mean_measurement_value"] == 0.8
    assert target["wildfire_perimeter_count"] == 1

    no_coverage = mart[
        (mart["grid_cell_key"] == "bc_10km_x2_y2") & (mart["reference_month"] == "2016-01")
    ].iloc[0]

    assert bool(no_coverage["has_climate_feature"]) is False
    assert bool(no_coverage["has_hydro_spatial_coverage"]) is False
    assert bool(no_coverage["has_hydro_flow_feature"]) is False
    assert bool(no_coverage["has_hydro_level_feature"]) is False
    assert bool(no_coverage["has_hydro_feature"]) is False
    assert bool(no_coverage["has_wildfire_perimeter_feature"]) is True
    assert bool(no_coverage["has_wildfire_observed_perimeter_overlap"]) is False

    assert no_coverage["climate_mapping_method"] == "no_station_within_radius"
    assert pd.isna(no_coverage["climate_mean_temp_c"])
    assert no_coverage["hydro_spatial_assignment_method"] == "no_hydro_coverage"
    assert pd.isna(no_coverage["flow_mean_measurement_value"])
    assert no_coverage["wildfire_perimeter_count"] == 0


def test_build_gold_grid_month_risk_feature_mart_rejects_duplicate_climate_rows():
    climate = pd.concat(
        [make_climate_grid_month(), make_climate_grid_month().iloc[[0]]],
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
            wildfire_grid_month=make_wildfire_grid_month(),
        )


def test_build_gold_grid_month_risk_feature_mart_rejects_duplicate_wildfire_rows():
    wildfire = pd.concat(
        [make_wildfire_grid_month(), make_wildfire_grid_month().iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(
        GoldRiskMartError,
        match="duplicate grid_cell_key",
    ):
        build_gold_grid_month_risk_feature_mart(
            grid=make_grid(),
            municipality_bridge=make_municipality_bridge(),
            climate_grid_month=make_climate_grid_month(),
            hydro_grid_month=make_hydro_grid_month(),
            wildfire_grid_month=wildfire,
        )
