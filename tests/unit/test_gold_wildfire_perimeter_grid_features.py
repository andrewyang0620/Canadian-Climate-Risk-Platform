from __future__ import annotations

import pandas as pd
from shapely.geometry import box

from src.gold.wildfire.perimeter_grid_features import (
    build_gold_grid_month_wildfire_perimeter_feature,
)


def test_gold_wildfire_perimeter_feature_builds_grid_month_skeleton_and_overlay():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_area_sq_km": 100.0,
                "analysis_geometry_wkt": box(0, 0, 10_000, 10_000).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    wildfire_perimeters = pd.DataFrame(
        [
            {
                "wildfire_perimeter_key": "nfdb_poly__test",
                "province": "AB",
                "fire_year": 2016,
                "fire_month": 6,
                "source_size_ha": 100.0,
                "calculated_size_ha": 100.0,
                "fire_cause": "N",
                "prescribed": "",
                "geometry_wkt": box(0, 0, 1_000, 1_000).wkt,
                "geometry_is_valid": True,
                "source_crs": "EPSG:3347",
            }
        ]
    )

    result, summary = build_gold_grid_month_wildfire_perimeter_feature(
        wildfire_perimeters=wildfire_perimeters,
        grid=grid,
    )

    assert len(result) == 120
    assert summary.grid_cell_count == 1
    assert summary.month_count == 120
    assert summary.monthly_assignable_polygon_count == 1
    assert summary.missing_or_invalid_month_polygon_count == 0
    assert summary.output_nonzero_grid_month_count == 1

    june = result[result["reference_month"] == "2016-06"].iloc[0]
    assert june["wildfire_perimeter_count"] == 1
    assert june["wildfire_intersection_area_ha"] == 100.0
    assert june["wildfire_cause_n_polygon_count"] == 1
    assert june["wildfire_has_observed_perimeter_overlap"] is True or bool(
        june["wildfire_has_observed_perimeter_overlap"]
    )

    january = result[result["reference_month"] == "2016-01"].iloc[0]
    assert january["wildfire_perimeter_count"] == 0
    assert january["wildfire_intersection_area_ha"] == 0.0
    assert january["wildfire_temporal_assignment_method"] == ("no_observed_perimeter_overlap")


def test_gold_wildfire_perimeter_feature_excludes_missing_month_from_monthly_aggregation():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "bc_10km_test",
                "grid_system": "bc_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "BC",
                "analysis_area_sq_km": 100.0,
                "analysis_geometry_wkt": box(0, 0, 10_000, 10_000).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    wildfire_perimeters = pd.DataFrame(
        [
            {
                "wildfire_perimeter_key": "nfdb_poly__missing_month",
                "province": "BC",
                "fire_year": 2018,
                "fire_month": 0,
                "source_size_ha": 50.0,
                "calculated_size_ha": 50.0,
                "fire_cause": "H",
                "prescribed": "",
                "geometry_wkt": box(0, 0, 1_000, 1_000).wkt,
                "geometry_is_valid": True,
                "source_crs": "EPSG:3347",
            }
        ]
    )

    result, summary = build_gold_grid_month_wildfire_perimeter_feature(
        wildfire_perimeters=wildfire_perimeters,
        grid=grid,
    )

    assert len(result) == 120
    assert summary.feature_window_polygon_count == 1
    assert summary.monthly_assignable_polygon_count == 0
    assert summary.missing_or_invalid_month_polygon_count == 1
    assert result["wildfire_perimeter_count"].sum() == 0
    assert result["wildfire_has_observed_perimeter_overlap"].sum() == 0


def test_gold_wildfire_perimeter_feature_filters_to_ab_bc_10km_only():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_area_sq_km": 100.0,
                "analysis_geometry_wkt": box(0, 0, 10_000, 10_000).wkt,
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "calgary_1km_test",
                "grid_system": "calgary_1km",
                "grid_level": "city",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_area_sq_km": 1.0,
                "analysis_geometry_wkt": box(0, 0, 1_000, 1_000).wkt,
                "crs_epsg": 3347,
            },
        ]
    )

    wildfire_perimeters = pd.DataFrame(
        [
            {
                "wildfire_perimeter_key": "nfdb_poly__test",
                "province": "AB",
                "fire_year": 2020,
                "fire_month": 8,
                "source_size_ha": 10.0,
                "calculated_size_ha": 10.0,
                "fire_cause": "U",
                "prescribed": "",
                "geometry_wkt": box(0, 0, 500, 500).wkt,
                "geometry_is_valid": True,
                "source_crs": "EPSG:3347",
            }
        ]
    )

    result, summary = build_gold_grid_month_wildfire_perimeter_feature(
        wildfire_perimeters=wildfire_perimeters,
        grid=grid,
    )

    assert summary.grid_cell_count == 1
    assert set(result["grid_system"]) == {"ab_10km"}
    assert len(result) == 120


def test_gold_wildfire_perimeter_feature_counts_prescribed_burn_separately():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_prescribed_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_area_sq_km": 100.0,
                "analysis_geometry_wkt": box(0, 0, 10_000, 10_000).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    wildfire_perimeters = pd.DataFrame(
        [
            {
                "wildfire_perimeter_key": "nfdb_poly__prescribed",
                "province": "AB",
                "fire_year": 2021,
                "fire_month": 7,
                "source_size_ha": 25.0,
                "calculated_size_ha": 25.0,
                "fire_cause": "H-PB",
                "prescribed": "Y",
                "geometry_wkt": box(0, 0, 500, 500).wkt,
                "geometry_is_valid": True,
                "source_crs": "EPSG:3347",
            }
        ]
    )

    result, summary = build_gold_grid_month_wildfire_perimeter_feature(
        wildfire_perimeters=wildfire_perimeters,
        grid=grid,
    )

    july = result[result["reference_month"] == "2021-07"].iloc[0]

    assert summary.monthly_assignable_polygon_count == 1
    assert july["wildfire_perimeter_count"] == 1
    assert july["wildfire_cause_prescribed_burn_polygon_count"] == 1
    assert july["wildfire_cause_other_polygon_count"] == 0
    assert july["wildfire_cause_h_polygon_count"] == 0
