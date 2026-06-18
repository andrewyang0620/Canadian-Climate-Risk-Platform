import pandas as pd
from pyproj import CRS
from shapely.geometry import box

from src.gold.grid_municipality_bridge import (
    build_gold_grid_municipality_bridge,
)


CRS_WKT = CRS.from_epsg(3347).to_wkt()


def test_build_grid_municipality_bridge_assigns_primary():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "grid_1",
                "grid_system": "test_1km",
                "grid_level": "city",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_area_sq_km": 1.0,
                "analysis_geometry_wkt": (box(0, 0, 1_000, 1_000).wkt),
                "crs_epsg": 3347,
            }
        ]
    )

    municipalities = pd.DataFrame(
        [
            {
                "municipality_key": "left",
                "municipality_name": "Left",
                "municipality_type": "CY",
                "province_key": "AB",
                "province_code": "48",
                "province_name": "Alberta",
                "boundary_year": 2021,
                "geometry_wkt": (box(0, 0, 600, 1_000).wkt),
                "crs": CRS_WKT,
            },
            {
                "municipality_key": "right",
                "municipality_name": "Right",
                "municipality_type": "CY",
                "province_key": "AB",
                "province_code": "48",
                "province_name": "Alberta",
                "boundary_year": 2021,
                "geometry_wkt": (box(600, 0, 1_000, 1_000).wkt),
                "crs": CRS_WKT,
            },
        ]
    )

    bridge, summary = build_gold_grid_municipality_bridge(
        grid_dataframe=grid,
        municipality_dataframe=municipalities,
        progress_interval=0,
    )

    assert len(bridge) == 2
    assert summary["matched_grid_cell_count"] == 1
    assert summary["multi_municipality_grid_count"] == 1

    primary = bridge[bridge["is_primary_municipality"]].iloc[0]

    assert primary["municipality_key"] == "left"
    assert primary["grid_coverage_ratio"] == 0.6

    assert bridge["intersection_area_sq_km"].sum() == 1.0


def test_bridge_tracks_unmatched_grid_cells():
    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "matched",
                "grid_system": "test_1km",
                "grid_level": "city",
                "grid_version": "v1",
                "province_key": "BC",
                "analysis_area_sq_km": 1.0,
                "analysis_geometry_wkt": (box(0, 0, 1_000, 1_000).wkt),
                "crs_epsg": 3347,
            },
            {
                "grid_cell_key": "unmatched",
                "grid_system": "test_1km",
                "grid_level": "city",
                "grid_version": "v1",
                "province_key": "BC",
                "analysis_area_sq_km": 1.0,
                "analysis_geometry_wkt": (box(2_000, 0, 3_000, 1_000).wkt),
                "crs_epsg": 3347,
            },
        ]
    )

    municipalities = pd.DataFrame(
        [
            {
                "municipality_key": "municipality",
                "municipality_name": "Municipality",
                "municipality_type": "CY",
                "province_key": "BC",
                "province_code": "59",
                "province_name": "British Columbia",
                "boundary_year": 2021,
                "geometry_wkt": (box(0, 0, 1_000, 1_000).wkt),
                "crs": CRS_WKT,
            }
        ]
    )

    _, summary = build_gold_grid_municipality_bridge(
        grid_dataframe=grid,
        municipality_dataframe=municipalities,
        progress_interval=0,
    )

    assert summary["matched_grid_cell_count"] == 1
    assert summary["unmatched_grid_cell_count"] == 1
    assert summary["unmatched_grid_cell_sample"] == ["unmatched"]
