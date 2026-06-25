import pandas as pd
from pyproj import CRS
from shapely.geometry import box

from src.gold.spatial.municipality_bridge import (
    build_gold_grid_municipality_bridge,
)
from src.gold.spatial.validation import (
    validate_spatial_grid_foundation_dataframes,
)


CRS_WKT = CRS.from_epsg(3347).to_wkt()


def make_inputs():
    ab_boundary = box(
        0,
        0,
        10_000,
        10_000,
    )
    bc_boundary = box(
        20_000,
        0,
        30_000,
        10_000,
    )

    calgary = box(
        0,
        0,
        1_000,
        1_000,
    )
    vancouver = box(
        20_000,
        0,
        21_000,
        1_000,
    )

    province = pd.DataFrame(
        [
            {
                "province_key": "AB",
                "geometry_wkt": ab_boundary.wkt,
            },
            {
                "province_key": "BC",
                "geometry_wkt": bc_boundary.wkt,
            },
        ]
    )

    municipality = pd.DataFrame(
        [
            municipality_row(
                key="calgary",
                name="Calgary",
                province_key="AB",
                geometry=calgary,
            ),
            municipality_row(
                key="ab_remainder",
                name="Alberta Remainder",
                province_key="AB",
                geometry=ab_boundary.difference(calgary),
            ),
            municipality_row(
                key="vancouver",
                name="Vancouver",
                province_key="BC",
                geometry=vancouver,
            ),
            municipality_row(
                key="bc_remainder",
                name="BC Remainder",
                province_key="BC",
                geometry=bc_boundary.difference(vancouver),
            ),
        ]
    )

    grid = pd.DataFrame(
        [
            grid_row(
                key="ab_10km_x0_y0",
                system="ab_10km",
                level="province",
                province_key="AB",
                city_name=None,
                size=10_000,
                x_index=0,
                y_index=0,
                geometry=ab_boundary,
            ),
            grid_row(
                key="bc_10km_x2_y0",
                system="bc_10km",
                level="province",
                province_key="BC",
                city_name=None,
                size=10_000,
                x_index=2,
                y_index=0,
                geometry=bc_boundary,
            ),
            grid_row(
                key="calgary_1km_x0_y0",
                system="calgary_1km",
                level="city",
                province_key="AB",
                city_name="Calgary",
                size=1_000,
                x_index=0,
                y_index=0,
                geometry=calgary,
            ),
            grid_row(
                key="vancouver_1km_x20_y0",
                system="vancouver_1km",
                level="city",
                province_key="BC",
                city_name="Vancouver",
                size=1_000,
                x_index=20,
                y_index=0,
                geometry=vancouver,
            ),
        ]
    )

    bridge, _ = build_gold_grid_municipality_bridge(
        grid_dataframe=grid,
        municipality_dataframe=municipality,
        progress_interval=0,
    )

    return grid, bridge, province, municipality


def grid_row(
    *,
    key,
    system,
    level,
    province_key,
    city_name,
    size,
    x_index,
    y_index,
    geometry,
):
    area_sq_km = geometry.area / 1_000_000

    return {
        "grid_cell_key": key,
        "grid_system": system,
        "grid_level": level,
        "grid_version": "v1",
        "province_key": province_key,
        "city_name": city_name,
        "cell_size_m": size,
        "grid_x_index": x_index,
        "grid_y_index": y_index,
        "cell_min_x": x_index * size,
        "cell_min_y": y_index * size,
        "analysis_area_sq_km": area_sq_km,
        "full_cell_area_sq_km": area_sq_km,
        "boundary_coverage_ratio": 1.0,
        "is_boundary_edge_cell": False,
        "full_cell_geometry_wkt": geometry.wkt,
        "analysis_geometry_wkt": geometry.wkt,
        "crs_epsg": 3347,
    }


def municipality_row(
    *,
    key,
    name,
    province_key,
    geometry,
):
    return {
        "municipality_key": key,
        "municipality_name": name,
        "municipality_type": "TEST",
        "province_key": province_key,
        "province_code": ("48" if province_key == "AB" else "59"),
        "province_name": ("Alberta" if province_key == "AB" else "British Columbia"),
        "boundary_year": 2021,
        "geometry_wkt": geometry.wkt,
        "crs": CRS_WKT,
    }


def test_spatial_grid_foundation_validation_passes():
    grid, bridge, province, municipality = make_inputs()

    report = validate_spatial_grid_foundation_dataframes(
        grid=grid,
        bridge=bridge,
        province=province,
        municipality=municipality,
    )

    assert report.passed is True
    assert len(report.checks) > 10


def test_spatial_grid_foundation_validation_detects_bad_coverage():
    grid, bridge, province, municipality = make_inputs()

    bridge.loc[
        bridge.index[0],
        "grid_coverage_ratio",
    ] = 0.25

    report = validate_spatial_grid_foundation_dataframes(
        grid=grid,
        bridge=bridge,
        province=province,
        municipality=municipality,
    )

    assert report.passed is False

    failed_names = {check.name for check in report.checks if not check.passed}

    assert "gold_grid_municipality_bridge_area_conserved" in failed_names
