import pandas as pd
from pyproj import CRS
from shapely.geometry import box

from src.gold.run_spatial_grid_foundation import (
    build_gold_grid_cell,
)


CRS_WKT = CRS.from_epsg(3347).to_wkt()


def province_row(
    *,
    province_key,
    province_code,
    province_name,
    geometry,
):
    return {
        "province_key": province_key,
        "province_code": province_code,
        "province_name": province_name,
        "boundary_year": 2021,
        "geometry_wkt": geometry.wkt,
        "crs": CRS_WKT,
    }


def municipality_row(
    *,
    municipality_key,
    municipality_name,
    province_key,
    province_code,
    province_name,
    geometry,
):
    return {
        "municipality_key": municipality_key,
        "municipality_name": municipality_name,
        "province_key": province_key,
        "province_code": province_code,
        "province_name": province_name,
        "boundary_year": 2021,
        "geometry_wkt": geometry.wkt,
        "crs": CRS_WKT,
    }


def test_build_gold_grid_cell_builds_four_grid_systems():
    province_dataframe = pd.DataFrame(
        [
            province_row(
                province_key="AB",
                province_code="48",
                province_name="Alberta",
                geometry=box(0, 0, 10_000, 10_000),
            ),
            province_row(
                province_key="BC",
                province_code="59",
                province_name="British Columbia",
                geometry=box(
                    20_000,
                    0,
                    30_000,
                    10_000,
                ),
            ),
        ]
    )

    municipality_dataframe = pd.DataFrame(
        [
            municipality_row(
                municipality_key="calgary",
                municipality_name="Calgary",
                province_key="AB",
                province_code="48",
                province_name="Alberta",
                geometry=box(0, 0, 1_000, 1_000),
            ),
            municipality_row(
                municipality_key="vancouver",
                municipality_name="Vancouver",
                province_key="BC",
                province_code="59",
                province_name="British Columbia",
                geometry=box(
                    2_000,
                    0,
                    3_000,
                    1_000,
                ),
            ),
        ]
    )

    result = build_gold_grid_cell(
        province_dataframe=province_dataframe,
        municipality_dataframe=municipality_dataframe,
    )

    assert len(result) == 4
    assert result["grid_cell_key"].is_unique

    assert set(result["grid_system"]) == {
        "ab_10km",
        "bc_10km",
        "calgary_1km",
        "vancouver_1km",
    }

    assert result.groupby("grid_system").size().to_dict() == {
        "ab_10km": 1,
        "bc_10km": 1,
        "calgary_1km": 1,
        "vancouver_1km": 1,
    }

    city_rows = result[result["grid_level"] == "city"]

    assert set(city_rows["city_name"]) == {
        "Calgary",
        "Vancouver",
    }

    assert city_rows["municipality_key"].notna().all()


def test_build_gold_grid_cell_preserves_numeric_province_codes():
    province_dataframe = pd.DataFrame(
        [
            province_row(
                province_key="AB",
                province_code=48,
                province_name="Alberta",
                geometry=box(0, 0, 10_000, 10_000),
            ),
            province_row(
                province_key="BC",
                province_code=59,
                province_name="British Columbia",
                geometry=box(
                    20_000,
                    0,
                    30_000,
                    10_000,
                ),
            ),
        ]
    )

    municipality_dataframe = pd.DataFrame(
        [
            municipality_row(
                municipality_key="calgary",
                municipality_name="Calgary",
                province_key="AB",
                province_code=48,
                province_name="Alberta",
                geometry=box(0, 0, 1_000, 1_000),
            ),
            municipality_row(
                municipality_key="vancouver",
                municipality_name="Vancouver",
                province_key="BC",
                province_code=59,
                province_name="British Columbia",
                geometry=box(
                    2_000,
                    0,
                    3_000,
                    1_000,
                ),
            ),
        ]
    )

    result = build_gold_grid_cell(
        province_dataframe=province_dataframe,
        municipality_dataframe=municipality_dataframe,
    )

    assert set(result["province_code"]) == {"48", "59"}
