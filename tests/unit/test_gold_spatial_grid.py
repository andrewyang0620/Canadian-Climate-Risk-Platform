import pandas as pd
import pytest
from pyproj import CRS
from shapely.geometry import Polygon, box

from src.gold.spatial_grid import (
    GoldSpatialGridError,
    GridSpec,
    generate_boundary_grid,
    normalize_polygonal_geometry,
    require_analysis_crs,
    select_municipality_boundary,
)


def test_require_analysis_crs_accepts_epsg_3347():
    crs_wkt = CRS.from_epsg(3347).to_wkt()

    assert require_analysis_crs(crs_wkt) == 3347


def test_require_analysis_crs_rejects_epsg_4326():
    crs_wkt = CRS.from_epsg(4326).to_wkt()

    with pytest.raises(GoldSpatialGridError):
        require_analysis_crs(crs_wkt)


def test_select_municipality_boundary_uses_province_key():
    dataframe = pd.DataFrame(
        [
            {
                "municipality_key": "calgary_ab",
                "municipality_name": "Calgary",
                "province_key": "AB",
                "geometry_wkt": box(0, 0, 1, 1).wkt,
                "crs": CRS.from_epsg(3347).to_wkt(),
            },
            {
                "municipality_key": "calgary_other",
                "municipality_name": "Calgary",
                "province_key": "XX",
                "geometry_wkt": box(0, 0, 1, 1).wkt,
                "crs": CRS.from_epsg(3347).to_wkt(),
            },
        ]
    )

    result = select_municipality_boundary(
        dataframe,
        municipality_name="Calgary",
        province_key="AB",
    )

    assert result["municipality_key"] == "calgary_ab"


def test_normalize_polygonal_geometry_repairs_invalid_polygon():
    invalid_polygon = Polygon(
        [
            (0, 0),
            (2, 2),
            (0, 2),
            (2, 0),
            (0, 0),
        ]
    )

    result, repaired = normalize_polygonal_geometry(invalid_polygon)

    assert repaired is True
    assert result.is_valid
    assert result.geom_type in {"Polygon", "MultiPolygon"}
    assert result.area > 0


def test_generate_boundary_grid_uses_stable_global_alignment():
    boundary = box(500, 500, 2500, 2500)

    spec = GridSpec(
        grid_system="test_1km",
        grid_level="test",
        cell_size_m=1000,
        province_key="AB",
        province_code="48",
        province_name="Alberta",
    )

    dataframe = generate_boundary_grid(
        boundary_geometry=boundary,
        boundary_key="test_boundary",
        boundary_year=2021,
        spec=spec,
        crs_value=CRS.from_epsg(3347).to_wkt(),
    )

    assert len(dataframe) == 9
    assert dataframe["grid_cell_key"].is_unique
    assert set(dataframe["grid_x_index"]) == {0, 1, 2}
    assert set(dataframe["grid_y_index"]) == {0, 1, 2}

    assert dataframe["analysis_area_sq_km"].sum() == pytest.approx(4.0)

    assert dataframe["full_cell_area_sq_km"].eq(1.0).all()

    assert dataframe["boundary_coverage_ratio"].between(0, 1, inclusive="both").all()


def test_grid_keys_are_repeatable():
    boundary = box(0, 0, 2000, 1000)

    spec = GridSpec(
        grid_system="repeatable_1km",
        grid_level="test",
        cell_size_m=1000,
        province_key="BC",
        province_code="59",
        province_name="British Columbia",
    )

    first = generate_boundary_grid(
        boundary_geometry=boundary,
        boundary_key="boundary",
        boundary_year=2021,
        spec=spec,
        crs_value=CRS.from_epsg(3347).to_wkt(),
    )

    second = generate_boundary_grid(
        boundary_geometry=boundary,
        boundary_key="boundary",
        boundary_year=2021,
        spec=spec,
        crs_value=CRS.from_epsg(3347).to_wkt(),
    )

    assert first["grid_cell_key"].tolist() == second["grid_cell_key"].tolist()
