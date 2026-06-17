from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pyproj import CRS, Transformer
from shapely import make_valid, wkt
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


ANALYSIS_CRS_EPSG = 3347
DISPLAY_CRS_EPSG = 4326
DEFAULT_GRID_VERSION = "v1"


class GoldSpatialGridError(Exception):
    """Raised when Gold spatial-grid construction fails."""


@dataclass(frozen=True)
class GridSpec:
    """Configuration for one analytical grid system."""

    grid_system: str  # name of the grid system
    grid_level: str  # the level, such as city or prov
    cell_size_m: int
    province_key: str
    province_code: str
    province_name: str
    city_name: str | None = None
    grid_version: str = DEFAULT_GRID_VERSION


def require_analysis_crs(crs_value: str) -> int:
    """Validate that an input CRS is EPSG:3347."""
    try:
        crs = CRS.from_user_input(crs_value)
    except Exception as exc:
        raise GoldSpatialGridError("Could not parse boundary CRS.") from exc

    epsg = crs.to_epsg()

    if epsg != ANALYSIS_CRS_EPSG:
        raise GoldSpatialGridError(f"Expected EPSG:{ANALYSIS_CRS_EPSG}, got EPSG:{epsg}.")

    return epsg


def normalize_polygonal_geometry(
    geometry_value: str | BaseGeometry,
) -> tuple[BaseGeometry, bool]:
    """Parse, repair, and retain only polygonal geometry components."""
    if isinstance(geometry_value, str):
        try:
            geometry = wkt.loads(geometry_value)
        except Exception as exc:
            raise GoldSpatialGridError("Could not parse boundary WKT.") from exc
    else:
        geometry = geometry_value

    if geometry is None or geometry.is_empty:
        raise GoldSpatialGridError("Boundary geometry is empty.")

    was_repaired = not geometry.is_valid

    if was_repaired:
        geometry = make_valid(geometry)

    polygons = _polygonal_components(geometry)

    if not polygons:
        raise GoldSpatialGridError("Boundary contains no polygonal geometry.")

    polygonal_geometry = unary_union(polygons)

    if not polygonal_geometry.is_valid:
        polygonal_geometry = make_valid(polygonal_geometry)
        was_repaired = True

    polygons = _polygonal_components(polygonal_geometry)

    if not polygons:
        raise GoldSpatialGridError("Repaired boundary contains no polygonal geometry.")

    polygonal_geometry = unary_union(polygons)

    if polygonal_geometry.is_empty:
        raise GoldSpatialGridError("Normalized polygonal boundary is empty.")

    return polygonal_geometry, was_repaired


def select_municipality_boundary(
    municipality_dataframe: pd.DataFrame,
    *,
    municipality_name: str,
    province_key: str,
) -> pd.Series:
    """Select exactly one municipality using name and province abbreviation."""
    required_columns = {
        "municipality_name",
        "province_key",
        "municipality_key",
        "geometry_wkt",
        "crs",
    }

    missing_columns = required_columns - set(municipality_dataframe.columns)

    if missing_columns:
        raise GoldSpatialGridError(
            "Municipality boundary table is missing columns: " f"{sorted(missing_columns)}"
        )

    matches = municipality_dataframe[
        (municipality_dataframe["municipality_name"] == municipality_name)
        & (municipality_dataframe["province_key"] == province_key)
    ]

    if len(matches) != 1:
        raise GoldSpatialGridError(
            "Expected exactly one municipality boundary for "
            f"{municipality_name}, {province_key}; found {len(matches)}."
        )

    return matches.iloc[0]


def generate_boundary_grid(
    *,
    boundary_geometry: str | BaseGeometry,
    boundary_key: str,
    boundary_year: int,
    spec: GridSpec,
    crs_value: str,
) -> pd.DataFrame:
    """Generate stable full cells clipped to an analytical boundary."""
    require_analysis_crs(crs_value)

    normalized_boundary, boundary_repaired = normalize_polygonal_geometry(boundary_geometry)

    if spec.cell_size_m <= 0:
        raise GoldSpatialGridError("Grid cell size must be greater than zero.")

    cell_size = int(spec.cell_size_m)
    min_x, min_y, max_x, max_y = normalized_boundary.bounds

    min_x_index = math.floor(min_x / cell_size)
    max_x_index = math.ceil(max_x / cell_size) - 1
    min_y_index = math.floor(min_y / cell_size)
    max_y_index = math.ceil(max_y / cell_size) - 1

    transformer = Transformer.from_crs(
        ANALYSIS_CRS_EPSG,
        DISPLAY_CRS_EPSG,
        always_xy=True,
    )

    full_cell_area_sq_m = float(cell_size * cell_size)
    rows: list[dict[str, Any]] = []

    for y_index in range(min_y_index, max_y_index + 1):
        cell_min_y = y_index * cell_size
        cell_max_y = cell_min_y + cell_size

        for x_index in range(min_x_index, max_x_index + 1):
            cell_min_x = x_index * cell_size
            cell_max_x = cell_min_x + cell_size

            full_cell = box(
                cell_min_x,
                cell_min_y,
                cell_max_x,
                cell_max_y,
            )

            if not normalized_boundary.intersects(full_cell):
                continue

            intersection = full_cell.intersection(normalized_boundary)

            if intersection.is_empty or intersection.area <= 0:
                continue

            analysis_geometry, _ = normalize_polygonal_geometry(intersection)

            analysis_area_sq_m = float(analysis_geometry.area)

            if analysis_area_sq_m <= 0:
                continue

            coverage_ratio = analysis_area_sq_m / full_cell_area_sq_m
            coverage_ratio = min(max(coverage_ratio, 0.0), 1.0)

            centroid = full_cell.centroid
            longitude, latitude = transformer.transform(
                centroid.x,
                centroid.y,
            )

            grid_cell_key = f"{spec.grid_system}" f"_x{x_index}" f"_y{y_index}"

            rows.append(
                {
                    "grid_cell_key": grid_cell_key,
                    "grid_system": spec.grid_system,
                    "grid_level": spec.grid_level,
                    "grid_version": spec.grid_version,
                    "province_key": spec.province_key,
                    "province_code": spec.province_code,
                    "province_name": spec.province_name,
                    "city_name": spec.city_name,
                    "source_boundary_key": boundary_key,
                    "boundary_year": int(boundary_year),
                    "cell_size_m": cell_size,
                    "grid_x_index": x_index,
                    "grid_y_index": y_index,
                    "cell_min_x": float(cell_min_x),
                    "cell_min_y": float(cell_min_y),
                    "cell_max_x": float(cell_max_x),
                    "cell_max_y": float(cell_max_y),
                    "centroid_x": float(centroid.x),
                    "centroid_y": float(centroid.y),
                    "centroid_longitude": float(longitude),
                    "centroid_latitude": float(latitude),
                    "full_cell_area_sq_km": (full_cell_area_sq_m / 1_000_000),
                    "analysis_area_sq_km": (analysis_area_sq_m / 1_000_000),
                    "boundary_coverage_ratio": coverage_ratio,
                    "is_boundary_edge_cell": coverage_ratio < 0.999999,
                    "source_boundary_geometry_repaired": (boundary_repaired),
                    "full_cell_geometry_wkt": full_cell.wkt,
                    "analysis_geometry_type": (analysis_geometry.geom_type),
                    "analysis_geometry_wkt": (analysis_geometry.wkt),
                    "crs_epsg": ANALYSIS_CRS_EPSG,
                }
            )

    if not rows:
        raise GoldSpatialGridError(f"Grid system '{spec.grid_system}' produced zero cells.")

    dataframe = pd.DataFrame(rows)

    dataframe = dataframe.sort_values(["grid_system", "grid_y_index", "grid_x_index"]).reset_index(
        drop=True
    )

    if dataframe["grid_cell_key"].duplicated().any():
        raise GoldSpatialGridError(f"Grid system '{spec.grid_system}' produced duplicate keys.")

    return dataframe


def _polygonal_components(
    geometry: BaseGeometry,
) -> list[Polygon]:
    if isinstance(geometry, Polygon):
        return [geometry]

    if isinstance(geometry, MultiPolygon):
        return list(geometry.geoms)

    components: list[Polygon] = []

    if hasattr(geometry, "geoms"):
        for part in geometry.geoms:
            components.extend(_polygonal_components(part))

    return components
