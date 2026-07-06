from __future__ import annotations

from typing import Any

import pandas as pd
from shapely import wkt
from shapely.strtree import STRtree

from src.gold.spatial.grid import (
    ANALYSIS_CRS_EPSG,
    GoldSpatialGridError,
    normalize_polygonal_geometry,
    require_analysis_crs,
)


REQUIRED_GRID_COLUMNS = {
    "grid_cell_key",
    "grid_system",
    "grid_level",
    "grid_version",
    "province_key",
    "analysis_area_sq_km",
    "analysis_geometry_wkt",
    "crs_epsg",
}

REQUIRED_MUNICIPALITY_COLUMNS = {
    "municipality_key",
    "municipality_name",
    "municipality_type",
    "province_key",
    "province_code",
    "province_name",
    "boundary_year",
    "geometry_wkt",
    "crs",
}


def build_gold_grid_municipality_bridge(
    *,
    grid_dataframe: pd.DataFrame,
    municipality_dataframe: pd.DataFrame,
    progress_interval: int = 2_000,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build positive-area grid-cell and municipality intersections."""
    _require_columns(
        dataframe=grid_dataframe,
        required_columns=REQUIRED_GRID_COLUMNS,
        table_name="gold_grid_cell",
    )
    _require_columns(
        dataframe=municipality_dataframe,
        required_columns=REQUIRED_MUNICIPALITY_COLUMNS,
        table_name="silver_boundary_municipality",
    )

    if grid_dataframe["grid_cell_key"].duplicated().any():
        raise GoldSpatialGridError("Gold grid contains duplicate grid_cell_key values.")

    if municipality_dataframe["municipality_key"].duplicated().any():
        raise GoldSpatialGridError("Municipality table contains duplicate municipality_key values.")

    if set(grid_dataframe["crs_epsg"].dropna().unique()) != {ANALYSIS_CRS_EPSG}:
        raise GoldSpatialGridError("Gold grid must use EPSG:3347.")

    municipalities, repaired_count = _prepare_municipality_geometries(municipality_dataframe)

    rows: list[dict[str, Any]] = []
    processed_grid_count = 0

    for province_key, province_grids in grid_dataframe.groupby(
        "province_key",
        sort=True,
    ):
        province_municipalities = municipalities[
            municipalities["province_key"] == province_key
        ].reset_index(drop=True)

        if province_municipalities.empty:
            raise GoldSpatialGridError(f"No municipalities found for province {province_key}.")

        municipality_geometries = province_municipalities["_geometry"].tolist()

        spatial_index = STRtree(municipality_geometries)

        for grid_row in province_grids.itertuples(index=False):
            processed_grid_count += 1

            try:
                grid_geometry = wkt.loads(grid_row.analysis_geometry_wkt)
            except Exception as exc:
                raise GoldSpatialGridError(
                    "Could not parse grid analysis geometry for " f"{grid_row.grid_cell_key}."
                ) from exc

            if not grid_geometry.is_valid:
                grid_geometry, _ = normalize_polygonal_geometry(grid_geometry)

            candidate_indices = spatial_index.query(
                grid_geometry,
                predicate="intersects",
            )

            grid_area_sq_m = float(grid_geometry.area)

            if grid_area_sq_m <= 0:
                raise GoldSpatialGridError(
                    "Grid cell has non-positive analysis area: " f"{grid_row.grid_cell_key}"
                )

            for municipality_index in candidate_indices:
                municipality_row = province_municipalities.iloc[int(municipality_index)]
                municipality_geometry = municipality_geometries[int(municipality_index)]

                intersection = grid_geometry.intersection(municipality_geometry)

                intersection_area_sq_m = float(intersection.area)

                if intersection_area_sq_m <= 0:
                    continue

                municipality_area_sq_m = float(municipality_geometry.area)

                grid_coverage_ratio = min(
                    max(
                        intersection_area_sq_m / grid_area_sq_m,
                        0.0,
                    ),
                    1.0,
                )

                municipality_coverage_ratio = min(
                    max(
                        intersection_area_sq_m / municipality_area_sq_m,
                        0.0,
                    ),
                    1.0,
                )

                municipality_key = str(municipality_row["municipality_key"])

                bridge_key = f"{grid_row.grid_cell_key}" f"__{municipality_key}"

                rows.append(
                    {
                        "grid_municipality_bridge_key": (bridge_key),
                        "grid_cell_key": (grid_row.grid_cell_key),
                        "grid_system": grid_row.grid_system,
                        "grid_level": grid_row.grid_level,
                        "grid_version": grid_row.grid_version,
                        "province_key": str(province_key),
                        "municipality_key": municipality_key,
                        "municipality_name": str(municipality_row["municipality_name"]),
                        "municipality_type": (municipality_row["municipality_type"]),
                        "municipality_province_code": str(municipality_row["province_code"]),
                        "municipality_province_name": str(municipality_row["province_name"]),
                        "municipality_boundary_year": int(municipality_row["boundary_year"]),
                        "grid_analysis_area_sq_km": (grid_area_sq_m / 1_000_000),
                        "municipality_area_sq_km": (municipality_area_sq_m / 1_000_000),
                        "intersection_area_sq_km": (intersection_area_sq_m / 1_000_000),
                        "grid_coverage_ratio": (grid_coverage_ratio),
                        "municipality_coverage_ratio": (municipality_coverage_ratio),
                        "municipality_geometry_repaired": bool(
                            municipality_row["_geometry_repaired"]
                        ),
                        "spatial_join_method": ("polygon_intersection_epsg3347"),
                        "crs_epsg": ANALYSIS_CRS_EPSG,
                    }
                )

            if progress_interval > 0 and processed_grid_count % progress_interval == 0:
                print(
                    "[INFO] municipality bridge progress | "
                    f"processed_grids="
                    f"{processed_grid_count}/"
                    f"{len(grid_dataframe)} "
                    f"bridge_rows={len(rows)}"
                )

    if not rows:
        raise GoldSpatialGridError("Municipality bridge produced zero rows.")

    bridge = pd.DataFrame(rows)

    bridge = _assign_primary_municipalities(bridge)

    if bridge["grid_municipality_bridge_key"].duplicated().any():
        raise GoldSpatialGridError("Municipality bridge contains duplicate keys.")

    matched_grid_cells = set(bridge["grid_cell_key"])
    all_grid_cells = set(grid_dataframe["grid_cell_key"])
    unmatched_grid_cells = sorted(all_grid_cells - matched_grid_cells)

    match_counts = bridge.groupby("grid_cell_key").size().rename("municipality_match_count")

    bridge = bridge.merge(
        match_counts,
        left_on="grid_cell_key",
        right_index=True,
        how="left",
        validate="many_to_one",
    )

    bridge = bridge.sort_values(
        [
            "grid_system",
            "grid_cell_key",
            "is_primary_municipality",
            "municipality_key",
        ],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)

    summary = {
        "grid_count": int(len(grid_dataframe)),
        "bridge_row_count": int(len(bridge)),
        "matched_grid_cell_count": int(len(matched_grid_cells)),
        "unmatched_grid_cell_count": int(len(unmatched_grid_cells)),
        "multi_municipality_grid_count": int((match_counts > 1).sum()),
        "municipality_count": int(bridge["municipality_key"].nunique()),
        "repaired_municipality_count": int(repaired_count),
        "unmatched_grid_cell_sample": (unmatched_grid_cells[:20]),
    }

    return bridge, summary


def _prepare_municipality_geometries(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    crs_values = dataframe["crs"].dropna().unique()

    if len(crs_values) != 1:
        raise GoldSpatialGridError("Municipality table must contain exactly one CRS.")

    require_analysis_crs(str(crs_values[0]))

    prepared_rows: list[dict[str, Any]] = []
    repaired_count = 0

    for row in dataframe.to_dict(orient="records"):
        geometry, repaired = normalize_polygonal_geometry(row["geometry_wkt"])

        if repaired:
            repaired_count += 1

        prepared_rows.append(
            {
                **row,
                "_geometry": geometry,
                "_geometry_repaired": repaired,
            }
        )

    return pd.DataFrame(prepared_rows), repaired_count


def _assign_primary_municipalities(
    bridge: pd.DataFrame,
) -> pd.DataFrame:
    ordered = bridge.sort_values(
        [
            "grid_cell_key",
            "grid_coverage_ratio",
            "intersection_area_sq_km",
            "municipality_key",
        ],
        ascending=[True, False, False, True],
    ).copy()

    ordered["is_primary_municipality"] = ~ordered["grid_cell_key"].duplicated()

    return ordered


def _require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldSpatialGridError(
            f"{table_name} is missing columns: " f"{sorted(missing_columns)}"
        )
