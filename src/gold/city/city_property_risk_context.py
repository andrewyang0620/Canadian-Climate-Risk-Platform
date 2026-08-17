from __future__ import annotations

from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import make_valid, wkt
from shapely.ops import transform
from shapely.strtree import STRtree


SOURCE_CRS = "EPSG:4326"
ANALYSIS_CRS = "EPSG:3347"

NATIONAL_RISK_RESOLUTION = "10km"
SPATIAL_ASSIGNMENT_METHOD = "max_area_overlap"

AREA_EPSILON = 1e-6

GRID_SYSTEM_BY_CITY = {
    "vancouver": "bc_10km",
    "calgary": "ab_10km",
}

REQUIRED_GRID_COLUMNS = {
    "grid_cell_key",
    "grid_system",
    "cell_size_m",
    "full_cell_geometry_wkt",
    "crs_epsg",
}


class CityPropertyRiskContextError(Exception):
    pass


def build_gold_vancouver_parcel_risk_context(
    *,
    assessment_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
    grid_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    key = "property_parcel_key"

    _require_columns(
        assessment_dataframe,
        {key, "has_latest_assessment"},
        "gold_vancouver_parcel_assessment_context",
    )
    _require_columns(
        flood_dataframe,
        {key, "geometry_wkt", "is_flood_exposed"},
        "gold_vancouver_parcel_flood_exposure",
    )

    result = _merge_serving_context(
        primary=flood_dataframe,
        secondary=assessment_dataframe,
        key=key,
        primary_name="gold_vancouver_parcel_flood_exposure",
        secondary_name="gold_vancouver_parcel_assessment_context",
    )

    result, assignment_summary = _attach_national_grid_context(
        dataframe=result,
        key=key,
        city="vancouver",
        grid_dataframe=grid_dataframe,
    )

    summary = {
        "city": "vancouver",
        "entity_key": key,
        "input_row_count": int(len(flood_dataframe)),
        "output_row_count": int(len(result)),
        "assessment_context_count": int(
            result["has_latest_assessment"].eq(True).sum()
        ),
        "flood_exposed_count": int(result["is_flood_exposed"].eq(True).sum()),
        **assignment_summary,
    }

    return result, summary


def build_gold_calgary_property_risk_context(
    *,
    assessment_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
    grid_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    key = "source_parcel_id"

    _require_columns(
        assessment_dataframe,
        {key, "assessment_year", "geometry_wkt"},
        "gold_calgary_property_location_assessment",
    )
    _require_columns(
        flood_dataframe,
        {key, "geometry_wkt", "is_flood_exposed"},
        "gold_calgary_property_location_flood_exposure",
    )

    result = _merge_serving_context(
        primary=assessment_dataframe,
        secondary=flood_dataframe,
        key=key,
        primary_name="gold_calgary_property_location_assessment",
        secondary_name="gold_calgary_property_location_flood_exposure",
    )

    result, assignment_summary = _attach_national_grid_context(
        dataframe=result,
        key=key,
        city="calgary",
        grid_dataframe=grid_dataframe,
    )

    summary = {
        "city": "calgary",
        "entity_key": key,
        "input_row_count": int(len(assessment_dataframe)),
        "output_row_count": int(len(result)),
        "assessment_year_min": int(result["assessment_year"].min()),
        "assessment_year_max": int(result["assessment_year"].max()),
        "flood_exposed_count": int(result["is_flood_exposed"].eq(True).sum()),
        **assignment_summary,
    }

    return result, summary


def _merge_serving_context(
    *,
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    key: str,
    primary_name: str,
    secondary_name: str,
) -> pd.DataFrame:
    _require_unique_key(primary, key, primary_name)
    _require_unique_key(secondary, key, secondary_name)

    primary_keys = set(primary[key].astype(str))
    secondary_keys = set(secondary[key].astype(str))

    if primary_keys != secondary_keys:
        raise CityPropertyRiskContextError(
            f"{primary_name} and {secondary_name} do not contain "
            f"the same {key} universe."
        )

    secondary_columns = [
        key,
        *[
            column
            for column in secondary.columns
            if column != key and column not in primary.columns
        ],
    ]

    return primary.merge(
        secondary[secondary_columns],
        on=key,
        how="left",
        validate="one_to_one",
    )


def _attach_national_grid_context(
    *,
    dataframe: pd.DataFrame,
    key: str,
    city: str,
    grid_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _require_columns(
        dataframe,
        {key, "geometry_wkt"},
        f"{city} property context",
    )

    grids = _select_national_grids(
        grid_dataframe=grid_dataframe,
        city=city,
    )

    grid_geometries = [
        wkt.loads(value) for value in grids["full_cell_geometry_wkt"]
    ]
    grid_keys = grids["grid_cell_key"].astype(str).tolist()

    tree = STRtree(grid_geometries)
    transformer = Transformer.from_crs(
        SOURCE_CRS,
        ANALYSIS_CRS,
        always_xy=True,
    )

    assignment_rows: list[dict[str, Any]] = []
    repaired_geometry_count = 0
    unassigned_count = 0

    for row in dataframe[[key, "geometry_wkt"]].itertuples(index=False):
        entity_key = getattr(row, key)
        geometry, repaired = _project_geometry(
            row.geometry_wkt,
            transformer,
        )

        if repaired:
            repaired_geometry_count += 1

        candidates: list[tuple[int, float]] = []

        if geometry is not None:
            entity_area = float(geometry.area)

            for index in tree.query(geometry, predicate="intersects"):
                grid_index = int(index)
                overlap = geometry.intersection(grid_geometries[grid_index])
                overlap_area = float(overlap.area)

                if overlap_area <= AREA_EPSILON:
                    continue
                
                candidates.append((grid_index, overlap_area))

        if not candidates:
            assignment_rows.append(
                {
                    key: entity_key,
                    "national_grid_cell_key": None,
                    "national_grid_candidate_count": 0,
                    "national_grid_overlap_area_sq_m": None,
                    "national_grid_overlap_ratio": None,
                    "has_national_grid_assignment": False,
                    "national_grid_assignment_geometry_repaired": repaired,
                }
            )
            unassigned_count += 1
            continue

        best_index, best_area = min(
            candidates,
            key=lambda item: (-item[1], grid_keys[item[0]]),
        )

        overlap_ratio = min(max(best_area / entity_area, 0.0), 1.0)

        assignment_rows.append(
            {
                key: entity_key,
                "national_grid_cell_key": grid_keys[best_index],
                "national_grid_candidate_count": int(len(candidates)),
                "national_grid_overlap_area_sq_m": float(best_area),
                "national_grid_overlap_ratio": float(overlap_ratio),
                "has_national_grid_assignment": True,
                "national_grid_assignment_geometry_repaired": repaired,
            }
        )

    assignments = pd.DataFrame(assignment_rows)

    result = dataframe.merge(
        assignments,
        on=key,
        how="left",
        validate="one_to_one",
    )

    result["national_risk_resolution"] = NATIONAL_RISK_RESOLUTION
    result["spatial_assignment_method"] = SPATIAL_ASSIGNMENT_METHOD
    result["national_grid_assignment_geometry"] = "full_cell_geometry_wkt"

    assigned = result["has_national_grid_assignment"].eq(True)
    ratios = result.loc[assigned, "national_grid_overlap_ratio"]
    candidate_counts = result.loc[assigned, "national_grid_candidate_count"]

    summary = {
        "national_grid_system": GRID_SYSTEM_BY_CITY[city],
        "national_risk_resolution": NATIONAL_RISK_RESOLUTION,
        "spatial_assignment_method": SPATIAL_ASSIGNMENT_METHOD,
        "national_grid_assignment_geometry": "full_cell_geometry_wkt",
        "national_grid_assigned_count": int(assigned.sum()),
        "national_grid_unassigned_count": int(unassigned_count),
        "multi_grid_candidate_count": int((candidate_counts > 1).sum()),
        "repaired_assignment_geometry_count": int(repaired_geometry_count),
        "minimum_primary_overlap_ratio": (
            float(ratios.min()) if not ratios.empty else None
        ),
        "primary_overlap_ratio_lt_0_90_count": int((ratios < 0.90).sum()),
        "primary_overlap_ratio_lt_0_99_count": int((ratios < 0.99).sum()),
    }

    return result, summary


def _select_national_grids(
    *,
    grid_dataframe: pd.DataFrame,
    city: str,
) -> pd.DataFrame:
    _require_columns(
        grid_dataframe,
        REQUIRED_GRID_COLUMNS,
        "gold_grid_cell",
    )

    grid_system = GRID_SYSTEM_BY_CITY[city]
    grids = grid_dataframe[
        grid_dataframe["grid_system"].eq(grid_system)
    ].copy()

    if grids.empty:
        raise CityPropertyRiskContextError(
            f"No national grids found for {grid_system}."
        )

    if grids["grid_cell_key"].isna().any():
        raise CityPropertyRiskContextError(
            f"{grid_system} contains null grid keys."
        )

    if grids["grid_cell_key"].duplicated().any():
        raise CityPropertyRiskContextError(
            f"{grid_system} contains duplicate grid keys."
        )

    if not grids["cell_size_m"].eq(10_000).all():
        raise CityPropertyRiskContextError(
            f"{grid_system} contains non-10km cells."
        )

    if not grids["crs_epsg"].eq(3347).all():
        raise CityPropertyRiskContextError(
            f"{grid_system} is not EPSG:3347."
        )

    return grids.reset_index(drop=True)


def _project_geometry(
    geometry_text: object,
    transformer: Transformer,
):
    if geometry_text is None or pd.isna(geometry_text):
        return None, False

    geometry = wkt.loads(str(geometry_text))

    if geometry.is_empty:
        return None, False

    repaired = False

    if not geometry.is_valid:
        geometry = make_valid(geometry)
        repaired = True

    geometry = transform(transformer.transform, geometry)

    if not geometry.is_valid:
        geometry = make_valid(geometry)
        repaired = True

    if geometry.is_empty or geometry.area <= 0:
        return None, repaired

    return geometry, repaired


def _require_unique_key(
    dataframe: pd.DataFrame,
    key: str,
    table_name: str,
) -> None:
    _require_columns(dataframe, {key}, table_name)

    if dataframe[key].isna().any():
        raise CityPropertyRiskContextError(
            f"{table_name} contains null {key}."
        )

    if dataframe[key].duplicated().any():
        raise CityPropertyRiskContextError(
            f"{table_name} contains duplicate {key}."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required: set[str],
    table_name: str,
) -> None:
    missing = required - set(dataframe.columns)

    if missing:
        raise CityPropertyRiskContextError(
            f"{table_name} missing columns: {sorted(missing)}"
        )