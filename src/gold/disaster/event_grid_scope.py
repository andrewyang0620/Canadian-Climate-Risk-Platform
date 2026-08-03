from __future__ import annotations

import json
from typing import Any

import pandas as pd
from shapely import wkt
from shapely.prepared import prep


TABLE_NAME = "gold_disaster_event_grid_scope"
TARGET_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
EXPECTED_CRS_EPSG = 3347


class GoldDisasterEventGridScopeError(Exception):
    """Raised when disaster event grid scope construction fails."""


def build_gold_disaster_event_grid_scope(
    *,
    event_cd_scope: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _validate_inputs(
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_spatial_reference,
        grid_cell=grid_cell,
    )

    events = event_cd_scope.copy()
    events["resolved_census_division_key"] = events["resolved_census_division_key"].astype(str)

    grids = grid_cell[grid_cell["grid_system"].isin(TARGET_GRID_SYSTEMS)].copy()

    grids["grid_cell_key"] = grids["grid_cell_key"].astype(str)
    grids["province_key"] = grids["province_key"].astype(str)

    relevant_cd_keys = set(events["resolved_census_division_key"])

    cds = cd_spatial_reference[
        cd_spatial_reference["census_division_key"].astype(str).isin(relevant_cd_keys)
    ].copy()

    cds["census_division_key"] = cds["census_division_key"].astype(str)
    cds["province_key"] = cds["province_key"].astype(str)

    available_cd_keys = set(cds["census_division_key"])
    missing_cd_keys = sorted(relevant_cd_keys - available_cd_keys)

    if missing_cd_keys:
        raise GoldDisasterEventGridScopeError(
            "Event CD scope contains keys missing from CD spatial reference: " f"{missing_cd_keys}"
        )

    grid_geometries = {
        str(row.grid_cell_key): wkt.loads(str(row.analysis_geometry_wkt))
        for row in grids[["grid_cell_key", "analysis_geometry_wkt"]].itertuples(index=False)
    }

    grid_cd_bridge_rows: list[dict[str, Any]] = []

    for cd_row in cds.itertuples(index=False):
        cd_key = str(cd_row.census_division_key)
        cd_province = str(cd_row.province_key)
        cd_geometry = wkt.loads(str(cd_row.geometry_wkt))
        prepared_cd = prep(cd_geometry)

        min_x, min_y, max_x, max_y = cd_geometry.bounds

        candidates = grids[
            grids["province_key"].eq(cd_province)
            & grids["cell_max_x"].gt(min_x)
            & grids["cell_min_x"].lt(max_x)
            & grids["cell_max_y"].gt(min_y)
            & grids["cell_min_y"].lt(max_y)
        ]

        for grid_row in candidates.itertuples(index=False):
            grid_key = str(grid_row.grid_cell_key)
            grid_geometry = grid_geometries[grid_key]

            if not prepared_cd.intersects(grid_geometry):
                continue

            intersection = cd_geometry.intersection(grid_geometry)
            intersection_area_m2 = float(intersection.area)

            # Excludes boundary-only touches.
            if intersection_area_m2 <= 0.0:
                continue

            grid_geometry_area_m2 = float(grid_geometry.area)

            if grid_geometry_area_m2 <= 0.0:
                raise GoldDisasterEventGridScopeError(
                    f"Grid geometry has non-positive area: {grid_key}"
                )

            grid_cd_bridge_rows.append(
                {
                    "resolved_census_division_key": cd_key,
                    "census_division_province_key": cd_province,
                    "grid_cell_key": grid_key,
                    "grid_system": str(grid_row.grid_system),
                    "grid_province_key": str(grid_row.province_key),
                    "grid_analysis_area_sq_km": float(grid_row.analysis_area_sq_km),
                    "grid_geometry_area_sq_km": (grid_geometry_area_m2 / 1_000_000.0),
                    "intersection_area_sq_km": (intersection_area_m2 / 1_000_000.0),
                    "single_cd_grid_coverage_ratio": min(
                        intersection_area_m2 / grid_geometry_area_m2,
                        1.0,
                    ),
                }
            )

    grid_cd_bridge = pd.DataFrame(grid_cd_bridge_rows)

    if grid_cd_bridge.empty:
        raise GoldDisasterEventGridScopeError(
            "No positive-area grid-CD intersections were produced."
        )

    bridge_cd_keys = set(grid_cd_bridge["resolved_census_division_key"].astype(str))
    unmatched_cd_keys = sorted(relevant_cd_keys - bridge_cd_keys)

    if unmatched_cd_keys:
        raise GoldDisasterEventGridScopeError(
            "Some event CD keys produced no grid intersections: " f"{unmatched_cd_keys}"
        )

    expanded = events.merge(
        grid_cd_bridge,
        on=[
            "resolved_census_division_key",
            "census_division_province_key",
        ],
        how="inner",
        validate="many_to_many",
    )

    if expanded.empty:
        raise GoldDisasterEventGridScopeError(
            "Event-CD to grid intersection join produced no rows."
        )

    expected_event_cd_keys = set(events["event_cd_scope_key"].astype(str))
    observed_event_cd_keys = set(expanded["event_cd_scope_key"].astype(str))

    missing_event_cd_keys = sorted(expected_event_cd_keys - observed_event_cd_keys)

    if missing_event_cd_keys:
        raise GoldDisasterEventGridScopeError(
            "Some event-CD scope rows produced no grid rows: " f"{missing_event_cd_keys}"
        )

    rows_by_event_grid: dict[tuple[str, str], dict[str, Any]] = {}

    for row in expanded.itertuples(index=False):
        event_key = str(row.disaster_event_reference_key)
        grid_key = str(row.grid_cell_key)
        row_key = (event_key, grid_key)

        if row_key not in rows_by_event_grid:
            rows_by_event_grid[row_key] = {
                "event_grid_scope_key": (f"disaster_event_grid_scope__{event_key}__{grid_key}"),
                "disaster_event_reference_key": event_key,
                "source_disaster_event_key": _nullable_str(row.source_disaster_event_key),
                "reference_month": str(row.reference_month),
                "event_year": int(row.event_year),
                "event_month_number": int(row.event_month_number),
                "province_key": str(row.province_key),
                "disaster_domain": str(row.disaster_domain),
                "location_text": _nullable_str(row.location_text),
                "location_tier": _nullable_str(row.location_tier),
                "grid_cell_key": grid_key,
                "grid_system": str(row.grid_system),
                "grid_province_key": str(row.grid_province_key),
                "grid_analysis_area_sq_km": float(row.grid_analysis_area_sq_km),
                "grid_geometry_area_sq_km": float(row.grid_geometry_area_sq_km),
                "matched_census_division_keys": [],
                "source_event_cd_scope_keys": [],
                "source_mapped_geo_levels": [],
                "resolution_methods": [],
                "mapping_confidences": [],
                "mapping_methods": [],
                "affected_overlap_area_sq_km": 0.0,
                "maximum_single_cd_coverage_ratio": 0.0,
                "is_csd_to_cd_approximation": False,
                "is_backtest_window": bool(row.is_backtest_window),
                "is_ab_bc_scope": bool(row.is_ab_bc_scope),
                "is_domain_relevant": bool(row.is_domain_relevant),
                "is_grid_backtest_eligible": bool(row.is_grid_backtest_eligible),
            }

        output_row = rows_by_event_grid[row_key]

        output_row["matched_census_division_keys"].append(str(row.resolved_census_division_key))
        output_row["source_event_cd_scope_keys"].append(str(row.event_cd_scope_key))
        output_row["source_mapped_geo_levels"].append(str(row.source_mapped_geo_level))
        output_row["resolution_methods"].append(str(row.resolution_method))
        output_row["mapping_confidences"].append(str(row.mapping_confidence))
        output_row["mapping_methods"].append(str(row.mapping_method))

        output_row["affected_overlap_area_sq_km"] += float(row.intersection_area_sq_km)
        output_row["maximum_single_cd_coverage_ratio"] = max(
            float(output_row["maximum_single_cd_coverage_ratio"]),
            float(row.single_cd_grid_coverage_ratio),
        )

        if bool(row.is_csd_to_cd_approximation):
            output_row["is_csd_to_cd_approximation"] = True

    output_rows: list[dict[str, Any]] = []

    for row in rows_by_event_grid.values():
        matched_cd_keys = sorted(set(row.pop("matched_census_division_keys")))
        event_cd_keys = sorted(set(row.pop("source_event_cd_scope_keys")))
        mapped_levels = sorted(set(row.pop("source_mapped_geo_levels")))
        resolution_methods = sorted(set(row.pop("resolution_methods")))
        mapping_confidences = sorted(set(row.pop("mapping_confidences")))
        mapping_methods = sorted(set(row.pop("mapping_methods")))

        geometry_area = float(row["grid_geometry_area_sq_km"])
        affected_area = float(row["affected_overlap_area_sq_km"])

        row["matched_census_division_keys_json"] = json.dumps(matched_cd_keys)
        row["matched_census_division_count"] = len(matched_cd_keys)
        row["source_event_cd_scope_keys_json"] = json.dumps(event_cd_keys)
        row["source_mapped_geo_levels_json"] = json.dumps(mapped_levels)
        row["resolution_methods_json"] = json.dumps(resolution_methods)
        row["mapping_confidences_json"] = json.dumps(mapping_confidences)
        row["mapping_methods_json"] = json.dumps(mapping_methods)
        row["affected_grid_coverage_ratio"] = min(
            affected_area / geometry_area,
            1.0,
        )

        output_rows.append(row)

    result = pd.DataFrame(output_rows)

    result = (
        result[
            [
                "event_grid_scope_key",
                "disaster_event_reference_key",
                "source_disaster_event_key",
                "reference_month",
                "event_year",
                "event_month_number",
                "province_key",
                "disaster_domain",
                "location_text",
                "location_tier",
                "grid_cell_key",
                "grid_system",
                "grid_province_key",
                "grid_analysis_area_sq_km",
                "grid_geometry_area_sq_km",
                "matched_census_division_keys_json",
                "matched_census_division_count",
                "source_event_cd_scope_keys_json",
                "source_mapped_geo_levels_json",
                "resolution_methods_json",
                "mapping_confidences_json",
                "mapping_methods_json",
                "affected_overlap_area_sq_km",
                "affected_grid_coverage_ratio",
                "maximum_single_cd_coverage_ratio",
                "is_csd_to_cd_approximation",
                "is_backtest_window",
                "is_ab_bc_scope",
                "is_domain_relevant",
                "is_grid_backtest_eligible",
            ]
        ]
        .sort_values(
            [
                "reference_month",
                "disaster_event_reference_key",
                "grid_cell_key",
            ]
        )
        .reset_index(drop=True)
    )

    _validate_result(
        result=result,
        event_cd_scope=events,
        target_grids=grids,
    )

    summary = _build_summary(
        result=result,
        event_cd_scope=events,
        grid_cd_bridge=grid_cd_bridge,
        relevant_cd_keys=relevant_cd_keys,
    )

    return result, summary


def _validate_inputs(
    *,
    event_cd_scope: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> None:
    required_event_columns = {
        "event_cd_scope_key",
        "disaster_event_reference_key",
        "source_disaster_event_key",
        "reference_month",
        "event_year",
        "event_month_number",
        "province_key",
        "disaster_domain",
        "location_text",
        "location_tier",
        "source_mapped_geo_level",
        "resolved_census_division_key",
        "census_division_province_key",
        "resolution_method",
        "is_csd_to_cd_approximation",
        "mapping_confidence",
        "mapping_method",
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    }

    required_cd_columns = {
        "census_division_key",
        "province_key",
        "geometry_crs_epsg",
        "geometry_wkt",
    }

    required_grid_columns = {
        "grid_cell_key",
        "grid_system",
        "province_key",
        "cell_min_x",
        "cell_min_y",
        "cell_max_x",
        "cell_max_y",
        "analysis_area_sq_km",
        "analysis_geometry_wkt",
        "crs_epsg",
    }

    missing_event = required_event_columns - set(event_cd_scope.columns)
    missing_cd = required_cd_columns - set(cd_spatial_reference.columns)
    missing_grid = required_grid_columns - set(grid_cell.columns)

    if missing_event:
        raise GoldDisasterEventGridScopeError(
            f"Missing event-CD scope columns: {sorted(missing_event)}"
        )

    if missing_cd:
        raise GoldDisasterEventGridScopeError(f"Missing CD spatial columns: {sorted(missing_cd)}")

    if missing_grid:
        raise GoldDisasterEventGridScopeError(f"Missing grid-cell columns: {sorted(missing_grid)}")

    if event_cd_scope.empty:
        raise GoldDisasterEventGridScopeError("Event-CD scope input is empty.")

    if cd_spatial_reference.empty:
        raise GoldDisasterEventGridScopeError("CD spatial reference input is empty.")

    if grid_cell.empty:
        raise GoldDisasterEventGridScopeError("Grid-cell input is empty.")

    cd_crs = set(
        pd.to_numeric(
            cd_spatial_reference["geometry_crs_epsg"],
            errors="coerce",
        ).dropna()
    )
    grid_crs = set(
        pd.to_numeric(
            grid_cell.loc[
                grid_cell["grid_system"].isin(TARGET_GRID_SYSTEMS),
                "crs_epsg",
            ],
            errors="coerce",
        ).dropna()
    )

    if cd_crs != {EXPECTED_CRS_EPSG}:
        raise GoldDisasterEventGridScopeError(
            f"Expected CD CRS {EXPECTED_CRS_EPSG}, found {sorted(cd_crs)}"
        )

    if grid_crs != {EXPECTED_CRS_EPSG}:
        raise GoldDisasterEventGridScopeError(
            f"Expected grid CRS {EXPECTED_CRS_EPSG}, " f"found {sorted(grid_crs)}"
        )


def _validate_result(
    *,
    result: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    target_grids: pd.DataFrame,
) -> None:
    if result.empty:
        raise GoldDisasterEventGridScopeError("Event-grid scope output is empty.")

    if result["event_grid_scope_key"].isna().any():
        raise GoldDisasterEventGridScopeError("event_grid_scope_key contains nulls.")

    if result["event_grid_scope_key"].duplicated().any():
        raise GoldDisasterEventGridScopeError("event_grid_scope_key contains duplicates.")

    duplicate_event_grid = result.duplicated(["disaster_event_reference_key", "grid_cell_key"])

    if duplicate_event_grid.any():
        raise GoldDisasterEventGridScopeError("Output contains duplicate event-grid rows.")

    expected_events = set(event_cd_scope["disaster_event_reference_key"].astype(str))
    observed_events = set(result["disaster_event_reference_key"].astype(str))

    if expected_events != observed_events:
        missing = sorted(expected_events - observed_events)
        extra = sorted(observed_events - expected_events)

        raise GoldDisasterEventGridScopeError(
            "Output event set does not match event-CD source. " f"missing={missing}, extra={extra}"
        )

    known_grid_keys = set(target_grids["grid_cell_key"].astype(str))
    output_grid_keys = set(result["grid_cell_key"].astype(str))

    missing_grid_keys = sorted(output_grid_keys - known_grid_keys)

    if missing_grid_keys:
        raise GoldDisasterEventGridScopeError(
            "Output contains unknown grid keys: " f"{missing_grid_keys[:20]}"
        )

    province_mismatch = result[result["province_key"].ne(result["grid_province_key"])]

    if not province_mismatch.empty:
        raise GoldDisasterEventGridScopeError("Event province and grid province do not match.")

    required_true = [
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    ]

    for column in required_true:
        if not result[column].astype(bool).all():
            raise GoldDisasterEventGridScopeError(
                f"{column} must be true for every event-grid row."
            )

    if (
        not result["affected_grid_coverage_ratio"]
        .between(
            0.0,
            1.0,
            inclusive="right",
        )
        .all()
    ):
        raise GoldDisasterEventGridScopeError("affected_grid_coverage_ratio must be within (0, 1].")

    if (
        not result["maximum_single_cd_coverage_ratio"]
        .between(
            0.0,
            1.0,
            inclusive="right",
        )
        .all()
    ):
        raise GoldDisasterEventGridScopeError(
            "maximum_single_cd_coverage_ratio must be within (0, 1]."
        )

    if not result["matched_census_division_count"].ge(1).all():
        raise GoldDisasterEventGridScopeError(
            "Every event-grid row must have at least one matched CD."
        )


def _build_summary(
    *,
    result: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    grid_cd_bridge: pd.DataFrame,
    relevant_cd_keys: set[str],
) -> dict[str, Any]:
    grids_per_event = result.groupby("disaster_event_reference_key").size()

    return {
        "table_name": TABLE_NAME,
        "row_count": int(len(result)),
        "source_event_cd_scope_row_count": int(len(event_cd_scope)),
        "source_grid_backtest_event_count": int(
            event_cd_scope["disaster_event_reference_key"].nunique()
        ),
        "unique_event_count": int(result["disaster_event_reference_key"].nunique()),
        "unique_grid_cell_count": int(result["grid_cell_key"].nunique()),
        "source_census_division_count": int(len(relevant_cd_keys)),
        "grid_cd_bridge_row_count": int(len(grid_cd_bridge)),
        "minimum_reference_month": str(result["reference_month"].min()),
        "maximum_reference_month": str(result["reference_month"].max()),
        "province_counts": _value_counts(result["province_key"]),
        "grid_system_counts": _value_counts(result["grid_system"]),
        "disaster_domain_counts": _value_counts(result["disaster_domain"]),
        "csd_approximation_event_grid_row_count": int(result["is_csd_to_cd_approximation"].sum()),
        "grids_per_event_min": int(grids_per_event.min()),
        "grids_per_event_median": float(grids_per_event.median()),
        "grids_per_event_mean": float(grids_per_event.mean()),
        "grids_per_event_max": int(grids_per_event.max()),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _nullable_str(value: Any) -> str | None:
    if pd.isna(value):
        return None

    return str(value)
