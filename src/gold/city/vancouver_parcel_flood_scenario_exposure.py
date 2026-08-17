from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
from shapely import wkt
from shapely.ops import unary_union


REQUIRED_COLUMNS = {
    "property_parcel_key",
    "source_parcel_id",
    "flood_hazard_zone_key",
    "scenario_name",
    "parcel_area_sq_m",
    "intersection_area_sq_m",
    "intersection_geometry_wkt_3347",
    "crs_epsg",
}


class VancouverParcelFloodScenarioError(Exception):
    """Raised when Vancouver scenario exposure cannot be built."""


def build_gold_vancouver_parcel_flood_scenario_exposure(
    *,
    overlay_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Roll source-zone intersections to parcel × scenario grain."""
    missing_columns = REQUIRED_COLUMNS - set(overlay_dataframe.columns)

    if missing_columns:
        raise VancouverParcelFloodScenarioError(
            "Parcel flood-zone overlay is missing columns: "
            f"{sorted(missing_columns)}"
        )

    if overlay_dataframe.empty:
        raise VancouverParcelFloodScenarioError(
            "Parcel flood-zone overlay is empty."
        )

    if (
        overlay_dataframe["property_parcel_key"].isna().any()
        or overlay_dataframe["scenario_name"].isna().any()
    ):
        raise VancouverParcelFloodScenarioError(
            "Parcel key and scenario name must be non-null."
        )

    crs_values = set(
        overlay_dataframe["crs_epsg"]
        .dropna()
        .astype(int)
        .unique()
    )

    if crs_values != {3347}:
        raise VancouverParcelFloodScenarioError(
            "Scenario exposure requires EPSG:3347 "
            f"intersection geometries; found {sorted(crs_values)}."
        )

    rows: list[dict[str, Any]] = []

    grouped = overlay_dataframe.groupby(
        ["property_parcel_key", "scenario_name"],
        sort=True,
    )

    for (property_parcel_key, scenario_name), group in grouped:
        parcel_area_sq_m = float(group["parcel_area_sq_m"].iloc[0])

        geometries = [
            wkt.loads(value)
            for value in group["intersection_geometry_wkt_3347"]
        ]
        scenario_geometry = unary_union(geometries)

        scenario_intersection_area_sq_m = float(scenario_geometry.area)
        scenario_overlap_ratio = min(
            scenario_intersection_area_sq_m / parcel_area_sq_m,
            1.0,
        )

        source_zone_count = int(
            group["flood_hazard_zone_key"].nunique()
        )

        source_zone_area_sum_sq_m = float(
            group["intersection_area_sq_m"].sum()
        )

        overlap_removed_by_union_sq_m = max(
            source_zone_area_sum_sq_m
            - scenario_intersection_area_sq_m,
            0.0,
        )

        source_parcel_ids = (
            group["source_parcel_id"]
            .dropna()
            .astype(str)
            .unique()
        )

        source_parcel_id = (
            source_parcel_ids[0]
            if len(source_parcel_ids) == 1
            else None
        )

        rows.append(
            {
                "parcel_flood_scenario_exposure_key": build_scenario_exposure_key(
                    property_parcel_key=str(property_parcel_key),
                    scenario_name=str(scenario_name),
                ),
                "property_parcel_key": str(property_parcel_key),
                "source_parcel_id": source_parcel_id,
                "scenario_name": str(scenario_name),
                "source_zone_count": source_zone_count,
                "parcel_area_sq_m": parcel_area_sq_m,
                "source_zone_intersection_area_sum_sq_m": (
                    source_zone_area_sum_sq_m
                ),
                "scenario_intersection_area_sq_m": (
                    scenario_intersection_area_sq_m
                ),
                "overlap_removed_by_union_sq_m": (
                    overlap_removed_by_union_sq_m
                ),
                "scenario_overlap_ratio": scenario_overlap_ratio,
                "scenario_intersection_geometry_wkt_3347": (
                    scenario_geometry.wkt
                ),
                "crs_epsg": 3347,
                "rollup_method": "union_source_zone_intersections",
            }
        )

    result = (
        pd.DataFrame(rows)
        .sort_values(["property_parcel_key", "scenario_name"])
        .reset_index(drop=True)
    )

    multi_zone_scenario_rows = int(
        (result["source_zone_count"] > 1).sum()
    )

    union_adjusted_rows = int(
        (result["overlap_removed_by_union_sq_m"] > 1e-8).sum()
    )

    overlap_ratio = result["scenario_overlap_ratio"]

    summary = {
        "input_overlay_row_count": int(len(overlay_dataframe)),
        "scenario_exposure_row_count": int(len(result)),
        "parcel_count": int(
            result["property_parcel_key"].nunique()
        ),
        "scenario_count": int(
            result["scenario_name"].nunique()
        ),
        "multi_zone_scenario_row_count": multi_zone_scenario_rows,
        "union_adjusted_row_count": union_adjusted_rows,
        "scenario_overlap_ratio_p50": float(
            overlap_ratio.quantile(0.50)
        ),
        "scenario_overlap_ratio_p90": float(
            overlap_ratio.quantile(0.90)
        ),
        "scenario_overlap_ratio_p95": float(
            overlap_ratio.quantile(0.95)
        ),
        "scenario_overlap_ratio_p99": float(
            overlap_ratio.quantile(0.99)
        ),
        "scenario_overlap_ratio_max": float(
            overlap_ratio.max()
        ),
    }

    return result, summary


def build_scenario_exposure_key(
    *,
    property_parcel_key: str,
    scenario_name: str,
) -> str:
    identity = {
        "property_parcel_key": property_parcel_key,
        "scenario_name": scenario_name,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"