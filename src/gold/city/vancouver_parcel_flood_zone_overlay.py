from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform
from shapely.strtree import STRtree

from src.gold.spatial.grid import ANALYSIS_CRS_EPSG, normalize_polygonal_geometry


SOURCE_CRS_EPSG = 4326
MINIMUM_INTERSECTION_AREA_SQ_M = 1e-6
CITY = "vancouver"

REQUIRED_PARCEL_COLUMNS = {
    "property_parcel_key",
    "city",
    "source_name",
    "source_parcel_id",
    "geometry_wkt",
}

REQUIRED_FLOOD_COLUMNS = {
    "flood_hazard_zone_key",
    "city",
    "source_zone_id",
    "hazard_class",
    "geometry_wkt",
    "source_name",
    "source_properties_json",
}


class VancouverParcelFloodOverlayError(Exception):
    """Raised when Vancouver parcel/flood overlay construction fails."""


def build_gold_vancouver_parcel_flood_zone_overlay(
    *,
    parcel_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
    progress_interval: int = 10_000,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build positive-area Vancouver parcel -- source flood-zone intersections."""
    _require_columns(
        dataframe=parcel_dataframe,
        required_columns=REQUIRED_PARCEL_COLUMNS,
        table_name="silver_property_parcel",
    )
    _require_columns(
        dataframe=flood_dataframe,
        required_columns=REQUIRED_FLOOD_COLUMNS,
        table_name="silver_flood_hazard_zone",
    )

    parcels = _filter_city(parcel_dataframe, table_name="silver_property_parcel")
    floods = _filter_city(flood_dataframe, table_name="silver_flood_hazard_zone")

    _require_unique_key(
        dataframe=parcels,
        column="property_parcel_key",
        table_name="silver_property_parcel",
    )
    _require_unique_key(
        dataframe=floods,
        column="flood_hazard_zone_key",
        table_name="silver_flood_hazard_zone",
    )

    transformer = Transformer.from_crs(
        SOURCE_CRS_EPSG,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )

    prepared_floods, flood_geometry_summary = _prepare_flood_geometries(
        floods,
        transformer=transformer,
    )
    spatial_index = STRtree([record["_geometry"] for record in prepared_floods])

    rows: list[dict[str, Any]] = []
    parcel_invalid_count = 0
    parcel_repaired_count = 0
    candidate_pair_count = 0
    boundary_touch_only_pair_count = 0

    for processed_parcel_count, parcel_row in enumerate(
        parcels.itertuples(index=False),
        start=1,
    ):
        parcel_geometry, parcel_initially_invalid, parcel_repaired = (
            _parse_normalize_project_geometry(
                geometry_wkt=parcel_row.geometry_wkt,
                transformer=transformer,
                entity_name=f"parcel {parcel_row.property_parcel_key}",
            )
        )

        parcel_invalid_count += int(parcel_initially_invalid)
        parcel_repaired_count += int(parcel_repaired)
        parcel_area_sq_m = float(parcel_geometry.area)

        candidate_indices = spatial_index.query(
            parcel_geometry,
            predicate="intersects",
        )
        candidate_pair_count += len(candidate_indices)

        for flood_index in candidate_indices:
            flood_record = prepared_floods[int(flood_index)]
            intersection = parcel_geometry.intersection(flood_record["_geometry"])
            intersection_area_sq_m = float(intersection.area)

            # Boundary-only touches are candidates, not exposure relationships.
            if intersection_area_sq_m <= MINIMUM_INTERSECTION_AREA_SQ_M:
                boundary_touch_only_pair_count += 1
                continue

            parcel_overlap_ratio = min(
                intersection_area_sq_m / parcel_area_sq_m,
                1.0,
            )

            overlay_key = build_parcel_flood_zone_overlay_key(
                property_parcel_key=str(parcel_row.property_parcel_key),
                flood_hazard_zone_key=str(flood_record["flood_hazard_zone_key"]),
            )

            rows.append(
                {
                    "parcel_flood_zone_overlay_key": overlay_key,
                    "property_parcel_key": str(parcel_row.property_parcel_key),
                    "source_parcel_id": _clean_optional_string(
                        parcel_row.source_parcel_id
                    ),
                    "flood_hazard_zone_key": str(
                        flood_record["flood_hazard_zone_key"]
                    ),
                    "source_zone_id": _clean_optional_string(
                        flood_record["source_zone_id"]
                    ),
                    "scenario_name": flood_record["scenario_name"],
                    "hazard_description": flood_record["hazard_description"],
                    "hazard_class": flood_record["hazard_class"],
                    "parcel_area_sq_m": parcel_area_sq_m,
                    "source_zone_area_sq_m": flood_record["source_zone_area_sq_m"],
                    "intersection_area_sq_m": intersection_area_sq_m,
                    "parcel_overlap_ratio": parcel_overlap_ratio,
                    "intersection_geometry_wkt_3347": intersection.wkt,
                    "parcel_geometry_repaired": bool(parcel_repaired),
                    "flood_geometry_repaired": bool(
                        flood_record["_geometry_repaired"]
                    ),
                    "parcel_source_name": str(parcel_row.source_name),
                    "flood_source_name": str(flood_record["source_name"]),
                    "spatial_join_method": "polygon_intersection_epsg3347",
                    "crs_epsg": ANALYSIS_CRS_EPSG,
                }
            )

        if progress_interval > 0 and processed_parcel_count % progress_interval == 0:
            print(
                "[INFO] Vancouver parcel flood overlay progress | "
                f"processed_parcels={processed_parcel_count}/{len(parcels)} "
                f"candidate_pairs={candidate_pair_count} "
                f"overlay_rows={len(rows)}"
            )

    if not rows:
        raise VancouverParcelFloodOverlayError(
            "Vancouver parcel/flood overlay produced zero positive-area intersections."
        )

    overlay = pd.DataFrame(rows)

    if overlay.duplicated(
        subset=["property_parcel_key", "flood_hazard_zone_key"]
    ).any():
        raise VancouverParcelFloodOverlayError(
            "Overlay contains duplicate parcel -- flood-zone relationships."
        )

    overlay = overlay.sort_values(
        ["property_parcel_key", "flood_hazard_zone_key"]
    ).reset_index(drop=True)

    matched_parcel_count = overlay["property_parcel_key"].nunique()
    unmatched_parcel_count = len(parcels) - matched_parcel_count

    scenario_overlay_row_counts = {
        str(key): int(value)
        for key, value in overlay["scenario_name"].value_counts().sort_index().items()
    }

    scenario_matched_parcel_counts = {
        str(key): int(value)
        for key, value in (
            overlay.groupby("scenario_name")["property_parcel_key"]
            .nunique()
            .sort_index()
            .items()
        )
    }

    ratio_series = overlay["parcel_overlap_ratio"]

    summary = {
        "parcel_input_count": len(parcels),
        "flood_zone_input_count": len(floods),
        "parcel_geometry_invalid_count": parcel_invalid_count,
        "parcel_geometry_repaired_count": parcel_repaired_count,
        **flood_geometry_summary,
        "candidate_pair_count": candidate_pair_count,
        "boundary_touch_only_pair_count": boundary_touch_only_pair_count,
        "overlay_row_count": len(overlay),
        "matched_parcel_count": matched_parcel_count,
        "unmatched_parcel_count": unmatched_parcel_count,
        "parcel_match_rate": matched_parcel_count / len(parcels),
        "scenario_overlay_row_counts": scenario_overlay_row_counts,
        "scenario_matched_parcel_counts": scenario_matched_parcel_counts,
        "parcel_overlap_ratio_p50": float(ratio_series.quantile(0.50)),
        "parcel_overlap_ratio_p90": float(ratio_series.quantile(0.90)),
        "parcel_overlap_ratio_p95": float(ratio_series.quantile(0.95)),
        "parcel_overlap_ratio_p99": float(ratio_series.quantile(0.99)),
        "parcel_overlap_ratio_max": float(ratio_series.max()),
    }

    return overlay, summary


def build_parcel_flood_zone_overlay_key(
    *,
    property_parcel_key: str,
    flood_hazard_zone_key: str,
) -> str:
    identity = {
        "property_parcel_key": property_parcel_key,
        "flood_hazard_zone_key": flood_hazard_zone_key,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"


def _prepare_flood_geometries(
    dataframe: pd.DataFrame,
    *,
    transformer: Transformer,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    records: list[dict[str, Any]] = []
    initially_invalid_count = 0
    repaired_count = 0

    for row in dataframe.itertuples(index=False):
        geometry, initially_invalid, repaired = _parse_normalize_project_geometry(
            geometry_wkt=row.geometry_wkt,
            transformer=transformer,
            entity_name=f"flood zone {row.flood_hazard_zone_key}",
        )

        initially_invalid_count += int(initially_invalid)
        repaired_count += int(repaired)

        properties = _parse_source_properties(row.source_properties_json)
        scenario_name = _clean_optional_string(properties.get("name"))

        if scenario_name is None:
            raise VancouverParcelFloodOverlayError(
                f"Vancouver flood zone is missing source 'name': "
                f"{row.flood_hazard_zone_key}"
            )

        hazard_description = _clean_optional_string(properties.get("description"))
        if hazard_description is None:
            hazard_description = _clean_optional_string(row.hazard_class)

        records.append(
            {
                "flood_hazard_zone_key": str(row.flood_hazard_zone_key),
                "source_zone_id": _clean_optional_string(row.source_zone_id),
                "scenario_name": scenario_name,
                "hazard_description": hazard_description,
                "hazard_class": _clean_optional_string(row.hazard_class),
                "source_name": str(row.source_name),
                "source_zone_area_sq_m": float(geometry.area),
                "_geometry": geometry,
                "_geometry_repaired": bool(repaired),
            }
        )

    return records, {
        "flood_geometry_invalid_count": initially_invalid_count,
        "flood_geometry_repaired_count": repaired_count,
    }


def _parse_normalize_project_geometry(
    *,
    geometry_wkt: Any,
    transformer: Transformer,
    entity_name: str,
) -> tuple[BaseGeometry, bool, bool]:
    if geometry_wkt is None or pd.isna(geometry_wkt):
        raise VancouverParcelFloodOverlayError(
            f"Missing geometry WKT for {entity_name}."
        )

    source_geometry = wkt.loads(str(geometry_wkt))
    initially_invalid = not source_geometry.is_valid

    normalized_geometry, repaired = normalize_polygonal_geometry(source_geometry)

    projected_geometry = shapely_transform(
        transformer.transform,
        normalized_geometry,
    )

    if not projected_geometry.is_valid:
        projected_geometry, projected_repaired = normalize_polygonal_geometry(
            projected_geometry
        )
        repaired = repaired or projected_repaired

    return projected_geometry, initially_invalid, bool(repaired)


def _parse_source_properties(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value

    if value is None or pd.isna(value):
        return {}

    return json.loads(str(value))


def _filter_city(
    dataframe: pd.DataFrame,
    *,
    table_name: str,
) -> pd.DataFrame:
    city = dataframe["city"].astype("string").str.lower()
    filtered = dataframe[city == CITY].copy()

    if filtered.empty:
        raise VancouverParcelFloodOverlayError(
            f"{table_name} contains no Vancouver rows."
        )

    return filtered.reset_index(drop=True)


def _require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise VancouverParcelFloodOverlayError(
            f"{table_name} is missing columns: {sorted(missing_columns)}"
        )


def _require_unique_key(
    *,
    dataframe: pd.DataFrame,
    column: str,
    table_name: str,
) -> None:
    if dataframe[column].isna().any():
        raise VancouverParcelFloodOverlayError(
            f"{table_name} contains null {column} values."
        )

    if dataframe[column].duplicated().any():
        raise VancouverParcelFloodOverlayError(
            f"{table_name} contains duplicate {column} values."
        )


def _clean_optional_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    return text or None
