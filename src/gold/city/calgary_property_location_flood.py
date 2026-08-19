from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import make_valid, wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree


SOURCE_CRS = "EPSG:4326"
TARGET_CRS = "EPSG:3347"

FLOOD_EXPOSURE_CLASSES = {
    "Flood Fringe",
    "Floodway",
    "Floodplain",
    "Overland Flow",
}

EXPECTED_CLASSES = FLOOD_EXPOSURE_CLASSES | {
    "Normal River Channel"
}

SORTED_EXPECTED_CLASSES = sorted(EXPECTED_CLASSES)

REQUIRED_LOCATION_COLUMNS = {
    "source_parcel_id",
    "geometry_wkt",
}

REQUIRED_FLOOD_COLUMNS = {
    "flood_hazard_zone_key",
    "source_zone_id",
    "hazard_class",
    "geometry_wkt",
}


class CalgaryPropertyLocationFloodError(Exception):
    pass


def build_gold_calgary_property_location_flood(
    *,
    location_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    locations = location_dataframe.copy()

    floods = flood_dataframe[
        flood_dataframe["city"]
        .astype("string")
        .str.lower()
        .eq("calgary")
    ].copy()

    _require(
        locations,
        REQUIRED_LOCATION_COLUMNS,
        "gold_calgary_property_location_assessment",
    )
    _require(
        floods,
        REQUIRED_FLOOD_COLUMNS,
        "silver_flood_hazard_zone",
    )

    if locations["source_parcel_id"].duplicated().any():
        raise CalgaryPropertyLocationFloodError(
            "Location input must be one row per source_parcel_id."
        )

    actual_classes = set(
        floods["hazard_class"].dropna().astype(str).unique()
    )
    unexpected = actual_classes - EXPECTED_CLASSES

    if unexpected:
        raise CalgaryPropertyLocationFloodError(
            f"Unexpected Calgary flood classes: {sorted(unexpected)}"
        )

    transformer = Transformer.from_crs(
        SOURCE_CRS,
        TARGET_CRS,
        always_xy=True,
    )

    # Prepare flood polygons
    flood_geometries = []
    flood_records = []
    repaired_flood_count = 0

    for row in floods.itertuples(index=False):
        geometry = wkt.loads(row.geometry_wkt)

        if not geometry.is_valid:
            geometry = make_valid(geometry)
            repaired_flood_count += 1

        geometry = _polygonal_only(geometry)

        if geometry is None or geometry.is_empty:
            continue

        geometry = transform(
            transformer.transform,
            geometry,
        )

        flood_geometries.append(geometry)
        flood_records.append(
            {
                "flood_hazard_zone_key": row.flood_hazard_zone_key,
                "source_zone_id": row.source_zone_id,
                "hazard_class": row.hazard_class,
                "geometry": geometry,
            }
        )

    tree = STRtree(flood_geometries)

    # Location polygons × flood polygons
    overlay_rows = []
    repaired_location_count = 0

    for row in locations.itertuples(index=False):
        geometry = wkt.loads(row.geometry_wkt)
        location_repaired = False

        if not geometry.is_valid:
            geometry = make_valid(geometry)
            repaired_location_count += 1
            location_repaired = True

        geometry = _polygonal_only(geometry)

        if geometry is None or geometry.is_empty:
            continue

        geometry = transform(
            transformer.transform,
            geometry,
        )

        location_area = float(geometry.area)

        for index in tree.query(
            geometry,
            predicate="intersects",
        ):
            flood = flood_records[int(index)]
            intersection = geometry.intersection(
                flood["geometry"]
            )
            intersection_area = float(intersection.area)

            if intersection_area <= 0:
                continue

            hazard_class = str(flood["hazard_class"])

            overlay_rows.append(
                {
                    "property_location_flood_overlay_key": (
                        _build_overlay_key(
                            source_parcel_id=row.source_parcel_id,
                            flood_hazard_zone_key=(
                                flood["flood_hazard_zone_key"]
                            ),
                        )
                    ),
                    "source_parcel_id": row.source_parcel_id,
                    "flood_hazard_zone_key": (
                        flood["flood_hazard_zone_key"]
                    ),
                    "source_zone_id": flood["source_zone_id"],
                    "hazard_class": hazard_class,
                    "is_flood_hazard_class": (
                        hazard_class in FLOOD_EXPOSURE_CLASSES
                    ),
                    "is_normal_river_channel": (
                        hazard_class == "Normal River Channel"
                    ),
                    "location_area_sq_m": location_area,
                    "intersection_area_sq_m": intersection_area,
                    "location_overlap_ratio": (
                        intersection_area / location_area
                    ),
                    "location_geometry_repaired": (
                        location_repaired
                    ),
                    "intersection_geometry_wkt_3347": (
                        intersection.wkt
                    ),
                    "crs_epsg": 3347,
                }
            )

    overlay = pd.DataFrame(overlay_rows)

    if not overlay.empty:
        if overlay.duplicated(
            subset=[
                "source_parcel_id",
                "flood_hazard_zone_key",
            ]
        ).any():
            raise CalgaryPropertyLocationFloodError(
                "Duplicate location × flood-zone rows."
            )

        if (
            overlay["location_overlap_ratio"] > 1 + 1e-8
        ).any():
            raise CalgaryPropertyLocationFloodError(
                "Location overlap ratio exceeds 1."
            )

    # Final one-row-per-location exposure
    base = locations[
        [
            "source_parcel_id",
            "geometry_wkt",
        ]
    ].copy()

    exposure_rows = []

    if not overlay.empty:
        for parcel_id, group in overlay.groupby(
            "source_parcel_id",
            sort=False,
        ):
            exposure_row = {
                "source_parcel_id": parcel_id,
                "intersects_regulatory_flood_layer": True,
                "is_flood_exposed": bool(
                    group["is_flood_hazard_class"].any()
                ),
                "intersects_normal_river_channel": bool(
                    group["is_normal_river_channel"].any()
                ),
                "flood_zone_membership_count": int(len(group)),
            }

            for hazard_class in SORTED_EXPECTED_CLASSES:
                prefix = _prefix(hazard_class)

                subset = group[
                    group["hazard_class"].eq(hazard_class)
                ]

                exposure_row[f"{prefix}_flag"] = (
                    not subset.empty
                )

                if subset.empty:
                    exposure_row[
                        f"{prefix}_overlap_area_sq_m"
                    ] = 0.0
                    exposure_row[
                        f"{prefix}_overlap_ratio"
                    ] = 0.0
                    continue

                geometries = [
                    wkt.loads(value)
                    for value in subset[
                        "intersection_geometry_wkt_3347"
                    ]
                ]

                union = unary_union(geometries)
                area = float(union.area)

                location_area = float(
                    subset["location_area_sq_m"].iloc[0]
                )

                exposure_row[
                    f"{prefix}_overlap_area_sq_m"
                ] = area
                exposure_row[
                    f"{prefix}_overlap_ratio"
                ] = area / location_area

            exposure_rows.append(exposure_row)

    exposure_context = pd.DataFrame(exposure_rows)

    if exposure_context.empty:
        result = base.copy()
    else:
        result = base.merge(
            exposure_context,
            on="source_parcel_id",
            how="left",
            validate="one_to_one",
        )

    boolean_columns = [
        "intersects_regulatory_flood_layer",
        "is_flood_exposed",
        "intersects_normal_river_channel",
        *[
            f"{_prefix(hazard_class)}_flag"
            for hazard_class in SORTED_EXPECTED_CLASSES
        ],
    ]

    for column in boolean_columns:
        if column not in result:
            result[column] = False

        result[column] = (
            result[column]
            .astype("boolean")
            .fillna(False)
            .astype(bool)
        )

    if "flood_zone_membership_count" not in result:
        result["flood_zone_membership_count"] = 0

    result["flood_zone_membership_count"] = (
        result["flood_zone_membership_count"]
        .fillna(0)
        .astype("int64")
    )

    for hazard_class in SORTED_EXPECTED_CLASSES:
        prefix = _prefix(hazard_class)

        for suffix in (
            "overlap_area_sq_m",
            "overlap_ratio",
        ):
            column = f"{prefix}_{suffix}"

            if column not in result:
                result[column] = 0.0

            result[column] = (
                result[column]
                .fillna(0.0)
                .astype(float)
            )

    summary = {
        "location_input_count": int(len(locations)),
        "flood_zone_input_count": int(len(floods)),
        "overlay_row_count": int(len(overlay)),
        "location_output_count": int(len(result)),
        "regulatory_layer_intersection_count": int(
            result[
                "intersects_regulatory_flood_layer"
            ].sum()
        ),
        "flood_exposed_location_count": int(
            result["is_flood_exposed"].sum()
        ),
        "normal_river_channel_location_count": int(
            result[
                "intersects_normal_river_channel"
            ].sum()
        ),
        "normal_river_channel_only_location_count": int(
            (
                result["intersects_normal_river_channel"]
                & ~result["is_flood_exposed"]
            ).sum()
        ),
        "repaired_location_geometry_count": int(
            repaired_location_count
        ),
        "repaired_flood_geometry_count": int(
            repaired_flood_count
        ),
        "hazard_class_location_counts": {
            hazard_class: int(
                result[
                    f"{_prefix(hazard_class)}_flag"
                ].sum()
            )
            for hazard_class in SORTED_EXPECTED_CLASSES
        },
    }

    return overlay, result, summary


def _prefix(hazard_class: str) -> str:
    return hazard_class.lower().replace(" ", "_")


def _build_overlay_key(
    *,
    source_parcel_id: str,
    flood_hazard_zone_key: str,
) -> str:
    identity = {
        "source_parcel_id": source_parcel_id,
        "flood_hazard_zone_key": flood_hazard_zone_key,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"calgary_{digest}"


def _polygonal_only(geometry):
    if geometry.geom_type in {
        "Polygon",
        "MultiPolygon",
    }:
        return geometry

    if geometry.geom_type == "GeometryCollection":
        polygons = [
            part
            for part in geometry.geoms
            if isinstance(
                part,
                (Polygon, MultiPolygon),
            )
        ]

        if not polygons:
            return None

        return unary_union(polygons)

    return None


def _require(
    dataframe: pd.DataFrame,
    required: set[str],
    name: str,
) -> None:
    missing = required - set(dataframe.columns)

    if missing:
        raise CalgaryPropertyLocationFloodError(
            f"{name} missing columns: {sorted(missing)}"
        )