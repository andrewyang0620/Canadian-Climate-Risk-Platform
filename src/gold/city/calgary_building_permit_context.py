from __future__ import annotations

from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.ops import transform
from shapely.strtree import STRtree


CITY = "calgary"
SOURCE_CRS = "EPSG:4326"
TARGET_CRS = "EPSG:3347"


class CalgaryBuildingPermitContextError(Exception):
    pass


def build_gold_calgary_building_permit_context(
    *,
    permit_dataframe: pd.DataFrame,
    location_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    permits = permit_dataframe[
        permit_dataframe["city"].astype("string").str.lower().eq(CITY)
    ].copy()

    if permits["building_permit_key"].isna().any():
        raise CalgaryBuildingPermitContextError(
            "Null building_permit_key."
        )

    if permits["building_permit_key"].duplicated().any():
        raise CalgaryBuildingPermitContextError(
            "Duplicate building_permit_key."
        )

    if location_dataframe["source_parcel_id"].duplicated().any():
        raise CalgaryBuildingPermitContextError(
            "Location input must be one row per source_parcel_id."
        )

    if flood_dataframe["source_parcel_id"].duplicated().any():
        raise CalgaryBuildingPermitContextError(
            "Flood input must be one row per source_parcel_id."
        )

    # Housing semantics
    permits["housing_units_reported"] = pd.to_numeric(
        permits["housing_units"],
        errors="coerce",
    )

    permits["housing_units_anomaly_flag"] = (
        permits["housing_units_reported"] < 0
    )

    permits["new_housing_units_created"] = (
        permits["housing_units_reported"].where(
            permits["housing_units_reported"] >= 0
        )
    )

    permits["creates_new_housing_units"] = (
        permits["new_housing_units_created"] > 0
    )

    permits["is_residential_permit"] = (
        permits["permit_class_mapped"].eq("Residential")
    )

    permits["is_housing_related"] = (
        permits["is_residential_permit"]
        | permits["creates_new_housing_units"]
    )

    permits["housing_activity_type"] = "non_housing"

    permits.loc[
        permits["is_housing_related"],
        "housing_activity_type",
    ] = "unspecified"

    permits.loc[
        permits["is_housing_related"]
        & permits["work_class_group"].eq("New"),
        "housing_activity_type",
    ] = "new"

    permits.loc[
        permits["is_housing_related"]
        & permits["work_class_group"].eq("Improvement"),
        "housing_activity_type",
    ] = "improvement"

    permits.loc[
        permits["is_housing_related"]
        & permits["permit_type_mapped"].eq("Demolition"),
        "housing_activity_type",
    ] = "demolition"

    # Prepare property-location polygons
    transformer = Transformer.from_crs(
        SOURCE_CRS,
        TARGET_CRS,
        always_xy=True,
    )

    location_geometries = []
    location_keys = []

    for row in location_dataframe.itertuples(index=False):
        geometry = wkt.loads(row.geometry_wkt)

        if geometry.is_empty:
            continue

        geometry = transform(
            transformer.transform,
            geometry,
        )

        location_geometries.append(geometry)
        location_keys.append(row.source_parcel_id)

    tree = STRtree(location_geometries)

    # Permit point -> current property location
    statuses = []
    match_counts = []
    matched_locations = []
    spatial_flags = []

    for geometry_text in permits["geometry_wkt"]:
        if (
            geometry_text is None
            or pd.isna(geometry_text)
            or str(geometry_text).strip() == ""
        ):
            spatial_flags.append(False)
            match_counts.append(0)
            statuses.append("no_geometry")
            matched_locations.append(pd.NA)
            continue

        point = wkt.loads(str(geometry_text))

        if (
            point.is_empty
            or not point.is_valid
            or point.geom_type != "Point"
        ):
            spatial_flags.append(False)
            match_counts.append(0)
            statuses.append("invalid_geometry")
            matched_locations.append(pd.NA)
            continue

        spatial_flags.append(True)

        point = transform(
            transformer.transform,
            point,
        )

        indices = tree.query(
            point,
            predicate="intersects",
        )

        matches = {
            location_keys[int(index)]
            for index in indices
            if location_geometries[int(index)].covers(point)
        }

        match_count = len(matches)
        match_counts.append(match_count)

        if match_count == 0:
            statuses.append("no_location_match")
            matched_locations.append(pd.NA)
        elif match_count == 1:
            statuses.append("exact_1_to_1")
            matched_locations.append(next(iter(matches)))
        else:
            statuses.append("ambiguous_1_to_many")
            matched_locations.append(pd.NA)

    permits["has_spatial_geometry"] = spatial_flags
    permits["location_match_count"] = match_counts
    permits["location_mapping_status"] = statuses
    permits["source_parcel_id"] = matched_locations

    # Assessment context
    assessment_columns = [
        "source_parcel_id",
        "assessment_year",
        "assessment_record_count",
        "assessed_value_total_sum",
        "assessed_value_residential_sum",
        "assessed_value_non_residential_sum",
        "assessed_value_farmland_sum",
        "community_code",
        "community_name",
        "land_use_designation",
        "property_type",
        "year_of_construction_min",
        "year_of_construction_max",
    ]

    result = permits.merge(
        location_dataframe[assessment_columns],
        on="source_parcel_id",
        how="left",
        validate="many_to_one",
    )

    # Flood context
    flood_columns = [
        column
        for column in flood_dataframe.columns
        if column != "geometry_wkt"
    ]

    result = result.merge(
        flood_dataframe[flood_columns],
        on="source_parcel_id",
        how="left",
        validate="many_to_one",
    )

    flood_exposed = (
        result["is_flood_exposed"]
        .astype("boolean")
        .fillna(False)
    )

    summary = {
        "permit_input_count": int(len(permits)),
        "output_row_count": int(len(result)),
        "residential_permit_count": int(
            result["is_residential_permit"].sum()
        ),
        "housing_related_permit_count": int(
            result["is_housing_related"].sum()
        ),
        "new_housing_supply_permit_count": int(
            result["creates_new_housing_units"].sum()
        ),
        "housing_units_anomaly_count": int(
            result["housing_units_anomaly_flag"].sum()
        ),
        "housing_units_reported_sum": float(
            result["housing_units_reported"].sum()
        ),
        "new_housing_units_created_sum": float(
            result["new_housing_units_created"].sum()
        ),
        "spatial_geometry_count": int(
            result["has_spatial_geometry"].sum()
        ),
        "exact_location_match_count": int(
            result["location_mapping_status"]
            .eq("exact_1_to_1")
            .sum()
        ),
        "ambiguous_location_match_count": int(
            result["location_mapping_status"]
            .eq("ambiguous_1_to_many")
            .sum()
        ),
        "no_location_match_count": int(
            result["location_mapping_status"]
            .eq("no_location_match")
            .sum()
        ),
        "no_geometry_count": int(
            result["location_mapping_status"]
            .eq("no_geometry")
            .sum()
        ),
        "exact_matched_housing_permit_count": int(
            (
                result["is_housing_related"]
                & result["location_mapping_status"].eq(
                    "exact_1_to_1"
                )
            ).sum()
        ),
        "flood_exposed_housing_permit_count": int(
            (
                result["is_housing_related"]
                & flood_exposed
            ).sum()
        ),
        "flood_exposed_new_housing_units": float(
            result.loc[
                flood_exposed,
                "new_housing_units_created",
            ].sum()
        ),
    }

    return result, summary