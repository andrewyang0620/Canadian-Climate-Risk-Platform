from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import MultiPoint, Point
from shapely.ops import transform
from shapely.strtree import STRtree


CITY = "calgary"
SOURCE_CRS = "EPSG:4326"
TARGET_CRS = "EPSG:3347"


class CalgaryDevelopmentPermitContextError(Exception):
    pass


def build_gold_calgary_development_permit_context(
    *,
    permit_dataframe: pd.DataFrame,
    location_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    permits = permit_dataframe[
        permit_dataframe["city"].astype("string").str.lower().eq(CITY)
    ].copy()

    if permits["development_permit_key"].isna().any():
        raise CalgaryDevelopmentPermitContextError(
            "Null development_permit_key."
        )

    if permits["development_permit_key"].duplicated().any():
        raise CalgaryDevelopmentPermitContextError(
            "Duplicate development_permit_key."
        )

    if location_dataframe["source_parcel_id"].duplicated().any():
        raise CalgaryDevelopmentPermitContextError(
            "Property-location input is not unique."
        )

    if flood_dataframe["source_parcel_id"].duplicated().any():
        raise CalgaryDevelopmentPermitContextError(
            "Flood context input is not unique."
        )

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

        location_geometries.append(
            transform(transformer.transform, geometry)
        )
        location_keys.append(row.source_parcel_id)

    tree = STRtree(location_geometries)

    bridge_rows = []
    mapping_rows = []

    for row in permits.itertuples(index=False):
        points = _extract_points(row.locations_wkt)
        raw_point_count = len(points)

        source_location_count = (
            int(row.location_count)
            if pd.notna(row.location_count)
            else 0
        )

        if raw_point_count != source_location_count:
            raise CalgaryDevelopmentPermitContextError(
                "locations_wkt point count does not match "
                f"location_count for {row.development_permit_key}."
            )

        source_titled_parcel_count = _count_titled_parcels(
            row.location_types
        )

        unique_points = {
            (float(point.x), float(point.y)): point
            for point in points
        }

        property_counter: Counter[str] = Counter()
        exact_point_count = 0
        ambiguous_point_count = 0
        unmatched_point_count = 0

        for point in unique_points.values():
            projected_point = transform(
                transformer.transform,
                point,
            )

            indices = tree.query(
                projected_point,
                predicate="intersects",
            )

            matches = {
                location_keys[int(index)]
                for index in indices
                if location_geometries[int(index)].covers(
                    projected_point
                )
            }

            if len(matches) == 1:
                source_parcel_id = next(iter(matches))
                property_counter[source_parcel_id] += 1
                exact_point_count += 1
            elif len(matches) > 1:
                ambiguous_point_count += 1
            else:
                unmatched_point_count += 1

        mapped_property_count = len(property_counter)

        if raw_point_count == 0:
            mapping_status = "no_geometry"
        elif mapped_property_count == 0:
            mapping_status = "no_property_match"
        elif mapped_property_count == 1:
            mapping_status = "single_property"
        else:
            mapping_status = "multi_property"

        has_partial_spatial_mapping = (
            mapped_property_count > 0
            and (
                ambiguous_point_count > 0
                or unmatched_point_count > 0
            )
        )

        single_source_parcel_id = (
            next(iter(property_counter))
            if mapped_property_count == 1
            else pd.NA
        )

        mapping_rows.append(
            {
                "development_permit_key": row.development_permit_key,
                "source_location_count": source_location_count,
                "source_titled_parcel_count": source_titled_parcel_count,
                "unique_source_point_count": len(unique_points),
                "exact_point_match_count": exact_point_count,
                "ambiguous_point_match_count": ambiguous_point_count,
                "unmatched_point_count": unmatched_point_count,
                "mapped_property_location_count": mapped_property_count,
                "location_mapping_status": mapping_status,
                "has_partial_spatial_mapping": has_partial_spatial_mapping,
                "single_source_parcel_id": single_source_parcel_id,
            }
        )

        for source_parcel_id, matched_point_count in property_counter.items():
            bridge_rows.append(
                {
                    "development_permit_key": row.development_permit_key,
                    "source_parcel_id": source_parcel_id,
                    "matched_unique_point_count": matched_point_count,
                }
            )

    mapping = pd.DataFrame(mapping_rows)

    bridge = pd.DataFrame(
        bridge_rows,
        columns=[
            "development_permit_key",
            "source_parcel_id",
            "matched_unique_point_count",
        ],
    )

    if not bridge.empty:
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
        ]

        bridge = bridge.merge(
            location_dataframe[assessment_columns],
            on="source_parcel_id",
            how="left",
            validate="many_to_one",
        )

        flood_columns = [
            column
            for column in flood_dataframe.columns
            if column != "geometry_wkt"
        ]

        bridge = bridge.merge(
            flood_dataframe[flood_columns],
            on="source_parcel_id",
            how="left",
            validate="many_to_one",
        )

    context = (
        permits.drop(
            columns=[
                "locations_geojson",
                "locations_wkt",
                "location_addresses",
                "location_types",
            ],
            errors="ignore",
        )
        .merge(
            mapping,
            on="development_permit_key",
            how="left",
            validate="one_to_one",
        )
    )

    context = _attach_permit_level_context(
        context=context,
        bridge=bridge,
    )

    summary = {
        "permit_input_count": int(len(permits)),
        "context_output_row_count": int(len(context)),
        "bridge_row_count": int(len(bridge)),
        "spatial_permit_count": int(
            (mapping["source_location_count"] > 0).sum()
        ),
        "no_geometry_permit_count": int(
            mapping["location_mapping_status"]
            .eq("no_geometry")
            .sum()
        ),
        "source_location_point_count": int(
            mapping["source_location_count"].sum()
        ),
        "unique_source_point_count": int(
            mapping["unique_source_point_count"].sum()
        ),
        "exact_point_match_count": int(
            mapping["exact_point_match_count"].sum()
        ),
        "ambiguous_point_match_count": int(
            mapping["ambiguous_point_match_count"].sum()
        ),
        "unmatched_point_count": int(
            mapping["unmatched_point_count"].sum()
        ),
        "mapped_permit_count": int(
            (mapping["mapped_property_location_count"] > 0).sum()
        ),
        "single_property_permit_count": int(
            mapping["location_mapping_status"]
            .eq("single_property")
            .sum()
        ),
        "multi_property_permit_count": int(
            mapping["location_mapping_status"]
            .eq("multi_property")
            .sum()
        ),
        "no_property_match_permit_count": int(
            mapping["location_mapping_status"]
            .eq("no_property_match")
            .sum()
        ),
        "partial_spatial_mapping_permit_count": int(
            mapping["has_partial_spatial_mapping"].sum()
        ),
        "flood_exposed_permit_count": int(
            context["is_flood_exposed"].eq(True).sum()
        ),
    }

    return bridge, context, summary


def _attach_permit_level_context(
    *,
    context: pd.DataFrame,
    bridge: pd.DataFrame,
) -> pd.DataFrame:
    if bridge.empty:
        context["mapped_assessed_value_total_sum"] = pd.NA
        context["mapped_assessed_value_residential_sum"] = pd.NA
        context["mapped_assessed_value_non_residential_sum"] = pd.NA
        context["flood_exposed_property_location_count"] = 0
        context["is_flood_exposed"] = pd.NA
        return context

    working = bridge.copy()

    working["_flood_exposed"] = (
        working["is_flood_exposed"]
        .eq(True)
        .astype("int64")
    )

    working["_regulatory"] = (
        working["intersects_regulatory_flood_layer"]
        .eq(True)
        .astype("int64")
    )

    working["_river_channel"] = (
        working["intersects_normal_river_channel"]
        .eq(True)
        .astype("int64")
    )

    aggregated = (
        working.groupby(
            "development_permit_key",
            sort=False,
        )
        .agg(
            mapped_assessed_value_total_sum=(
                "assessed_value_total_sum",
                _sum_min_count,
            ),
            mapped_assessed_value_residential_sum=(
                "assessed_value_residential_sum",
                _sum_min_count,
            ),
            mapped_assessed_value_non_residential_sum=(
                "assessed_value_non_residential_sum",
                _sum_min_count,
            ),
            mapped_assessed_value_farmland_sum=(
                "assessed_value_farmland_sum",
                _sum_min_count,
            ),
            flood_exposed_property_location_count=(
                "_flood_exposed",
                "sum",
            ),
            regulatory_property_location_count=(
                "_regulatory",
                "sum",
            ),
            normal_river_channel_property_location_count=(
                "_river_channel",
                "sum",
            ),
        )
        .reset_index()
    )

    aggregated["is_flood_exposed"] = (
        aggregated["flood_exposed_property_location_count"] > 0
    )

    aggregated["intersects_regulatory_flood_layer"] = (
        aggregated["regulatory_property_location_count"] > 0
    )

    aggregated["intersects_normal_river_channel"] = (
        aggregated["normal_river_channel_property_location_count"] > 0
    )

    result = context.merge(
        aggregated,
        on="development_permit_key",
        how="left",
        validate="one_to_one",
    )

    count_columns = [
        "flood_exposed_property_location_count",
        "regulatory_property_location_count",
        "normal_river_channel_property_location_count",
    ]

    for column in count_columns:
        result[column] = (
            result[column]
            .fillna(0)
            .astype("int64")
        )

    return result


def _extract_points(geometry_text) -> list[Point]:
    if (
        geometry_text is None
        or pd.isna(geometry_text)
        or str(geometry_text).strip() == ""
    ):
        return []

    geometry = wkt.loads(str(geometry_text))

    if isinstance(geometry, Point):
        return [geometry]

    if isinstance(geometry, MultiPoint):
        return list(geometry.geoms)

    raise CalgaryDevelopmentPermitContextError(
        "Unexpected development-permit "
        f"locations geometry type: {geometry.geom_type}"
    )


def _count_titled_parcels(value) -> int:
    if value is None or pd.isna(value):
        return 0

    return sum(
        token.strip() == "Titled Parcel"
        for token in str(value).split(";")
    )


def _sum_min_count(values: pd.Series) -> float:
    return values.sum(min_count=1)