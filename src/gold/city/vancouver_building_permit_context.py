from __future__ import annotations

from typing import Any

import pandas as pd
from pyproj import Transformer
from shapely import make_valid, wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree


CITY = "vancouver"
SOURCE_CRS = "EPSG:4326"
TARGET_CRS = "EPSG:3347"

HOUSING_ACTIVITY_MAP = {
    "New Building": "new_building",
    "Addition / Alteration": "renovation",
    "Demolition / Deconstruction": "demolition",
    "Salvage and Abatement": "salvage_abatement",
    "Temporary Building / Structure": "temporary",
    "Outdoor Uses (No Buildings Proposed)": "other",
}

REQUIRED_PERMIT_COLUMNS = {
    "building_permit_key",
    "city",
    "permit_number",
    "permit_type_mapped",
    "permit_class_group",
    "permit_class_mapped",
    "work_class_mapped",
    "issue_date",
    "issue_year",
    "year_month",
    "address_text",
    "project_description",
    "estimated_project_cost",
    "neighbourhood_name",
    "latitude",
    "longitude",
    "geometry_wkt",
}

REQUIRED_PARCEL_COLUMNS = {
    "property_parcel_key",
    "city",
    "geometry_wkt",
}

REQUIRED_FLOOD_COLUMNS = {
    "property_parcel_key",
    "is_flood_exposed",
    "scenario_count",
    "designated_floodplain_flag",
    "designated_floodplain_overlap_ratio",
    "fraser_risk_today_flag",
    "fraser_risk_today_overlap_ratio",
    "still_creek_floodplain_flag",
    "still_creek_floodplain_overlap_ratio",
    "wave_effect_zone_flag",
    "wave_effect_zone_overlap_ratio",
}

REQUIRED_ASSESSMENT_COLUMNS = {
    "property_parcel_key",
    "has_latest_assessment",
    "assessment_mapping_ambiguous",
    "assessment_mapping_exact_1_to_1",
    "report_year",
    "land_coordinate_current_land_value",
    "land_coordinate_current_improvement_value",
    "land_coordinate_current_total_assessed_value",
    "exact_mapped_current_land_value",
    "exact_mapped_current_improvement_value",
    "exact_mapped_current_total_assessed_value",
}


class VancouverBuildingPermitContextError(Exception):
    pass


def build_gold_vancouver_building_permit_context(
    *,
    permit_dataframe: pd.DataFrame,
    parcel_dataframe: pd.DataFrame,
    flood_dataframe: pd.DataFrame,
    assessment_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _require(
        permit_dataframe,
        REQUIRED_PERMIT_COLUMNS,
        "silver_building_permit",
    )
    _require(
        parcel_dataframe,
        REQUIRED_PARCEL_COLUMNS,
        "silver_property_parcel",
    )
    _require(
        flood_dataframe,
        REQUIRED_FLOOD_COLUMNS,
        "gold_vancouver_parcel_flood_exposure",
    )
    _require(
        assessment_dataframe,
        REQUIRED_ASSESSMENT_COLUMNS,
        "gold_vancouver_parcel_assessment_context",
    )

    permits = permit_dataframe[
        permit_dataframe["city"]
        .astype("string")
        .str.lower()
        .eq(CITY)
    ].copy()

    parcels = parcel_dataframe[
        parcel_dataframe["city"]
        .astype("string")
        .str.lower()
        .eq(CITY)
    ].copy()

    if permits["building_permit_key"].isna().any():
        raise VancouverBuildingPermitContextError(
            "Null building_permit_key."
        )

    if permits["building_permit_key"].duplicated().any():
        raise VancouverBuildingPermitContextError(
            "Duplicate building_permit_key."
        )

    # Housing classification
    permits["is_housing_related"] = permits[
        "permit_class_group"
    ].apply(_contains_dwelling_use)

    permits["housing_activity_type"] = "non_housing"

    housing_mask = permits["is_housing_related"]

    permits.loc[housing_mask, "housing_activity_type"] = (
        permits.loc[housing_mask, "permit_type_mapped"]
        .map(HOUSING_ACTIVITY_MAP)
        .fillna("other")
    )

    permits["is_new_housing_building_permit"] = (
        housing_mask
        & permits["permit_type_mapped"].eq("New Building")
    )

    permits["is_housing_renovation_permit"] = (
        housing_mask
        & permits["permit_type_mapped"].eq("Addition / Alteration")
    )

    permits["is_housing_demolition_permit"] = (
        housing_mask
        & permits["permit_type_mapped"].eq(
            "Demolition / Deconstruction"
        )
    )

    permits["is_housing_salvage_abatement_permit"] = (
        housing_mask
        & permits["permit_type_mapped"].eq(
            "Salvage and Abatement"
        )
    )

    # Prepare parcel polygons
    transformer = Transformer.from_crs(
        SOURCE_CRS,
        TARGET_CRS,
        always_xy=True,
    )

    parcel_geometries = []
    parcel_keys = []
    repaired_parcel_count = 0

    for row in parcels.itertuples(index=False):
        geometry = wkt.loads(row.geometry_wkt)

        if not geometry.is_valid:
            geometry = make_valid(geometry)
            repaired_parcel_count += 1

        geometry = _polygonal_only(geometry)

        if geometry is None or geometry.is_empty:
            continue

        geometry = transform(
            transformer.transform,
            geometry,
        )

        parcel_geometries.append(geometry)
        parcel_keys.append(row.property_parcel_key)

    tree = STRtree(parcel_geometries)

    # Match permit points to parcels
    parcel_match_counts = []
    parcel_mapping_statuses = []
    matched_parcel_keys = []
    spatial_geometry_flags = []

    for geometry_text in permits["geometry_wkt"]:
        if (
            geometry_text is None
            or pd.isna(geometry_text)
            or str(geometry_text).strip() == ""
        ):
            spatial_geometry_flags.append(False)
            parcel_match_counts.append(0)
            parcel_mapping_statuses.append("no_geometry")
            matched_parcel_keys.append(pd.NA)
            continue

        point = wkt.loads(str(geometry_text))

        if (
            point.is_empty
            or not point.is_valid
            or point.geom_type != "Point"
        ):
            spatial_geometry_flags.append(False)
            parcel_match_counts.append(0)
            parcel_mapping_statuses.append("invalid_geometry")
            matched_parcel_keys.append(pd.NA)
            continue

        spatial_geometry_flags.append(True)

        point = transform(
            transformer.transform,
            point,
        )

        candidate_indices = tree.query(
            point,
            predicate="intersects",
        )

        matches = {
            parcel_keys[int(index)]
            for index in candidate_indices
        }

        match_count = len(matches)
        parcel_match_counts.append(match_count)

        if match_count == 0:
            parcel_mapping_statuses.append("no_parcel_match")
            matched_parcel_keys.append(pd.NA)

        elif match_count == 1:
            parcel_mapping_statuses.append("exact_1_to_1")
            matched_parcel_keys.append(next(iter(matches)))

        else:
            parcel_mapping_statuses.append("ambiguous_1_to_many")
            matched_parcel_keys.append(pd.NA)

    permits["has_spatial_geometry"] = spatial_geometry_flags
    permits["parcel_match_count"] = parcel_match_counts
    permits["parcel_mapping_status"] = parcel_mapping_statuses
    permits["property_parcel_key"] = matched_parcel_keys

    # Attach parcel-level flood and assessment context
    flood = flood_dataframe[
        list(REQUIRED_FLOOD_COLUMNS)
    ].copy()

    assessment = assessment_dataframe[
        list(REQUIRED_ASSESSMENT_COLUMNS)
    ].copy()

    assessment = assessment.rename(
        columns={"report_year": "assessment_report_year"}
    )

    result = permits.merge(
        flood,
        on="property_parcel_key",
        how="left",
        validate="many_to_one",
    )

    result = result.merge(
        assessment,
        on="property_parcel_key",
        how="left",
        validate="many_to_one",
    )

    result = (
        result[
            [
                "building_permit_key",
                "permit_number",
                "issue_date",
                "issue_year",
                "year_month",
                "address_text",
                "project_description",
                "permit_type_mapped",
                "permit_class_group",
                "permit_class_mapped",
                "work_class_mapped",
                "is_housing_related",
                "housing_activity_type",
                "is_new_housing_building_permit",
                "is_housing_renovation_permit",
                "is_housing_demolition_permit",
                "is_housing_salvage_abatement_permit",
                "estimated_project_cost",
                "neighbourhood_name",
                "latitude",
                "longitude",
                "geometry_wkt",
                "has_spatial_geometry",
                "parcel_match_count",
                "parcel_mapping_status",
                "property_parcel_key",
                "is_flood_exposed",
                "scenario_count",
                "designated_floodplain_flag",
                "designated_floodplain_overlap_ratio",
                "fraser_risk_today_flag",
                "fraser_risk_today_overlap_ratio",
                "still_creek_floodplain_flag",
                "still_creek_floodplain_overlap_ratio",
                "wave_effect_zone_flag",
                "wave_effect_zone_overlap_ratio",
                "has_latest_assessment",
                "assessment_mapping_ambiguous",
                "assessment_mapping_exact_1_to_1",
                "assessment_report_year",
                "land_coordinate_current_land_value",
                "land_coordinate_current_improvement_value",
                "land_coordinate_current_total_assessed_value",
                "exact_mapped_current_land_value",
                "exact_mapped_current_improvement_value",
                "exact_mapped_current_total_assessed_value",
            ]
        ]
        .sort_values(
            [
                "issue_date",
                "building_permit_key",
            ]
        )
        .reset_index(drop=True)
    )

    summary = {
        "permit_input_count": int(len(permits)),
        "output_row_count": int(len(result)),
        "housing_related_permit_count": int(
            result["is_housing_related"].sum()
        ),
        "new_housing_building_permit_count": int(
            result["is_new_housing_building_permit"].sum()
        ),
        "housing_renovation_permit_count": int(
            result["is_housing_renovation_permit"].sum()
        ),
        "housing_demolition_permit_count": int(
            result["is_housing_demolition_permit"].sum()
        ),
        "housing_salvage_abatement_permit_count": int(
            result["is_housing_salvage_abatement_permit"].sum()
        ),
        "spatial_geometry_count": int(
            result["has_spatial_geometry"].sum()
        ),
        "exact_parcel_match_count": int(
            result["parcel_mapping_status"]
            .eq("exact_1_to_1")
            .sum()
        ),
        "ambiguous_parcel_match_count": int(
            result["parcel_mapping_status"]
            .eq("ambiguous_1_to_many")
            .sum()
        ),
        "no_parcel_match_count": int(
            result["parcel_mapping_status"]
            .eq("no_parcel_match")
            .sum()
        ),
        "no_geometry_count": int(
            result["parcel_mapping_status"]
            .eq("no_geometry")
            .sum()
        ),
        "repaired_parcel_geometry_count": int(
            repaired_parcel_count
        ),
        "housing_related_project_cost_sum": float(
            result.loc[
                result["is_housing_related"],
                "estimated_project_cost",
            ].sum()
        ),
        "exact_matched_housing_permit_count": int(
            (
                result["is_housing_related"]
                & result["parcel_mapping_status"].eq(
                    "exact_1_to_1"
                )
            ).sum()
        ),
        "flood_exposed_housing_permit_count": int(
            (
                result["is_housing_related"]
                & result["is_flood_exposed"].astype("boolean").fillna(False).astype(bool)
            ).sum()
        ),
    }

    return result, summary


def _contains_dwelling_use(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False

    tokens = {
        item.strip()
        for item in str(value).split(",")
        if item.strip()
    }

    return "Dwelling Uses" in tokens


def _polygonal_only(geometry):
    if isinstance(geometry, (Polygon, MultiPolygon)):
        return geometry

    if geometry.geom_type == "GeometryCollection":
        polygons = [
            part
            for part in geometry.geoms
            if isinstance(part, (Polygon, MultiPolygon))
        ]

        if polygons:
            return unary_union(polygons)

    return None


def _require(
    dataframe: pd.DataFrame,
    required: set[str],
    table_name: str,
) -> None:
    missing = required - set(dataframe.columns)

    if missing:
        raise VancouverBuildingPermitContextError(
            f"{table_name} missing columns: {sorted(missing)}"
        )