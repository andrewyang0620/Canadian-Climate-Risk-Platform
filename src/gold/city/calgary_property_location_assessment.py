from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


CITY = "calgary"

REQUIRED_COLUMNS = {
    "property_assessment_key",
    "city",
    "source_property_id",
    "source_parcel_id",
    "source_unique_key",
    "assessment_year",
    "assessed_value_total",
    "assessed_value_residential",
    "assessed_value_non_residential",
    "assessed_value_farmland",
    "assessment_class",
    "assessment_class_description",
    "community_code",
    "community_name",
    "year_of_construction",
    "land_use_designation",
    "property_type",
    "sub_property_use",
    "geometry_wkt",
    "source_name",
}


class CalgaryPropertyLocationAssessmentError(Exception):
    pass


def build_gold_calgary_property_location_assessment(
    *,
    assessment_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    missing = REQUIRED_COLUMNS - set(assessment_dataframe.columns)
    if missing:
        raise CalgaryPropertyLocationAssessmentError(
            f"silver_property_assessment missing columns: {sorted(missing)}"
        )

    dataframe = assessment_dataframe[
        assessment_dataframe["city"].astype("string").str.lower().eq(CITY)
        & assessment_dataframe["source_parcel_id"].notna()
        & assessment_dataframe["assessment_year"].notna()
    ].copy()

    if dataframe.empty:
        raise CalgaryPropertyLocationAssessmentError(
            "No usable Calgary assessment rows."
        )

    dataframe["source_parcel_id"] = (
        dataframe["source_parcel_id"].astype("string").str.strip()
    )
    dataframe["assessment_year"] = dataframe["assessment_year"].astype(int)

    result = (
        dataframe.groupby(
            ["source_parcel_id", "assessment_year"],
            sort=True,
            dropna=False,
        )
        .agg(
            assessment_record_count=("property_assessment_key", "size"),
            distinct_property_count=("source_property_id", "nunique"),
            distinct_source_unique_key_count=("source_unique_key", "nunique"),
            assessed_value_total_sum=("assessed_value_total", _sum_min_count),
            assessed_value_residential_sum=(
                "assessed_value_residential",
                _sum_min_count,
            ),
            assessed_value_non_residential_sum=(
                "assessed_value_non_residential",
                _sum_min_count,
            ),
            assessed_value_farmland_sum=(
                "assessed_value_farmland",
                _sum_min_count,
            ),
            assessment_class=("assessment_class", _single_unique_value),
            assessment_class_count=("assessment_class", "nunique"),
            assessment_class_description=(
                "assessment_class_description",
                _single_unique_value,
            ),
            community_code=("community_code", _single_unique_value),
            community_name=("community_name", _single_unique_value),
            community_count=("community_code", "nunique"),
            land_use_designation=(
                "land_use_designation",
                _single_unique_value,
            ),
            land_use_designation_count=("land_use_designation", "nunique"),
            property_type=("property_type", _single_unique_value),
            property_type_count=("property_type", "nunique"),
            year_of_construction_min=("year_of_construction", "min"),
            year_of_construction_max=("year_of_construction", "max"),
            geometry_wkt=("geometry_wkt", _single_unique_value),
            geometry_count=("geometry_wkt", "nunique"),
            source_name=("source_name", _single_unique_value),
        )
        .reset_index()
    )

    invalid_geometry = result["geometry_count"] != 1
    if invalid_geometry.any():
        raise CalgaryPropertyLocationAssessmentError(
            f"{int(invalid_geometry.sum())} location/year groups do not "
            "have exactly one geometry."
        )

    if (
        result["distinct_source_unique_key_count"]
        != result["assessment_record_count"]
    ).any():
        raise CalgaryPropertyLocationAssessmentError(
            "source_unique_key is not unique within some location/year groups."
        )

    result["has_multiple_assessment_records"] = (
        result["assessment_record_count"] > 1
    )
    result["has_multiple_assessment_classes"] = (
        result["assessment_class_count"] > 1
    )
    result["has_multiple_communities"] = result["community_count"] > 1
    result["has_multiple_land_use_designations"] = (
        result["land_use_designation_count"] > 1
    )
    result["has_multiple_property_types"] = (
        result["property_type_count"] > 1
    )

    result["property_location_assessment_key"] = [
        _build_key(
            source_parcel_id=source_parcel_id,
            assessment_year=assessment_year,
        )
        for source_parcel_id, assessment_year in zip(
            result["source_parcel_id"],
            result["assessment_year"],
            strict=True,
        )
    ]

    if result["property_location_assessment_key"].duplicated().any():
        raise CalgaryPropertyLocationAssessmentError(
            "Duplicate property-location assessment keys."
        )

    summary = {
        "input_row_count": int(len(dataframe)),
        "output_row_count": int(len(result)),
        "distinct_source_parcel_count": int(
            result["source_parcel_id"].nunique()
        ),
        "assessment_year_min": int(result["assessment_year"].min()),
        "assessment_year_max": int(result["assessment_year"].max()),
        "multi_record_location_count": int(
            result["has_multiple_assessment_records"].sum()
        ),
        "rows represented_by_multi_record_locations": int(
            result.loc[
                result["has_multiple_assessment_records"],
                "assessment_record_count",
            ].sum()
        ),
        "maximum_assessment_records_per_location": int(
            result["assessment_record_count"].max()
        ),
        "multiple_assessment_class_location_count": int(
            result["has_multiple_assessment_classes"].sum()
        ),
        "multiple_community_location_count": int(
            result["has_multiple_communities"].sum()
        ),
        "multiple_land_use_location_count": int(
            result["has_multiple_land_use_designations"].sum()
        ),
        "multiple_property_type_location_count": int(
            result["has_multiple_property_types"].sum()
        ),
    }

    result = (
        result[
            [
                "property_location_assessment_key",
                "source_parcel_id",
                "assessment_year",
                "assessment_record_count",
                "has_multiple_assessment_records",
                "distinct_property_count",
                "distinct_source_unique_key_count",
                "assessed_value_total_sum",
                "assessed_value_residential_sum",
                "assessed_value_non_residential_sum",
                "assessed_value_farmland_sum",
                "assessment_class",
                "assessment_class_count",
                "has_multiple_assessment_classes",
                "community_code",
                "community_name",
                "community_count",
                "has_multiple_communities",
                "land_use_designation",
                "land_use_designation_count",
                "has_multiple_land_use_designations",
                "property_type",
                "property_type_count",
                "has_multiple_property_types",
                "year_of_construction_min",
                "year_of_construction_max",
                "geometry_wkt",
                "source_name",
            ]
        ]
        .sort_values(["assessment_year", "source_parcel_id"])
        .reset_index(drop=True)
    )

    return result, summary


def _sum_min_count(values: pd.Series) -> float:
    return values.sum(min_count=1)


def _single_unique_value(values: pd.Series) -> Any:
    unique = values.dropna().drop_duplicates()

    if len(unique) == 1:
        return unique.iloc[0]

    return None


def _build_key(
    *,
    source_parcel_id: str,
    assessment_year: int,
) -> str:
    identity = {
        "source_parcel_id": source_parcel_id,
        "assessment_year": assessment_year,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"calgary_{digest}"