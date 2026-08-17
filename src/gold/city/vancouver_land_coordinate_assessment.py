from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = {
    "source_land_coordinate",
    "source_pid",
    "source_folio",
    "current_land_value",
    "current_improvement_value",
    "current_total_assessed_value",
    "previous_land_value",
    "previous_improvement_value",
    "previous_total_assessed_value",
    "tax_levy",
    "tax_assessment_year",
    "report_year",
    "zoning_district",
    "zoning_classification",
    "neighbourhood_code",
}


class VancouverLandCoordinateAssessmentError(Exception):
    """Raised when Vancouver assessment Gold cannot be built."""


def build_gold_vancouver_land_coordinate_assessment(
    *,
    property_tax_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Aggregate Vancouver property assessments to land-coordinate/year grain."""
    missing_columns = REQUIRED_COLUMNS - set(property_tax_dataframe.columns)

    if missing_columns:
        raise VancouverLandCoordinateAssessmentError(
            "silver_property_tax_assessment is missing columns: "
            f"{sorted(missing_columns)}"
        )

    dataframe = property_tax_dataframe.copy()

    dataframe["source_land_coordinate"] = (
        dataframe["source_land_coordinate"]
        .astype("string")
        .str.strip()
    )

    dataframe = dataframe[
        dataframe["source_land_coordinate"].notna()
        & dataframe["source_land_coordinate"].ne("")
        & dataframe["report_year"].notna()
    ].copy()

    if dataframe.empty:
        raise VancouverLandCoordinateAssessmentError(
            "No usable Vancouver property-tax rows were found."
        )

    dataframe["report_year"] = dataframe["report_year"].astype(int)

    group_columns = [
        "source_land_coordinate",
        "report_year",
    ]

    grouped = dataframe.groupby(
        group_columns,
        sort=True,
    )

    tax_year_counts = grouped["tax_assessment_year"].nunique(
        dropna=True
    )

    if (tax_year_counts > 1).any():
        raise VancouverLandCoordinateAssessmentError(
            "Some land-coordinate/year groups contain "
            "multiple tax_assessment_year values."
        )

    result = grouped.agg(
        assessment_record_count=(
            "source_folio",
            "size",
        ),
        distinct_folio_count=(
            "source_folio",
            "nunique",
        ),
        distinct_pid_count=(
            "source_pid",
            "nunique",
        ),
        current_land_value_sum=(
            "current_land_value",
            _sum_with_min_count,
        ),
        current_improvement_value_sum=(
            "current_improvement_value",
            _sum_with_min_count,
        ),
        current_total_assessed_value_sum=(
            "current_total_assessed_value",
            _sum_with_min_count,
        ),
        previous_land_value_sum=(
            "previous_land_value",
            _sum_with_min_count,
        ),
        previous_improvement_value_sum=(
            "previous_improvement_value",
            _sum_with_min_count,
        ),
        previous_total_assessed_value_sum=(
            "previous_total_assessed_value",
            _sum_with_min_count,
        ),
        tax_levy_sum=(
            "tax_levy",
            _sum_with_min_count,
        ),
        tax_assessment_year=(
            "tax_assessment_year",
            _single_unique_value,
        ),
        zoning_district=(
            "zoning_district",
            _single_unique_value,
        ),
        zoning_classification=(
            "zoning_classification",
            _single_unique_value,
        ),
        zoning_district_count=(
            "zoning_district",
            "nunique",
        ),
        neighbourhood_code=(
            "neighbourhood_code",
            _single_unique_value,
        ),
        neighbourhood_code_count=(
            "neighbourhood_code",
            "nunique",
        ),
    ).reset_index()

    result["has_multiple_neighbourhood_codes"] = (
        result["neighbourhood_code_count"] > 1
    )

    result["has_multiple_zoning_districts"] = (
        result["zoning_district_count"] > 1
    )

    result["land_coordinate_assessment_key"] = [
        build_land_coordinate_assessment_key(
            source_land_coordinate=land_coordinate,
            report_year=int(report_year),
        )
        for land_coordinate, report_year in zip(
            result["source_land_coordinate"],
            result["report_year"],
            strict=True,
        )
    ]

    result = (
        result[
            [
                "land_coordinate_assessment_key",
                "source_land_coordinate",
                "report_year",
                "tax_assessment_year",
                "assessment_record_count",
                "distinct_folio_count",
                "distinct_pid_count",
                "current_land_value_sum",
                "current_improvement_value_sum",
                "current_total_assessed_value_sum",
                "previous_land_value_sum",
                "previous_improvement_value_sum",
                "previous_total_assessed_value_sum",
                "tax_levy_sum",
                "zoning_district",
                "zoning_classification",
                "zoning_district_count",
                "has_multiple_zoning_districts",
                "neighbourhood_code",
                "neighbourhood_code_count",
                "has_multiple_neighbourhood_codes",
            ]
        ]
        .sort_values(
            [
                "report_year",
                "source_land_coordinate",
            ]
        )
        .reset_index(drop=True)
    )

    latest_report_year = int(result["report_year"].max())

    summary = {
        "input_row_count": int(len(property_tax_dataframe)),
        "usable_input_row_count": int(len(dataframe)),
        "output_row_count": int(len(result)),
        "distinct_land_coordinate_count": int(
            result["source_land_coordinate"].nunique()
        ),
        "report_year_min": int(result["report_year"].min()),
        "report_year_max": int(result["report_year"].max()),
        "multi_record_group_count": int(
            (result["assessment_record_count"] > 1).sum()
        ),
        "maximum_assessment_records_per_group": int(
            result["assessment_record_count"].max()
        ),
        "multiple_neighbourhood_group_count": int(
            result["has_multiple_neighbourhood_codes"].sum()
        ),
        "multiple_zoning_group_count": int(
            result["has_multiple_zoning_districts"].sum()
        ),
        "latest_report_year": latest_report_year,
        "latest_report_year_row_count": int(
            (result["report_year"] == latest_report_year).sum()
        ),
    }

    return result, summary


def build_land_coordinate_assessment_key(
    *,
    source_land_coordinate: str,
    report_year: int,
) -> str:
    identity = {
        "source_land_coordinate": source_land_coordinate,
        "report_year": report_year,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"


def _sum_with_min_count(series: pd.Series) -> Any:
    return series.sum(min_count=1)


def _single_unique_value(series: pd.Series) -> Any:
    values = series.dropna().drop_duplicates()

    if len(values) == 1:
        return values.iloc[0]

    return None