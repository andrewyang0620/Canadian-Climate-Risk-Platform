from __future__ import annotations

from typing import Any

import pandas as pd


CITY = "vancouver"

REQUIRED_PARCEL_COLUMNS = {
    "property_parcel_key",
    "city",
    "source_parcel_id",
    "source_tax_coord",
}

REQUIRED_BRIDGE_COLUMNS = {
    "property_parcel_key",
    "source_land_coordinate",
    "parcel_count_for_land_coordinate",
    "is_ambiguous_land_coordinate",
    "mapping_method",
}

REQUIRED_ASSESSMENT_COLUMNS = {
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
    "neighbourhood_code",
    "has_multiple_neighbourhood_codes",
}


class VancouverParcelAssessmentContextError(Exception):
    """Raised when Vancouver parcel assessment context cannot be built."""


def build_gold_vancouver_parcel_assessment_context(
    *,
    parcel_dataframe: pd.DataFrame,
    bridge_dataframe: pd.DataFrame,
    assessment_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build latest assessment context at one-row-per-parcel grain.

    Assessment values remain land-coordinate aggregates.

    Exact parcel-level value fields are populated only when one
    land coordinate maps to exactly one parcel.
    """
    _require_columns(
        dataframe=parcel_dataframe,
        required_columns=REQUIRED_PARCEL_COLUMNS,
        table_name="silver_property_parcel",
    )

    _require_columns(
        dataframe=bridge_dataframe,
        required_columns=REQUIRED_BRIDGE_COLUMNS,
        table_name="gold_vancouver_property_parcel_bridge",
    )

    _require_columns(
        dataframe=assessment_dataframe,
        required_columns=REQUIRED_ASSESSMENT_COLUMNS,
        table_name="gold_vancouver_land_coordinate_assessment",
    )

    parcels = parcel_dataframe[
        parcel_dataframe["city"]
        .astype("string")
        .str.lower()
        .eq(CITY)
    ][
        [
            "property_parcel_key",
            "source_parcel_id",
            "source_tax_coord",
        ]
    ].copy()

    if parcels.empty:
        raise VancouverParcelAssessmentContextError(
            "No Vancouver parcels were found."
        )

    if parcels["property_parcel_key"].isna().any():
        raise VancouverParcelAssessmentContextError(
            "Parcel keys contain null values."
        )

    if parcels["property_parcel_key"].duplicated().any():
        raise VancouverParcelAssessmentContextError(
            "Parcel keys must be unique."
        )

    bridge = bridge_dataframe[
        [
            "property_parcel_key",
            "source_land_coordinate",
            "parcel_count_for_land_coordinate",
            "is_ambiguous_land_coordinate",
            "mapping_method",
        ]
    ].copy()

    if bridge["property_parcel_key"].duplicated().any():
        raise VancouverParcelAssessmentContextError(
            "Bridge must contain at most one row per parcel."
        )

    latest_report_year = int(
        assessment_dataframe["report_year"].max()
    )

    latest = assessment_dataframe[
        assessment_dataframe["report_year"].eq(
            latest_report_year
        )
    ].copy()

    if latest["source_land_coordinate"].duplicated().any():
        raise VancouverParcelAssessmentContextError(
            "Latest assessment must contain one row per "
            "source_land_coordinate."
        )

    latest = latest.rename(
        columns={
            "current_land_value_sum": (
                "land_coordinate_current_land_value"
            ),
            "current_improvement_value_sum": (
                "land_coordinate_current_improvement_value"
            ),
            "current_total_assessed_value_sum": (
                "land_coordinate_current_total_assessed_value"
            ),
            "previous_land_value_sum": (
                "land_coordinate_previous_land_value"
            ),
            "previous_improvement_value_sum": (
                "land_coordinate_previous_improvement_value"
            ),
            "previous_total_assessed_value_sum": (
                "land_coordinate_previous_total_assessed_value"
            ),
            "tax_levy_sum": (
                "land_coordinate_tax_levy"
            ),
        }
    )

    result = parcels.merge(
        bridge,
        on="property_parcel_key",
        how="left",
        validate="one_to_one",
    )

    result = result.merge(
        latest[
            [
                "source_land_coordinate",
                "report_year",
                "tax_assessment_year",
                "assessment_record_count",
                "distinct_folio_count",
                "distinct_pid_count",
                "land_coordinate_current_land_value",
                "land_coordinate_current_improvement_value",
                "land_coordinate_current_total_assessed_value",
                "land_coordinate_previous_land_value",
                "land_coordinate_previous_improvement_value",
                "land_coordinate_previous_total_assessed_value",
                "land_coordinate_tax_levy",
                "zoning_district",
                "zoning_classification",
                "neighbourhood_code",
                "has_multiple_neighbourhood_codes",
            ]
        ],
        on="source_land_coordinate",
        how="left",
        validate="many_to_one",
    )

    result["has_parcel_bridge"] = (
        result["source_land_coordinate"].notna()
    )

    result["has_latest_assessment"] = (
        result["report_year"].notna()
    )

    result["assessment_mapping_ambiguous"] = (
        result["is_ambiguous_land_coordinate"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )

    result["assessment_mapping_exact_1_to_1"] = (
        result["has_latest_assessment"]
        & result["has_parcel_bridge"]
        & ~result["assessment_mapping_ambiguous"]
        & result[
            "parcel_count_for_land_coordinate"
        ].eq(1)
    )

    exact_mask = result[
        "assessment_mapping_exact_1_to_1"
    ]

    result[
        "exact_mapped_current_land_value"
    ] = result[
        "land_coordinate_current_land_value"
    ].where(exact_mask)

    result[
        "exact_mapped_current_improvement_value"
    ] = result[
        "land_coordinate_current_improvement_value"
    ].where(exact_mask)

    result[
        "exact_mapped_current_total_assessed_value"
    ] = result[
        "land_coordinate_current_total_assessed_value"
    ].where(exact_mask)

    result[
        "exact_mapped_tax_levy"
    ] = result[
        "land_coordinate_tax_levy"
    ].where(exact_mask)

    if len(result) != len(parcels):
        raise VancouverParcelAssessmentContextError(
            "Parcel row conservation failed."
        )

    if result["property_parcel_key"].duplicated().any():
        raise VancouverParcelAssessmentContextError(
            "Final parcel assessment context contains "
            "duplicate parcel keys."
        )

    result = result[
        [
            "property_parcel_key",
            "source_parcel_id",
            "source_tax_coord",
            "source_land_coordinate",
            "has_parcel_bridge",
            "has_latest_assessment",
            "parcel_count_for_land_coordinate",
            "is_ambiguous_land_coordinate",
            "mapping_method",
            "assessment_mapping_ambiguous",
            "assessment_mapping_exact_1_to_1",
            "report_year",
            "tax_assessment_year",
            "assessment_record_count",
            "distinct_folio_count",
            "distinct_pid_count",
            "land_coordinate_current_land_value",
            "land_coordinate_current_improvement_value",
            "land_coordinate_current_total_assessed_value",
            "land_coordinate_previous_land_value",
            "land_coordinate_previous_improvement_value",
            "land_coordinate_previous_total_assessed_value",
            "land_coordinate_tax_levy",
            "exact_mapped_current_land_value",
            "exact_mapped_current_improvement_value",
            "exact_mapped_current_total_assessed_value",
            "exact_mapped_tax_levy",
            "zoning_district",
            "zoning_classification",
            "neighbourhood_code",
            "has_multiple_neighbourhood_codes",
        ]
    ].sort_values(
        "property_parcel_key"
    ).reset_index(drop=True)

    summary = {
        "parcel_input_count": int(len(parcels)),
        "output_row_count": int(len(result)),
        "latest_report_year": latest_report_year,
        "bridge_parcel_count": int(
            result["has_parcel_bridge"].sum()
        ),
        "latest_assessment_parcel_count": int(
            result["has_latest_assessment"].sum()
        ),
        "exact_1_to_1_assessment_parcel_count": int(
            result[
                "assessment_mapping_exact_1_to_1"
            ].sum()
        ),
        "ambiguous_assessment_parcel_count": int(
            (
                result["has_latest_assessment"]
                & result[
                    "assessment_mapping_ambiguous"
                ]
            ).sum()
        ),
        "parcel_without_bridge_count": int(
            (~result["has_parcel_bridge"]).sum()
        ),
        "bridge_without_latest_assessment_count": int(
            (
                result["has_parcel_bridge"]
                & ~result["has_latest_assessment"]
            ).sum()
        ),
        "latest_assessment_context_rate": float(
            result["has_latest_assessment"].mean()
        ),
    }

    return result, summary


def _require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = (
        required_columns - set(dataframe.columns)
    )

    if missing_columns:
        raise VancouverParcelAssessmentContextError(
            f"{table_name} is missing columns: "
            f"{sorted(missing_columns)}"
        )