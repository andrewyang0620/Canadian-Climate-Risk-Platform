from __future__ import annotations

from typing import Any

import pandas as pd


CITY = "vancouver"

SCENARIO_COLUMNS = {
    "Designated Floodplain (FCL 4.6m)": "designated_floodplain",
    "Fraser Risk Today 1/500 Storm": "fraser_risk_today",
    "Still Creek Floodplain": "still_creek_floodplain",
    "Wave Effect Zone": "wave_effect_zone",
}

REQUIRED_PARCEL_COLUMNS = {
    "property_parcel_key",
    "city",
    "province",
    "source_name",
    "source_parcel_id",
    "source_tax_coord",
    "address_text",
    "geometry_wkt",
}

REQUIRED_SCENARIO_COLUMNS = {
    "property_parcel_key",
    "scenario_name",
    "source_zone_count",
    "scenario_intersection_area_sq_m",
    "scenario_overlap_ratio",
}


class VancouverParcelFloodExposureError(Exception):
    """Raised when Vancouver parcel flood exposure cannot be built."""


def build_gold_vancouver_parcel_flood_exposure(
    *,
    parcel_dataframe: pd.DataFrame,
    scenario_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build one-row-per-parcel Vancouver flood exposure Gold.

    All Vancouver parcels are retained, including parcels with no
    positive-area flood exposure.

    Different flood scenarios remain separate. No overlap areas or
    ratios are summed across scenarios.
    """
    _require_columns(
        dataframe=parcel_dataframe,
        required_columns=REQUIRED_PARCEL_COLUMNS,
        table_name="silver_property_parcel",
    )

    _require_columns(
        dataframe=scenario_dataframe,
        required_columns=REQUIRED_SCENARIO_COLUMNS,
        table_name=(
            "gold_vancouver_parcel_flood_scenario_exposure"
        ),
    )

    parcels = parcel_dataframe[
        parcel_dataframe["city"]
        .astype("string")
        .str.lower()
        .eq(CITY)
    ].copy()

    if parcels.empty:
        raise VancouverParcelFloodExposureError(
            "No Vancouver parcel rows were found."
        )

    if parcels["property_parcel_key"].isna().any():
        raise VancouverParcelFloodExposureError(
            "silver_property_parcel contains null "
            "property_parcel_key values."
        )

    if parcels["property_parcel_key"].duplicated().any():
        raise VancouverParcelFloodExposureError(
            "silver_property_parcel contains duplicate "
            "property_parcel_key values."
        )

    scenarios = scenario_dataframe.copy()

    if scenarios.empty:
        raise VancouverParcelFloodExposureError(
            "Scenario exposure input is empty."
        )

    if (
        scenarios["property_parcel_key"].isna().any()
        or scenarios["scenario_name"].isna().any()
    ):
        raise VancouverParcelFloodExposureError(
            "Scenario exposure contains null parcel keys "
            "or scenario names."
        )

    if scenarios.duplicated(
        subset=[
            "property_parcel_key",
            "scenario_name",
        ]
    ).any():
        raise VancouverParcelFloodExposureError(
            "Scenario exposure contains duplicate "
            "parcel × scenario rows."
        )

    actual_scenarios = set(
        scenarios["scenario_name"].astype(str).unique()
    )

    unknown_scenarios = (
        actual_scenarios - set(SCENARIO_COLUMNS)
    )

    if unknown_scenarios:
        raise VancouverParcelFloodExposureError(
            "Unexpected Vancouver flood scenarios: "
            f"{sorted(unknown_scenarios)}"
        )

    parcel_keys = set(
        parcels["property_parcel_key"].astype(str)
    )
    scenario_parcel_keys = set(
        scenarios["property_parcel_key"].astype(str)
    )

    orphan_parcel_keys = (
        scenario_parcel_keys - parcel_keys
    )

    if orphan_parcel_keys:
        raise VancouverParcelFloodExposureError(
            "Scenario exposure contains parcel keys not "
            "present in silver_property_parcel. "
            f"Example: {sorted(orphan_parcel_keys)[:10]}"
        )

    result = parcels[
        [
            "property_parcel_key",
            "city",
            "province",
            "source_name",
            "source_parcel_id",
            "source_tax_coord",
            "address_text",
            "geometry_wkt",
        ]
    ].copy()

    for scenario_name, prefix in SCENARIO_COLUMNS.items():
        scenario_subset = scenarios[
            scenarios["scenario_name"].eq(
                scenario_name
            )
        ][
            [
                "property_parcel_key",
                "source_zone_count",
                "scenario_intersection_area_sq_m",
                "scenario_overlap_ratio",
            ]
        ].copy()

        scenario_subset = scenario_subset.rename(
            columns={
                "source_zone_count": (
                    f"{prefix}_source_zone_count"
                ),
                "scenario_intersection_area_sq_m": (
                    f"{prefix}_overlap_area_sq_m"
                ),
                "scenario_overlap_ratio": (
                    f"{prefix}_overlap_ratio"
                ),
            }
        )

        result = result.merge(
            scenario_subset,
            on="property_parcel_key",
            how="left",
            validate="one_to_one",
        )

        source_zone_count_column = (
            f"{prefix}_source_zone_count"
        )
        overlap_area_column = (
            f"{prefix}_overlap_area_sq_m"
        )
        overlap_ratio_column = (
            f"{prefix}_overlap_ratio"
        )
        flag_column = f"{prefix}_flag"

        result[source_zone_count_column] = (
            result[source_zone_count_column]
            .fillna(0)
            .astype("int64")
        )

        result[overlap_area_column] = (
            result[overlap_area_column]
            .fillna(0.0)
            .astype(float)
        )

        result[overlap_ratio_column] = (
            result[overlap_ratio_column]
            .fillna(0.0)
            .astype(float)
        )

        result[flag_column] = (
            result[overlap_ratio_column] > 0
        )

    flag_columns = [
        f"{prefix}_flag"
        for prefix in SCENARIO_COLUMNS.values()
    ]

    source_zone_count_columns = [
        f"{prefix}_source_zone_count"
        for prefix in SCENARIO_COLUMNS.values()
    ]

    result["scenario_count"] = (
        result[flag_columns]
        .sum(axis=1)
        .astype("int64")
    )

    result["source_zone_count"] = (
        result[source_zone_count_columns]
        .sum(axis=1)
        .astype("int64")
    )

    result["is_flood_exposed"] = (
        result["scenario_count"] > 0
    )

    if len(result) != len(parcels):
        raise VancouverParcelFloodExposureError(
            "Parcel row conservation failed: "
            f"input={len(parcels)}, output={len(result)}."
        )

    if result["property_parcel_key"].duplicated().any():
        raise VancouverParcelFloodExposureError(
            "Final Vancouver parcel exposure contains "
            "duplicate property_parcel_key values."
        )

    result = result[
        [
            "property_parcel_key",
            "city",
            "province",
            "source_name",
            "source_parcel_id",
            "source_tax_coord",
            "address_text",
            "geometry_wkt",
            "is_flood_exposed",
            "scenario_count",
            "source_zone_count",
            "designated_floodplain_flag",
            "designated_floodplain_source_zone_count",
            "designated_floodplain_overlap_area_sq_m",
            "designated_floodplain_overlap_ratio",
            "fraser_risk_today_flag",
            "fraser_risk_today_source_zone_count",
            "fraser_risk_today_overlap_area_sq_m",
            "fraser_risk_today_overlap_ratio",
            "still_creek_floodplain_flag",
            "still_creek_floodplain_source_zone_count",
            "still_creek_floodplain_overlap_area_sq_m",
            "still_creek_floodplain_overlap_ratio",
            "wave_effect_zone_flag",
            "wave_effect_zone_source_zone_count",
            "wave_effect_zone_overlap_area_sq_m",
            "wave_effect_zone_overlap_ratio",
        ]
    ].sort_values(
        "property_parcel_key"
    ).reset_index(drop=True)

    exposed = result[
        result["is_flood_exposed"]
    ]

    scenario_parcel_counts = {
        prefix: int(result[f"{prefix}_flag"].sum())
        for prefix in SCENARIO_COLUMNS.values()
    }

    summary = {
        "parcel_input_count": int(len(parcels)),
        "output_row_count": int(len(result)),
        "exposed_parcel_count": int(len(exposed)),
        "unexposed_parcel_count": int(
            len(result) - len(exposed)
        ),
        "parcel_exposure_rate": (
            len(exposed) / len(result)
        ),
        "scenario_membership_row_count": int(
            result["scenario_count"].sum()
        ),
        "source_zone_membership_count": int(
            result["source_zone_count"].sum()
        ),
        "multi_scenario_parcel_count": int(
            (result["scenario_count"] > 1).sum()
        ),
        "maximum_scenarios_per_parcel": int(
            result["scenario_count"].max()
        ),
        "scenario_parcel_counts": (
            scenario_parcel_counts
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
        raise VancouverParcelFloodExposureError(
            f"{table_name} is missing columns: "
            f"{sorted(missing_columns)}"
        )