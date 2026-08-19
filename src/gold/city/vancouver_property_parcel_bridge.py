from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


REQUIRED_PARCEL_COLUMNS = {
    "property_parcel_key",
    "source_parcel_id",
    "source_tax_coord",
}

REQUIRED_TAX_COLUMNS = {
    "source_land_coordinate",
}


class VancouverPropertyParcelBridgeError(Exception):
    """Raised when the Vancouver property-to-parcel bridge cannot be built."""


def build_gold_vancouver_property_parcel_bridge(
    *,
    parcel_dataframe: pd.DataFrame,
    property_tax_dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build the Vancouver land-coordinate to parcel bridge.

    The verified join relationship is:

        property_tax.source_land_coordinate
            =
        property_parcel.source_tax_coord

    One land coordinate can map to multiple parcel records. Those mappings are
    retained and explicitly marked as ambiguous. No assessed-value allocation
    or parcel-area weighting is performed here.
    """
    _require_columns(
        dataframe=parcel_dataframe,
        required_columns=REQUIRED_PARCEL_COLUMNS,
        table_name="silver_property_parcel",
    )
    _require_columns(
        dataframe=property_tax_dataframe,
        required_columns=REQUIRED_TAX_COLUMNS,
        table_name="silver_property_tax_assessment",
    )

    if parcel_dataframe["property_parcel_key"].isna().any():
        raise VancouverPropertyParcelBridgeError(
            "silver_property_parcel contains null property_parcel_key values."
        )

    if parcel_dataframe["property_parcel_key"].duplicated().any():
        raise VancouverPropertyParcelBridgeError(
            "silver_property_parcel contains duplicate property_parcel_key values."
        )

    parcels = parcel_dataframe[
        [
            "property_parcel_key",
            "source_parcel_id",
            "source_tax_coord",
        ]
    ].copy()

    taxes = property_tax_dataframe[
        [
            "source_land_coordinate",
        ]
    ].copy()

    parcels["_land_coordinate"] = _normalize_key_series(
        parcels["source_tax_coord"]
    )
    taxes["_land_coordinate"] = _normalize_key_series(
        taxes["source_land_coordinate"]
    )

    parcel_candidates = parcels[
        parcels["_land_coordinate"].notna()
    ].copy()

    tax_candidates = taxes[
        taxes["_land_coordinate"].notna()
    ].copy()

    if parcel_candidates.empty:
        raise VancouverPropertyParcelBridgeError(
            "No Vancouver parcel records contain source_tax_coord."
        )

    if tax_candidates.empty:
        raise VancouverPropertyParcelBridgeError(
            "No Vancouver property-tax records contain source_land_coordinate."
        )

    parcel_counts = (
        parcel_candidates.groupby("_land_coordinate")[
            "property_parcel_key"
        ]
        .size()
        .rename("parcel_count_for_land_coordinate")
    )

    parcel_land_coordinates = set(
        parcel_candidates["_land_coordinate"].unique()
    )
    tax_land_coordinates = set(
        tax_candidates["_land_coordinate"].unique()
    )

    matched_land_coordinates = (
        parcel_land_coordinates & tax_land_coordinates
    )

    if not matched_land_coordinates:
        raise VancouverPropertyParcelBridgeError(
            "No matching land coordinates were found between "
            "Vancouver property tax and parcel Silver tables."
        )

    bridge = parcel_candidates[
        parcel_candidates["_land_coordinate"].isin(
            matched_land_coordinates
        )
    ].copy()

    bridge = bridge.merge(
        parcel_counts,
        left_on="_land_coordinate",
        right_index=True,
        how="left",
        validate="many_to_one",
    )

    bridge["source_land_coordinate"] = bridge["_land_coordinate"]

    bridge["is_ambiguous_land_coordinate"] = (
        bridge["parcel_count_for_land_coordinate"] > 1
    )

    bridge["mapping_method"] = bridge[
        "parcel_count_for_land_coordinate"
    ].map(
        lambda count: (
            "exact_land_coordinate_1_to_1"
            if count == 1
            else "exact_land_coordinate_1_to_many"
        )
    )

    bridge["property_parcel_bridge_key"] = [
        build_property_parcel_bridge_key(
            source_land_coordinate=land_coordinate,
            property_parcel_key=property_parcel_key,
        )
        for land_coordinate, property_parcel_key in zip(
            bridge["source_land_coordinate"],
            bridge["property_parcel_key"],
            strict=True,
        )
    ]

    bridge = bridge[
        [
            "property_parcel_bridge_key",
            "source_land_coordinate",
            "property_parcel_key",
            "source_parcel_id",
            "parcel_count_for_land_coordinate",
            "is_ambiguous_land_coordinate",
            "mapping_method",
        ]
    ]

    if bridge["property_parcel_bridge_key"].duplicated().any():
        raise VancouverPropertyParcelBridgeError(
            "Vancouver property-to-parcel bridge contains duplicate bridge keys."
        )

    duplicate_relationships = bridge.duplicated(
        subset=[
            "source_land_coordinate",
            "property_parcel_key",
        ]
    )

    if duplicate_relationships.any():
        raise VancouverPropertyParcelBridgeError(
            "Vancouver property-to-parcel bridge contains duplicate "
            "land-coordinate to parcel relationships."
        )

    bridge = bridge.sort_values(
        [
            "source_land_coordinate",
            "property_parcel_key",
        ]
    ).reset_index(drop=True)

    matched_tax_rows = int(
        tax_candidates["_land_coordinate"].isin(
            matched_land_coordinates
        ).sum()
    )

    unmatched_tax_land_coordinates = sorted(
        tax_land_coordinates - parcel_land_coordinates
    )

    unmatched_parcel_land_coordinates = sorted(
        parcel_land_coordinates - tax_land_coordinates
    )

    ambiguous_land_coordinate_count = int(
        bridge.loc[
            bridge["is_ambiguous_land_coordinate"],
            "source_land_coordinate",
        ].nunique()
    )

    summary = {
        "parcel_row_count": int(len(parcel_dataframe)),
        "parcel_with_tax_coord_count": int(
            len(parcel_candidates)
        ),
        "distinct_parcel_tax_coord_count": int(
            parcel_candidates["_land_coordinate"].nunique()
        ),
        "tax_row_count": int(len(property_tax_dataframe)),
        "tax_rows_with_land_coordinate_count": int(
            len(tax_candidates)
        ),
        "distinct_tax_land_coordinate_count": int(
            tax_candidates["_land_coordinate"].nunique()
        ),
        "matched_land_coordinate_count": int(
            len(matched_land_coordinates)
        ),
        "unmatched_tax_land_coordinate_count": int(
            len(unmatched_tax_land_coordinates)
        ),
        "unmatched_parcel_land_coordinate_count": int(
            len(unmatched_parcel_land_coordinates)
        ),
        "matched_tax_row_count": matched_tax_rows,
        "tax_row_match_rate": (
            matched_tax_rows / len(tax_candidates)
        ),
        "bridge_row_count": int(len(bridge)),
        "bridge_parcel_count": int(
            bridge["property_parcel_key"].nunique()
        ),
        "ambiguous_land_coordinate_count": (
            ambiguous_land_coordinate_count
        ),
        "ambiguous_bridge_row_count": int(
            bridge["is_ambiguous_land_coordinate"].sum()
        ),
        "maximum_parcel_count_for_land_coordinate": int(
            bridge["parcel_count_for_land_coordinate"].max()
        ),
        "unmatched_tax_land_coordinate_sample": (
            unmatched_tax_land_coordinates[:20]
        ),
        "unmatched_parcel_land_coordinate_sample": (
            unmatched_parcel_land_coordinates[:20]
        ),
    }

    return bridge, summary


def build_property_parcel_bridge_key(
    *,
    source_land_coordinate: str,
    property_parcel_key: str,
) -> str:
    identity = {
        "source_land_coordinate": source_land_coordinate,
        "property_parcel_key": property_parcel_key,
    }

    digest = hashlib.md5(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"


def _normalize_key_series(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip()

    return normalized.mask(
        normalized.isna() | normalized.eq("")
    )


def _require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise VancouverPropertyParcelBridgeError(
            f"{table_name} is missing columns: "
            f"{sorted(missing_columns)}"
        )