import pandas as pd
import pytest

from src.gold.city.vancouver_property_parcel_bridge import (
    VancouverPropertyParcelBridgeError,
    build_gold_vancouver_property_parcel_bridge,
)


def test_build_vancouver_property_parcel_bridge_tracks_ambiguity():
    parcels = pd.DataFrame(
        [
            {
                "property_parcel_key": "parcel_1",
                "source_parcel_id": "site_1",
                "source_tax_coord": "A",
            },
            {
                "property_parcel_key": "parcel_2",
                "source_parcel_id": "site_2",
                "source_tax_coord": "B",
            },
            {
                "property_parcel_key": "parcel_3",
                "source_parcel_id": "site_3",
                "source_tax_coord": "B",
            },
            {
                "property_parcel_key": "parcel_4",
                "source_parcel_id": "site_4",
                "source_tax_coord": None,
            },
        ]
    )

    property_tax = pd.DataFrame(
        [
            {"source_land_coordinate": "A"},
            {"source_land_coordinate": "A"},
            {"source_land_coordinate": "B"},
            {"source_land_coordinate": "C"},
            {"source_land_coordinate": None},
        ]
    )

    bridge, summary = (
        build_gold_vancouver_property_parcel_bridge(
            parcel_dataframe=parcels,
            property_tax_dataframe=property_tax,
        )
    )

    assert len(bridge) == 3

    assert set(
        bridge["source_land_coordinate"]
    ) == {"A", "B"}

    coordinate_a = bridge[
        bridge["source_land_coordinate"] == "A"
    ].iloc[0]

    assert coordinate_a["parcel_count_for_land_coordinate"] == 1
    assert not coordinate_a["is_ambiguous_land_coordinate"]
    assert (
        coordinate_a["mapping_method"]
        == "exact_land_coordinate_1_to_1"
    )

    coordinate_b = bridge[
        bridge["source_land_coordinate"] == "B"
    ]

    assert len(coordinate_b) == 2
    assert (
        coordinate_b["parcel_count_for_land_coordinate"]
        == 2
    ).all()
    assert coordinate_b[
        "is_ambiguous_land_coordinate"
    ].all()
    assert (
        coordinate_b["mapping_method"]
        == "exact_land_coordinate_1_to_many"
    ).all()

    assert summary["parcel_row_count"] == 4
    assert summary["parcel_with_tax_coord_count"] == 3

    assert summary["tax_row_count"] == 5
    assert summary["tax_rows_with_land_coordinate_count"] == 4

    assert summary["distinct_tax_land_coordinate_count"] == 3
    assert summary["matched_land_coordinate_count"] == 2

    assert summary["matched_tax_row_count"] == 3
    assert summary["tax_row_match_rate"] == pytest.approx(0.75)

    assert summary["ambiguous_land_coordinate_count"] == 1
    assert summary["ambiguous_bridge_row_count"] == 2
    assert (
        summary["maximum_parcel_count_for_land_coordinate"]
        == 2
    )


def test_bridge_excludes_unmatched_land_coordinates():
    parcels = pd.DataFrame(
        [
            {
                "property_parcel_key": "parcel_1",
                "source_parcel_id": "site_1",
                "source_tax_coord": "A",
            },
            {
                "property_parcel_key": "parcel_2",
                "source_parcel_id": "site_2",
                "source_tax_coord": "B",
            },
        ]
    )

    property_tax = pd.DataFrame(
        [
            {"source_land_coordinate": "A"},
            {"source_land_coordinate": "C"},
        ]
    )

    bridge, summary = (
        build_gold_vancouver_property_parcel_bridge(
            parcel_dataframe=parcels,
            property_tax_dataframe=property_tax,
        )
    )

    assert len(bridge) == 1
    assert bridge.iloc[0]["property_parcel_key"] == "parcel_1"

    assert summary["unmatched_tax_land_coordinate_count"] == 1
    assert summary["unmatched_parcel_land_coordinate_count"] == 1

    assert summary["unmatched_tax_land_coordinate_sample"] == [
        "C"
    ]
    assert (
        summary["unmatched_parcel_land_coordinate_sample"]
        == ["B"]
    )


def test_bridge_rejects_duplicate_parcel_keys():
    parcels = pd.DataFrame(
        [
            {
                "property_parcel_key": "parcel_1",
                "source_parcel_id": "site_1",
                "source_tax_coord": "A",
            },
            {
                "property_parcel_key": "parcel_1",
                "source_parcel_id": "site_2",
                "source_tax_coord": "B",
            },
        ]
    )

    property_tax = pd.DataFrame(
        [
            {"source_land_coordinate": "A"},
        ]
    )

    with pytest.raises(
        VancouverPropertyParcelBridgeError,
        match="duplicate property_parcel_key",
    ):
        build_gold_vancouver_property_parcel_bridge(
            parcel_dataframe=parcels,
            property_tax_dataframe=property_tax,
        )


def test_bridge_rejects_zero_matching_land_coordinates():
    parcels = pd.DataFrame(
        [
            {
                "property_parcel_key": "parcel_1",
                "source_parcel_id": "site_1",
                "source_tax_coord": "A",
            },
        ]
    )

    property_tax = pd.DataFrame(
        [
            {"source_land_coordinate": "B"},
        ]
    )

    with pytest.raises(
        VancouverPropertyParcelBridgeError,
        match="No matching land coordinates",
    ):
        build_gold_vancouver_property_parcel_bridge(
            parcel_dataframe=parcels,
            property_tax_dataframe=property_tax,
        )