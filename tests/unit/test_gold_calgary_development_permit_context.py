import pandas as pd
from shapely.geometry import MultiPoint, Point, Polygon

from src.gold.city.calgary_development_permit_context import (
    build_gold_calgary_development_permit_context,
)


def _polygon(
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
) -> str:
    return Polygon(
        [
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ]
    ).wkt


def _permit(
    key: str,
    *,
    locations: list[Point] | None,
    location_types: str | None = None,
) -> dict:
    if locations is None:
        locations_wkt = None
        location_count = None
    elif len(locations) == 1:
        locations_wkt = locations[0].wkt
        location_count = 1
    else:
        locations_wkt = MultiPoint(
            locations
        ).wkt
        location_count = len(locations)

    return {
        "development_permit_key": key,
        "city": "calgary",
        "province": "AB",
        "source_name": "calgary_development_permits",
        "source_permit_id": key,
        "permit_number": key,
        "address_text": "TEST ADDRESS",
        "applicant_name": None,
        "category": "Residential",
        "description": "TEST DEVELOPMENT",
        "proposed_use_code": "TEST",
        "proposed_use_description": (
            "SINGLE DETACHED DWELLING"
        ),
        "permitted_discretionary": None,
        "land_use_district": "R-CG",
        "land_use_district_description": (
            "Residential"
        ),
        "concurrent_location": None,
        "status_current": "Released",
        "applied_date": pd.Timestamp(
            "2026-01-01"
        ),
        "decision_date": pd.Timestamp(
            "2026-02-01"
        ),
        "release_date": pd.Timestamp(
            "2026-02-02"
        ),
        "must_commence_date": None,
        "canceled_refused_date": None,
        "decision": "Approval",
        "decision_by": None,
        "sdab_number": None,
        "sdab_hearing_date": None,
        "sdab_decision": None,
        "sdab_decision_date": None,
        "community_code": "TEST",
        "community_name": "Test Community",
        "ward": 1,
        "quadrant": "NW",
        "latitude": (
            locations[0].y
            if locations
            else None
        ),
        "longitude": (
            locations[0].x
            if locations
            else None
        ),
        "geometry_wkt": (
            locations[0].wkt
            if locations
            else None
        ),
        "location_count": location_count,
        "location_types": location_types,
        "location_addresses": None,
        "locations_geojson": None,
        "locations_wkt": locations_wkt,
        "source_record_count": 1,
        "applied_year": 2026,
        "decision_year": 2026,
    }


def _location(
    parcel_id: str,
    geometry_wkt: str,
    *,
    assessed_value: float = 500_000.0,
) -> dict:
    return {
        "source_parcel_id": parcel_id,
        "assessment_year": 2026,
        "assessment_record_count": 1,
        "assessed_value_total_sum": assessed_value,
        "assessed_value_residential_sum": (
            assessed_value
        ),
        "assessed_value_non_residential_sum": 0.0,
        "assessed_value_farmland_sum": 0.0,
        "community_code": "TEST",
        "community_name": "Test Community",
        "land_use_designation": "R-CG",
        "property_type": "Residential",
        "geometry_wkt": geometry_wkt,
    }


def _flood(
    parcel_id: str,
    *,
    exposed: bool = False,
    regulatory: bool | None = None,
    river_channel: bool = False,
) -> dict:
    if regulatory is None:
        regulatory = exposed

    return {
        "source_parcel_id": parcel_id,
        "intersects_regulatory_flood_layer": (
            regulatory
        ),
        "is_flood_exposed": exposed,
        "intersects_normal_river_channel": (
            river_channel
        ),
        "flood_zone_membership_count": (
            1 if regulatory else 0
        ),
    }


def _build(
    *,
    permits: list[dict],
    locations: list[dict],
    floods: list[dict],
):
    return (
        build_gold_calgary_development_permit_context(
            permit_dataframe=pd.DataFrame(
                permits
            ),
            location_dataframe=pd.DataFrame(
                locations
            ),
            flood_dataframe=pd.DataFrame(
                floods
            ),
        )
    )


def test_single_point_maps_to_single_property():
    permits = [
        _permit(
            "DP1",
            locations=[
                Point(
                    -114.095,
                    51.005,
                )
            ],
            location_types="Titled Parcel",
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
            exposed=False,
        )
    ]

    bridge, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    assert len(bridge) == 1
    assert len(context) == 1

    bridge_row = bridge.iloc[0]
    context_row = context.iloc[0]

    assert (
        bridge_row["source_parcel_id"]
        == "parcel_1"
    )

    assert (
        bridge_row[
            "matched_unique_point_count"
        ]
        == 1
    )

    assert (
        context_row[
            "location_mapping_status"
        ]
        == "single_property"
    )

    assert (
        context_row[
            "mapped_property_location_count"
        ]
        == 1
    )

    assert (
        summary[
            "single_property_permit_count"
        ]
        == 1
    )


def test_duplicate_source_points_are_deduplicated():
    point = Point(
        -114.095,
        51.005,
    )

    permits = [
        _permit(
            "DP1",
            locations=[
                point,
                point,
                point,
            ],
            location_types=(
                "Titled Parcel;"
                "Building;"
                "Building Suite"
            ),
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
        )
    ]

    bridge, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    row = context.iloc[0]

    assert (
        row["source_location_count"]
        == 3
    )

    assert (
        row["unique_source_point_count"]
        == 1
    )

    assert (
        row["source_titled_parcel_count"]
        == 1
    )

    assert (
        row["exact_point_match_count"]
        == 1
    )

    assert (
        len(bridge)
        == 1
    )

    assert (
        bridge.iloc[0][
            "matched_unique_point_count"
        ]
        == 1
    )

    assert (
        summary[
            "source_location_point_count"
        ]
        == 3
    )

    assert (
        summary[
            "unique_source_point_count"
        ]
        == 1
    )


def test_multi_location_permit_maps_to_multiple_properties():
    permits = [
        _permit(
            "DP1",
            locations=[
                Point(
                    -114.095,
                    51.005,
                ),
                Point(
                    -114.075,
                    51.005,
                ),
            ],
            location_types=(
                "Titled Parcel;"
                "Titled Parcel"
            ),
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
            assessed_value=500_000.0,
        ),
        _location(
            "parcel_2",
            _polygon(
                -114.08,
                51.00,
                -114.07,
                51.01,
            ),
            assessed_value=800_000.0,
        ),
    ]

    floods = [
        _flood(
            "parcel_1",
            exposed=False,
        ),
        _flood(
            "parcel_2",
            exposed=True,
        ),
    ]

    bridge, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    row = context.iloc[0]

    assert len(bridge) == 2

    assert set(
        bridge["source_parcel_id"]
    ) == {
        "parcel_1",
        "parcel_2",
    }

    assert (
        row["location_mapping_status"]
        == "multi_property"
    )

    assert (
        row[
            "mapped_property_location_count"
        ]
        == 2
    )

    assert (
        row[
            "mapped_assessed_value_total_sum"
        ]
        == 1_300_000.0
    )

    assert bool(
        row["is_flood_exposed"]
    )

    assert (
        row[
            "flood_exposed_property_location_count"
        ]
        == 1
    )

    assert (
        summary[
            "multi_property_permit_count"
        ]
        == 1
    )


def test_multiple_points_on_same_property_do_not_duplicate_bridge():
    permits = [
        _permit(
            "DP1",
            locations=[
                Point(
                    -114.098,
                    51.003,
                ),
                Point(
                    -114.092,
                    51.007,
                ),
            ],
            location_types=(
                "Titled Parcel;"
                "Building"
            ),
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
        )
    ]

    bridge, context, _ = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    assert len(bridge) == 1

    assert (
        bridge.iloc[0][
            "matched_unique_point_count"
        ]
        == 2
    )

    row = context.iloc[0]

    assert (
        row[
            "mapped_property_location_count"
        ]
        == 1
    )

    assert (
        row["location_mapping_status"]
        == "single_property"
    )


def test_partial_mapping_is_preserved_and_flagged():
    permits = [
        _permit(
            "DP1",
            locations=[
                Point(
                    -114.095,
                    51.005,
                ),
                Point(
                    -114.30,
                    51.20,
                ),
            ],
            location_types=(
                "Titled Parcel;"
                "Building"
            ),
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
        )
    ]

    bridge, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    assert len(bridge) == 1

    row = context.iloc[0]

    assert (
        row["exact_point_match_count"]
        == 1
    )

    assert (
        row["unmatched_point_count"]
        == 1
    )

    assert (
        row["mapped_property_location_count"]
        == 1
    )

    assert bool(
        row[
            "has_partial_spatial_mapping"
        ]
    )

    assert (
        summary[
            "partial_spatial_mapping_permit_count"
        ]
        == 1
    )


def test_no_geometry_permit_is_retained():
    permits = [
        _permit(
            "DP1",
            locations=None,
            location_types=None,
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
        )
    ]

    bridge, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    assert len(bridge) == 0
    assert len(context) == 1

    row = context.iloc[0]

    assert (
        row["location_mapping_status"]
        == "no_geometry"
    )

    assert (
        row[
            "mapped_property_location_count"
        ]
        == 0
    )

    assert (
        summary[
            "no_geometry_permit_count"
        ]
        == 1
    )


def test_river_channel_only_is_not_flood_exposed():
    permits = [
        _permit(
            "DP1",
            locations=[
                Point(
                    -114.095,
                    51.005,
                )
            ],
            location_types="Titled Parcel",
        )
    ]

    locations = [
        _location(
            "parcel_1",
            _polygon(
                -114.10,
                51.00,
                -114.09,
                51.01,
            ),
        )
    ]

    floods = [
        _flood(
            "parcel_1",
            exposed=False,
            regulatory=True,
            river_channel=True,
        )
    ]

    _, context, summary = _build(
        permits=permits,
        locations=locations,
        floods=floods,
    )

    row = context.iloc[0]

    assert not bool(
        row["is_flood_exposed"]
    )

    assert bool(
        row[
            "intersects_regulatory_flood_layer"
        ]
    )

    assert bool(
        row[
            "intersects_normal_river_channel"
        ]
    )

    assert (
        row[
            "normal_river_channel_property_location_count"
        ]
        == 1
    )

    assert (
        summary[
            "flood_exposed_permit_count"
        ]
        == 0
    )