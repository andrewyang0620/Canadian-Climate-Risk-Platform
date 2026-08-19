import pandas as pd
import pytest
from shapely.geometry import Polygon

from src.gold.city.calgary_property_location_flood import (
    CalgaryPropertyLocationFloodError,
    build_gold_calgary_property_location_flood,
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


def _location(
    parcel_id: str,
    geometry_wkt: str,
) -> dict:
    return {
        "source_parcel_id": parcel_id,
        "geometry_wkt": geometry_wkt,
    }


def _flood(
    *,
    key: str,
    zone_id: str,
    hazard_class: str,
    geometry_wkt: str,
) -> dict:
    return {
        "flood_hazard_zone_key": key,
        "city": "calgary",
        "source_zone_id": zone_id,
        "hazard_class": hazard_class,
        "geometry_wkt": geometry_wkt,
    }


def test_floodway_intersection_is_flood_exposed():
    locations = pd.DataFrame(
        [
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
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                zone_id="200",
                hazard_class="Floodway",
                geometry_wkt=_polygon(
                    -114.10,
                    51.00,
                    -114.095,
                    51.01,
                ),
            )
        ]
    )

    overlay, exposure, summary = (
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    assert len(overlay) == 1
    assert len(exposure) == 1

    row = exposure.iloc[0]

    assert bool(
        row["intersects_regulatory_flood_layer"]
    )
    assert bool(row["is_flood_exposed"])
    assert not bool(
        row["intersects_normal_river_channel"]
    )

    assert bool(row["floodway_flag"])

    assert (
        row["floodway_overlap_ratio"]
        == pytest.approx(0.5, abs=0.02)
    )

    assert (
        summary["flood_exposed_location_count"]
        == 1
    )


def test_normal_river_channel_is_not_flood_exposure():
    locations = pd.DataFrame(
        [
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
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="river_1",
                zone_id="500",
                hazard_class="Normal River Channel",
                geometry_wkt=_polygon(
                    -114.10,
                    51.00,
                    -114.095,
                    51.01,
                ),
            )
        ]
    )

    overlay, exposure, summary = (
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    assert len(overlay) == 1

    row = exposure.iloc[0]

    assert bool(
        row["intersects_regulatory_flood_layer"]
    )

    assert not bool(
        row["is_flood_exposed"]
    )

    assert bool(
        row["intersects_normal_river_channel"]
    )

    assert bool(
        row["normal_river_channel_flag"]
    )

    assert (
        summary[
            "normal_river_channel_location_count"
        ]
        == 1
    )

    assert (
        summary[
            "normal_river_channel_only_location_count"
        ]
        == 1
    )


def test_unexposed_location_is_retained():
    locations = pd.DataFrame(
        [
            _location(
                "parcel_1",
                _polygon(
                    -114.10,
                    51.00,
                    -114.09,
                    51.01,
                ),
            ),
            _location(
                "parcel_2",
                _polygon(
                    -114.30,
                    51.20,
                    -114.29,
                    51.21,
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                zone_id="100",
                hazard_class="Flood Fringe",
                geometry_wkt=_polygon(
                    -114.10,
                    51.00,
                    -114.095,
                    51.01,
                ),
            )
        ]
    )

    _, exposure, summary = (
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    assert len(exposure) == 2

    unexposed = exposure[
        exposure["source_parcel_id"]
        == "parcel_2"
    ].iloc[0]

    assert not bool(
        unexposed[
            "intersects_regulatory_flood_layer"
        ]
    )

    assert not bool(
        unexposed["is_flood_exposed"]
    )

    assert (
        unexposed[
            "flood_zone_membership_count"
        ]
        == 0
    )

    assert (
        unexposed[
            "flood_fringe_overlap_ratio"
        ]
        == 0.0
    )

    assert (
        summary["location_input_count"]
        == 2
    )

    assert (
        summary["location_output_count"]
        == 2
    )


def test_same_hazard_class_uses_union_not_raw_sum():
    location_geometry = _polygon(
        -114.10,
        51.00,
        -114.09,
        51.01,
    )

    locations = pd.DataFrame(
        [
            _location(
                "parcel_1",
                location_geometry,
            )
        ]
    )

    # Both source polygons cover the same location.
    # Overlay should retain both source memberships,
    # but final class overlap ratio must remain 1.0,
    # not 2.0.
    floods = pd.DataFrame(
        [
            _flood(
                key="fringe_1",
                zone_id="100",
                hazard_class="Flood Fringe",
                geometry_wkt=location_geometry,
            ),
            _flood(
                key="fringe_2",
                zone_id="100",
                hazard_class="Flood Fringe",
                geometry_wkt=location_geometry,
            ),
        ]
    )

    overlay, exposure, summary = (
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    assert len(overlay) == 2

    row = exposure.iloc[0]

    assert (
        row["flood_zone_membership_count"]
        == 2
    )

    assert bool(row["flood_fringe_flag"])

    assert (
        row["flood_fringe_overlap_ratio"]
        == pytest.approx(1.0)
    )

    assert (
        summary[
            "hazard_class_location_counts"
        ]["Flood Fringe"]
        == 1
    )


def test_duplicate_location_grain_is_rejected():
    geometry = _polygon(
        -114.10,
        51.00,
        -114.09,
        51.01,
    )

    locations = pd.DataFrame(
        [
            _location(
                "parcel_1",
                geometry,
            ),
            _location(
                "parcel_1",
                geometry,
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                zone_id="200",
                hazard_class="Floodway",
                geometry_wkt=geometry,
            )
        ]
    )

    with pytest.raises(
        CalgaryPropertyLocationFloodError,
        match="one row per source_parcel_id",
    ):
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )