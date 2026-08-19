import json

import pandas as pd
import pytest

from src.gold.city.vancouver_parcel_flood_zone_overlay import (
    VancouverParcelFloodOverlayError,
    build_gold_vancouver_parcel_flood_zone_overlay,
)


def _parcel(
    *,
    key: str,
    geometry_wkt: str,
) -> dict:
    return {
        "property_parcel_key": key,
        "city": "vancouver",
        "source_name": "vancouver_property_parcels",
        "source_parcel_id": key,
        "geometry_wkt": geometry_wkt,
    }


def _flood(
    *,
    key: str,
    source_zone_id: str,
    name: str,
    geometry_wkt: str,
) -> dict:
    description = f"{name} description"

    return {
        "flood_hazard_zone_key": key,
        "city": "vancouver",
        "source_zone_id": source_zone_id,
        "hazard_class": description,
        "geometry_wkt": geometry_wkt,
        "source_name": "vancouver_floodplain",
        "source_properties_json": json.dumps(
            {
                "name": name,
                "description": description,
            }
        ),
    }


def test_overlay_preserves_source_zone_and_scenario():
    parcels = pd.DataFrame(
        [
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1200 49.2500, "
                    "-123.1190 49.2500, "
                    "-123.1190 49.2510, "
                    "-123.1200 49.2510, "
                    "-123.1200 49.2500"
                    "))"
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                source_zone_id="20",
                name="Fraser Risk Today 1/500 Storm",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1210 49.2490, "
                    "-123.1180 49.2490, "
                    "-123.1180 49.2520, "
                    "-123.1210 49.2520, "
                    "-123.1210 49.2490"
                    "))"
                ),
            ),
        ]
    )

    overlay, summary = (
        build_gold_vancouver_parcel_flood_zone_overlay(
            parcel_dataframe=parcels,
            flood_dataframe=floods,
            progress_interval=0,
        )
    )

    assert len(overlay) == 1

    row = overlay.iloc[0]

    assert row["property_parcel_key"] == "parcel_1"
    assert row["flood_hazard_zone_key"] == "flood_1"
    assert row["source_zone_id"] == "20"
    assert (
        row["scenario_name"]
        == "Fraser Risk Today 1/500 Storm"
    )

    assert row["parcel_area_sq_m"] > 0
    assert row["intersection_area_sq_m"] > 0
    assert row["parcel_overlap_ratio"] == pytest.approx(
        1.0,
        abs=1e-8,
    )

    assert row["crs_epsg"] == 3347
    assert (
        row["spatial_join_method"]
        == "polygon_intersection_epsg3347"
    )

    assert summary["overlay_row_count"] == 1
    assert summary["matched_parcel_count"] == 1


def test_boundary_only_touch_is_not_exposure():
    parcels = pd.DataFrame(
        [
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1200 49.2500, "
                    "-123.1190 49.2500, "
                    "-123.1190 49.2510, "
                    "-123.1200 49.2510, "
                    "-123.1200 49.2500"
                    "))"
                ),
            ),
            _parcel(
                key="parcel_2",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1180 49.2500, "
                    "-123.1170 49.2500, "
                    "-123.1170 49.2510, "
                    "-123.1180 49.2510, "
                    "-123.1180 49.2500"
                    "))"
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                source_zone_id="19",
                name="Designated Floodplain (FCL 4.6m)",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1210 49.2490, "
                    "-123.1180 49.2490, "
                    "-123.1180 49.2520, "
                    "-123.1210 49.2520, "
                    "-123.1210 49.2490"
                    "))"
                ),
            ),
        ]
    )

    overlay, summary = (
        build_gold_vancouver_parcel_flood_zone_overlay(
            parcel_dataframe=parcels,
            flood_dataframe=floods,
            progress_interval=0,
        )
    )

    assert set(
        overlay["property_parcel_key"]
    ) == {"parcel_1"}

    assert summary["candidate_pair_count"] == 2
    assert summary["boundary_touch_only_pair_count"] == 1


def test_multiple_source_zones_are_not_dissolved():
    parcels = pd.DataFrame(
        [
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1200 49.2500, "
                    "-123.1180 49.2500, "
                    "-123.1180 49.2520, "
                    "-123.1200 49.2520, "
                    "-123.1200 49.2500"
                    "))"
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                source_zone_id="20",
                name="Fraser Risk Today 1/500 Storm",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1210 49.2490, "
                    "-123.1190 49.2490, "
                    "-123.1190 49.2530, "
                    "-123.1210 49.2530, "
                    "-123.1210 49.2490"
                    "))"
                ),
            ),
            _flood(
                key="flood_2",
                source_zone_id="22",
                name="Fraser Risk Today 1/500 Storm",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1195 49.2490, "
                    "-123.1170 49.2490, "
                    "-123.1170 49.2530, "
                    "-123.1195 49.2530, "
                    "-123.1195 49.2490"
                    "))"
                ),
            ),
        ]
    )

    overlay, _ = (
        build_gold_vancouver_parcel_flood_zone_overlay(
            parcel_dataframe=parcels,
            flood_dataframe=floods,
            progress_interval=0,
        )
    )

    assert len(overlay) == 2

    assert set(
        overlay["flood_hazard_zone_key"]
    ) == {
        "flood_1",
        "flood_2",
    }

    assert overlay["scenario_name"].nunique() == 1


def test_invalid_parcel_geometry_is_repaired():
    parcels = pd.DataFrame(
        [
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1200 49.2500, "
                    "-123.1180 49.2520, "
                    "-123.1200 49.2520, "
                    "-123.1180 49.2500, "
                    "-123.1200 49.2500"
                    "))"
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                source_zone_id="25",
                name="Still Creek Floodplain",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1210 49.2490, "
                    "-123.1170 49.2490, "
                    "-123.1170 49.2530, "
                    "-123.1210 49.2530, "
                    "-123.1210 49.2490"
                    "))"
                ),
            ),
        ]
    )

    overlay, summary = (
        build_gold_vancouver_parcel_flood_zone_overlay(
            parcel_dataframe=parcels,
            flood_dataframe=floods,
            progress_interval=0,
        )
    )

    assert len(overlay) == 1

    assert summary["parcel_geometry_invalid_count"] == 1
    assert summary["parcel_geometry_repaired_count"] == 1

    assert bool(
        overlay.iloc[0]["parcel_geometry_repaired"]
    )


def test_duplicate_parcel_key_is_rejected():
    parcels = pd.DataFrame(
        [
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1200 49.2500, "
                    "-123.1190 49.2500, "
                    "-123.1190 49.2510, "
                    "-123.1200 49.2510, "
                    "-123.1200 49.2500"
                    "))"
                ),
            ),
            _parcel(
                key="parcel_1",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1180 49.2500, "
                    "-123.1170 49.2500, "
                    "-123.1170 49.2510, "
                    "-123.1180 49.2510, "
                    "-123.1180 49.2500"
                    "))"
                ),
            ),
        ]
    )

    floods = pd.DataFrame(
        [
            _flood(
                key="flood_1",
                source_zone_id="25",
                name="Still Creek Floodplain",
                geometry_wkt=(
                    "POLYGON (("
                    "-123.1210 49.2490, "
                    "-123.1160 49.2490, "
                    "-123.1160 49.2530, "
                    "-123.1210 49.2530, "
                    "-123.1210 49.2490"
                    "))"
                ),
            ),
        ]
    )

    with pytest.raises(
        VancouverParcelFloodOverlayError,
        match="duplicate property_parcel_key",
    ):
        build_gold_vancouver_parcel_flood_zone_overlay(
            parcel_dataframe=parcels,
            flood_dataframe=floods,
            progress_interval=0,
        )