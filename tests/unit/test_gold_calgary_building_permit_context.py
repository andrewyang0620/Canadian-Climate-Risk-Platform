import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from src.gold.city.calgary_building_permit_context import (
    CalgaryBuildingPermitContextError,
    build_gold_calgary_building_permit_context,
)


def _permit(
    key: str,
    *,
    permit_class_mapped: str = "Residential",
    permit_class_group: str = "Single Family",
    permit_type_mapped: str = "Building",
    work_class_group: str = "New",
    housing_units: float = 1.0,
    point: Point | None = None,
) -> dict:
    return {
        "building_permit_key": key,
        "city": "calgary",
        "permit_type_mapped": permit_type_mapped,
        "permit_class_group": permit_class_group,
        "permit_class_mapped": permit_class_mapped,
        "work_class_group": work_class_group,
        "work_class_mapped": (
            "New"
            if work_class_group == "New"
            else "Existing"
        ),
        "housing_units": housing_units,
        "estimated_project_cost": 500_000.0,
        "issue_year": 2026,
        "geometry_wkt": (
            point.wkt
            if point is not None
            else None
        ),
    }


def _polygon(
    min_x=-114.10,
    min_y=51.00,
    max_x=-114.09,
    max_y=51.01,
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
    *,
    geometry_wkt: str | None = None,
) -> dict:
    return {
        "source_parcel_id": parcel_id,
        "assessment_year": 2026,
        "assessment_record_count": 1,
        "assessed_value_total_sum": 700_000.0,
        "assessed_value_residential_sum": 700_000.0,
        "assessed_value_non_residential_sum": 0.0,
        "assessed_value_farmland_sum": 0.0,
        "community_code": "COMM",
        "community_name": "Test Community",
        "land_use_designation": "R-CG",
        "property_type": "RESIDENTIAL",
        "year_of_construction_min": 2000,
        "year_of_construction_max": 2000,
        "geometry_wkt": (
            geometry_wkt
            if geometry_wkt is not None
            else _polygon()
        ),
    }


def _flood(
    parcel_id: str,
    *,
    exposed: bool = False,
) -> dict:
    return {
        "source_parcel_id": parcel_id,
        "intersects_regulatory_flood_layer": exposed,
        "is_flood_exposed": exposed,
        "intersects_normal_river_channel": False,
        "flood_zone_membership_count": (
            1 if exposed else 0
        ),
        "flood_fringe_flag": exposed,
        "flood_fringe_overlap_area_sq_m": (
            100.0 if exposed else 0.0
        ),
        "flood_fringe_overlap_ratio": (
            0.2 if exposed else 0.0
        ),
        "floodplain_flag": False,
        "floodplain_overlap_area_sq_m": 0.0,
        "floodplain_overlap_ratio": 0.0,
        "floodway_flag": False,
        "floodway_overlap_area_sq_m": 0.0,
        "floodway_overlap_ratio": 0.0,
        "normal_river_channel_flag": False,
        "normal_river_channel_overlap_area_sq_m": 0.0,
        "normal_river_channel_overlap_ratio": 0.0,
        "overland_flow_flag": False,
        "overland_flow_overlap_area_sq_m": 0.0,
        "overland_flow_overlap_ratio": 0.0,
    }


def _build(
    *,
    permits: list[dict],
    locations: list[dict],
    floods: list[dict],
):
    return build_gold_calgary_building_permit_context(
        permit_dataframe=pd.DataFrame(permits),
        location_dataframe=pd.DataFrame(locations),
        flood_dataframe=pd.DataFrame(floods),
    )


def test_residential_permit_with_units_is_housing_supply():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                housing_units=2,
                point=Point(-114.095, 51.005),
            )
        ],
        locations=[
            _location("parcel_1")
        ],
        floods=[
            _flood("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert bool(row["is_residential_permit"])
    assert bool(row["is_housing_related"])
    assert bool(row["creates_new_housing_units"])

    assert (
        row["new_housing_units_created"]
        == pytest.approx(2.0)
    )

    assert (
        row["housing_activity_type"]
        == "new"
    )

    assert (
        summary[
            "new_housing_supply_permit_count"
        ]
        == 1
    )

    assert (
        summary[
            "new_housing_units_created_sum"
        ]
        == pytest.approx(2.0)
    )


def test_non_residential_permit_with_units_is_still_housing_related():
    result, _ = _build(
        permits=[
            _permit(
                "permit_1",
                permit_class_mapped="Non-Residential",
                permit_class_group="Commercial",
                housing_units=5,
                point=Point(-114.095, 51.005),
            )
        ],
        locations=[
            _location("parcel_1")
        ],
        floods=[
            _flood("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert not bool(
        row["is_residential_permit"]
    )

    assert bool(
        row["creates_new_housing_units"]
    )

    assert bool(
        row["is_housing_related"]
    )

    assert (
        row["new_housing_units_created"]
        == pytest.approx(5.0)
    )


def test_negative_housing_units_are_flagged_not_counted():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                housing_units=-1,
                point=Point(-114.095, 51.005),
            )
        ],
        locations=[
            _location("parcel_1")
        ],
        floods=[
            _flood("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert bool(
        row["housing_units_anomaly_flag"]
    )

    assert pd.isna(
        row["new_housing_units_created"]
    )

    assert not bool(
        row["creates_new_housing_units"]
    )

    # It remains housing-related because it is
    # explicitly classified Residential.
    assert bool(
        row["is_housing_related"]
    )

    assert (
        summary["housing_units_anomaly_count"]
        == 1
    )


def test_exact_location_match_attaches_assessment_and_flood():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                housing_units=10,
                point=Point(-114.095, 51.005),
            )
        ],
        locations=[
            _location("parcel_1")
        ],
        floods=[
            _flood(
                "parcel_1",
                exposed=True,
            )
        ],
    )

    row = result.iloc[0]

    assert (
        row["location_mapping_status"]
        == "exact_1_to_1"
    )

    assert (
        row["source_parcel_id"]
        == "parcel_1"
    )

    assert (
        row["assessed_value_total_sum"]
        == pytest.approx(700_000.0)
    )

    assert bool(
        row["is_flood_exposed"]
    )

    assert (
        summary["exact_location_match_count"]
        == 1
    )

    assert (
        summary[
            "flood_exposed_housing_permit_count"
        ]
        == 1
    )

    assert (
        summary[
            "flood_exposed_new_housing_units"
        ]
        == pytest.approx(10.0)
    )


def test_ambiguous_location_match_does_not_assign_location():
    geometry = _polygon()

    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                point=Point(-114.095, 51.005),
            )
        ],
        locations=[
            _location(
                "parcel_1",
                geometry_wkt=geometry,
            ),
            _location(
                "parcel_2",
                geometry_wkt=geometry,
            ),
        ],
        floods=[
            _flood("parcel_1"),
            _flood("parcel_2"),
        ],
    )

    row = result.iloc[0]

    assert (
        row["location_mapping_status"]
        == "ambiguous_1_to_many"
    )

    assert (
        row["location_match_count"]
        == 2
    )

    assert pd.isna(
        row["source_parcel_id"]
    )

    assert pd.isna(
        row["assessed_value_total_sum"]
    )

    assert pd.isna(
        row["is_flood_exposed"]
    )

    assert (
        summary[
            "ambiguous_location_match_count"
        ]
        == 1
    )


def test_complete_permit_universe_is_preserved():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                point=Point(-114.095, 51.005),
            ),
            _permit(
                "permit_2",
                permit_class_mapped="Non-Residential",
                permit_class_group="Commercial",
                housing_units=0,
                point=Point(-114.30, 51.20),
            ),
            _permit(
                "permit_3",
                housing_units=0,
                point=None,
            ),
        ],
        locations=[
            _location("parcel_1")
        ],
        floods=[
            _flood("parcel_1")
        ],
    )

    assert len(result) == 3

    assert (
        result["building_permit_key"].is_unique
    )

    assert set(
        result["building_permit_key"]
    ) == {
        "permit_1",
        "permit_2",
        "permit_3",
    }

    assert summary["permit_input_count"] == 3
    assert summary["output_row_count"] == 3

    assert (
        summary["exact_location_match_count"]
        == 1
    )

    assert (
        summary["no_location_match_count"]
        == 1
    )

    assert (
        summary["no_geometry_count"]
        == 1
    )