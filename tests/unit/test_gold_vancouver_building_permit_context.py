import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from src.gold.city.vancouver_building_permit_context import (
    VancouverBuildingPermitContextError,
    build_gold_vancouver_building_permit_context,
)


def _permit(
    key: str,
    *,
    permit_class_group: str = "Dwelling Uses",
    permit_type: str = "New Building",
    point: Point | None = None,
) -> dict:
    return {
        "building_permit_key": key,
        "city": "vancouver",
        "permit_number": key,
        "permit_type_mapped": permit_type,
        "permit_class_group": permit_class_group,
        "permit_class_mapped": "Single Detached House",
        "work_class_mapped": "New Build - Low Density Housing",
        "issue_date": pd.Timestamp("2026-01-15"),
        "issue_year": 2026,
        "year_month": "2026-01",
        "address_text": "100 Test Street",
        "project_description": "Test permit",
        "estimated_project_cost": 500_000.0,
        "neighbourhood_name": "Test",
        "latitude": point.y if point else None,
        "longitude": point.x if point else None,
        "geometry_wkt": point.wkt if point else None,
    }


def _parcel(
    key: str,
    polygon: Polygon,
) -> dict:
    return {
        "property_parcel_key": key,
        "city": "vancouver",
        "geometry_wkt": polygon.wkt,
    }


def _flood(
    parcel_key: str,
    *,
    exposed: bool = False,
) -> dict:
    return {
        "property_parcel_key": parcel_key,
        "is_flood_exposed": exposed,
        "scenario_count": 1 if exposed else 0,
        "designated_floodplain_flag": exposed,
        "designated_floodplain_overlap_ratio": (
            0.5 if exposed else 0.0
        ),
        "fraser_risk_today_flag": False,
        "fraser_risk_today_overlap_ratio": 0.0,
        "still_creek_floodplain_flag": False,
        "still_creek_floodplain_overlap_ratio": 0.0,
        "wave_effect_zone_flag": False,
        "wave_effect_zone_overlap_ratio": 0.0,
    }


def _assessment(
    parcel_key: str,
) -> dict:
    return {
        "property_parcel_key": parcel_key,
        "has_latest_assessment": True,
        "assessment_mapping_ambiguous": False,
        "assessment_mapping_exact_1_to_1": True,
        "report_year": 2026,
        "land_coordinate_current_land_value": 300_000.0,
        "land_coordinate_current_improvement_value": 400_000.0,
        "land_coordinate_current_total_assessed_value": 700_000.0,
        "exact_mapped_current_land_value": 300_000.0,
        "exact_mapped_current_improvement_value": 400_000.0,
        "exact_mapped_current_total_assessed_value": 700_000.0,
    }


def _polygon(
    min_x=-123.2,
    min_y=49.2,
    max_x=-123.1,
    max_y=49.3,
) -> Polygon:
    return Polygon(
        [
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        ]
    )


def _build(
    permits: list[dict],
    parcels: list[dict],
    flood: list[dict],
    assessments: list[dict],
):
    return build_gold_vancouver_building_permit_context(
        permit_dataframe=pd.DataFrame(permits),
        parcel_dataframe=pd.DataFrame(parcels),
        flood_dataframe=pd.DataFrame(flood),
        assessment_dataframe=pd.DataFrame(assessments),
    )


def test_dwelling_use_is_classified_as_housing():
    result, _ = _build(
        permits=[
            _permit(
                "permit_1",
                permit_class_group=(
                    "Dwelling Uses,Retail Uses"
                ),
                point=Point(-123.15, 49.25),
            )
        ],
        parcels=[
            _parcel(
                "parcel_1",
                _polygon(),
            )
        ],
        flood=[
            _flood("parcel_1")
        ],
        assessments=[
            _assessment("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert bool(row["is_housing_related"])
    assert (
        row["housing_activity_type"]
        == "new_building"
    )
    assert bool(
        row["is_new_housing_building_permit"]
    )


def test_non_dwelling_use_is_not_housing():
    result, _ = _build(
        permits=[
            _permit(
                "permit_1",
                permit_class_group="Office Uses",
                permit_type="Addition / Alteration",
                point=Point(-123.15, 49.25),
            )
        ],
        parcels=[
            _parcel("parcel_1", _polygon())
        ],
        flood=[
            _flood("parcel_1")
        ],
        assessments=[
            _assessment("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert not bool(row["is_housing_related"])
    assert (
        row["housing_activity_type"]
        == "non_housing"
    )
    assert not bool(
        row["is_housing_renovation_permit"]
    )


def test_exact_spatial_match_attaches_parcel_context():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                point=Point(-123.15, 49.25),
            )
        ],
        parcels=[
            _parcel("parcel_1", _polygon())
        ],
        flood=[
            _flood(
                "parcel_1",
                exposed=True,
            )
        ],
        assessments=[
            _assessment("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert (
        row["parcel_mapping_status"]
        == "exact_1_to_1"
    )
    assert row["parcel_match_count"] == 1
    assert (
        row["property_parcel_key"]
        == "parcel_1"
    )

    assert bool(row["is_flood_exposed"])
    assert bool(
        row["assessment_mapping_exact_1_to_1"]
    )

    assert (
        row[
            "exact_mapped_current_total_assessed_value"
        ]
        == pytest.approx(700_000.0)
    )

    assert summary["exact_parcel_match_count"] == 1
    assert (
        summary[
            "flood_exposed_housing_permit_count"
        ]
        == 1
    )


def test_ambiguous_spatial_match_does_not_assign_parcel():
    polygon = _polygon()

    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                point=Point(-123.15, 49.25),
            )
        ],
        parcels=[
            _parcel("parcel_1", polygon),
            _parcel("parcel_2", polygon),
        ],
        flood=[
            _flood("parcel_1"),
            _flood("parcel_2"),
        ],
        assessments=[
            _assessment("parcel_1"),
            _assessment("parcel_2"),
        ],
    )

    row = result.iloc[0]

    assert (
        row["parcel_mapping_status"]
        == "ambiguous_1_to_many"
    )
    assert row["parcel_match_count"] == 2
    assert pd.isna(
        row["property_parcel_key"]
    )

    assert pd.isna(
        row["is_flood_exposed"]
    )
    assert pd.isna(
        row[
            "exact_mapped_current_total_assessed_value"
        ]
    )

    assert (
        summary["ambiguous_parcel_match_count"]
        == 1
    )


def test_missing_geometry_keeps_permit_without_parcel():
    result, summary = _build(
        permits=[
            _permit(
                "permit_1",
                point=None,
            )
        ],
        parcels=[
            _parcel("parcel_1", _polygon())
        ],
        flood=[
            _flood("parcel_1")
        ],
        assessments=[
            _assessment("parcel_1")
        ],
    )

    row = result.iloc[0]

    assert not bool(
        row["has_spatial_geometry"]
    )
    assert (
        row["parcel_mapping_status"]
        == "no_geometry"
    )
    assert row["parcel_match_count"] == 0
    assert pd.isna(
        row["property_parcel_key"]
    )

    assert summary["no_geometry_count"] == 1


def test_complete_permit_universe_is_preserved():
    permits = [
        _permit(
            "permit_1",
            point=Point(-123.15, 49.25),
        ),
        _permit(
            "permit_2",
            permit_class_group="Office Uses",
            point=Point(-124.0, 49.0),
        ),
        _permit(
            "permit_3",
            point=None,
        ),
    ]

    result, summary = _build(
        permits=permits,
        parcels=[
            _parcel("parcel_1", _polygon())
        ],
        flood=[
            _flood("parcel_1")
        ],
        assessments=[
            _assessment("parcel_1")
        ],
    )

    assert len(result) == 3
    assert result["building_permit_key"].is_unique

    assert set(
        result["building_permit_key"]
    ) == {
        "permit_1",
        "permit_2",
        "permit_3",
    }

    assert summary["permit_input_count"] == 3
    assert summary["output_row_count"] == 3
    assert summary["exact_parcel_match_count"] == 1
    assert summary["no_parcel_match_count"] == 1
    assert summary["no_geometry_count"] == 1


def test_duplicate_permit_key_is_rejected():
    permits = [
        _permit(
            "permit_1",
            point=Point(-123.15, 49.25),
        ),
        _permit(
            "permit_1",
            point=Point(-123.16, 49.26),
        ),
    ]

    with pytest.raises(
        VancouverBuildingPermitContextError,
        match="Duplicate building_permit_key",
    ):
        _build(
            permits=permits,
            parcels=[
                _parcel(
                    "parcel_1",
                    _polygon(),
                )
            ],
            flood=[
                _flood("parcel_1")
            ],
            assessments=[
                _assessment("parcel_1")
            ],
        )