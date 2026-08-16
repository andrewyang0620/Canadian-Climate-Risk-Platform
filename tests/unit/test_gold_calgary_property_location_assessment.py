import pandas as pd
import pytest

from src.gold.city.calgary_property_location_assessment import (
    CalgaryPropertyLocationAssessmentError,
    build_gold_calgary_property_location_assessment,
)


GEOMETRY = (
    "MULTIPOLYGON (((-114.1 51.0, "
    "-114.09 51.0, -114.09 51.01, "
    "-114.1 51.01, -114.1 51.0)))"
)


def _row(
    *,
    key: str,
    parcel_id: str,
    property_id: str,
    total: float,
    residential: float,
    geometry: str = GEOMETRY,
    community: str = "COMM",
) -> dict:
    return {
        "property_assessment_key": key,
        "city": "calgary",
        "source_property_id": property_id,
        "source_parcel_id": parcel_id,
        "source_unique_key": key,
        "assessment_year": 2026,
        "assessed_value_total": total,
        "assessed_value_residential": residential,
        "assessed_value_non_residential": (
            total - residential
        ),
        "assessed_value_farmland": 0.0,
        "assessment_class": "Residential",
        "assessment_class_description": "Residential",
        "community_code": community,
        "community_name": community,
        "year_of_construction": 2000,
        "land_use_designation": "R-CG",
        "property_type": "RESIDENTIAL",
        "sub_property_use": "Dwelling",
        "geometry_wkt": geometry,
        "source_name": "calgary_property_assessment",
    }


def test_multiple_assessments_are_aggregated_to_location():
    dataframe = pd.DataFrame(
        [
            _row(
                key="a",
                parcel_id="p1",
                property_id="unit1",
                total=300_000,
                residential=300_000,
            ),
            _row(
                key="b",
                parcel_id="p1",
                property_id="unit2",
                total=400_000,
                residential=400_000,
            ),
        ]
    )

    result, summary = (
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert row["assessment_record_count"] == 2
    assert row["distinct_property_count"] == 2
    assert (
        row["assessed_value_total_sum"]
        == pytest.approx(700_000)
    )
    assert bool(
        row["has_multiple_assessment_records"]
    )

    assert summary["multi_record_location_count"] == 1


def test_different_parcels_remain_separate():
    dataframe = pd.DataFrame(
        [
            _row(
                key="a",
                parcel_id="p1",
                property_id="unit1",
                total=300_000,
                residential=300_000,
            ),
            _row(
                key="b",
                parcel_id="p2",
                property_id="unit2",
                total=400_000,
                residential=400_000,
            ),
        ]
    )

    result, _ = (
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe
        )
    )

    assert len(result) == 2
    assert result["source_parcel_id"].is_unique


def test_multiple_communities_are_flagged():
    dataframe = pd.DataFrame(
        [
            _row(
                key="a",
                parcel_id="p1",
                property_id="unit1",
                total=300_000,
                residential=300_000,
                community="A",
            ),
            _row(
                key="b",
                parcel_id="p1",
                property_id="unit2",
                total=400_000,
                residential=400_000,
                community="B",
            ),
        ]
    )

    result, _ = (
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe
        )
    )

    row = result.iloc[0]

    assert row["community_count"] == 2
    assert bool(row["has_multiple_communities"])
    assert pd.isna(row["community_code"])


def test_multiple_geometries_for_one_location_are_rejected():
    other_geometry = (
        "MULTIPOLYGON (((-114.2 51.0, "
        "-114.19 51.0, -114.19 51.01, "
        "-114.2 51.01, -114.2 51.0)))"
    )

    dataframe = pd.DataFrame(
        [
            _row(
                key="a",
                parcel_id="p1",
                property_id="unit1",
                total=300_000,
                residential=300_000,
            ),
            _row(
                key="b",
                parcel_id="p1",
                property_id="unit2",
                total=400_000,
                residential=400_000,
                geometry=other_geometry,
            ),
        ]
    )

    with pytest.raises(
        CalgaryPropertyLocationAssessmentError,
        match="exactly one geometry",
    ):
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe
        )


def test_output_key_is_unique():
    dataframe = pd.DataFrame(
        [
            _row(
                key="a",
                parcel_id="p1",
                property_id="unit1",
                total=300_000,
                residential=300_000,
            ),
            _row(
                key="b",
                parcel_id="p2",
                property_id="unit2",
                total=400_000,
                residential=400_000,
            ),
        ]
    )

    result, _ = (
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe
        )
    )

    assert result[
        "property_location_assessment_key"
    ].is_unique