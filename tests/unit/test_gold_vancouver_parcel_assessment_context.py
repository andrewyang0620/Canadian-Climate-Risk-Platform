import pandas as pd
import pytest

from src.gold.city.vancouver_parcel_assessment_context import (
    VancouverParcelAssessmentContextError,
    build_gold_vancouver_parcel_assessment_context,
)


def _parcel(
    *,
    parcel_key: str,
    tax_coord: str | None,
    source_parcel_id: str | None = None,
) -> dict:
    return {
        "property_parcel_key": parcel_key,
        "city": "vancouver",
        "source_parcel_id": (
            source_parcel_id
            if source_parcel_id is not None
            else f"source_{parcel_key}"
        ),
        "source_tax_coord": tax_coord,
    }


def _bridge(
    *,
    parcel_key: str,
    land_coordinate: str,
    parcel_count: int = 1,
) -> dict:
    ambiguous = parcel_count > 1

    return {
        "property_parcel_key": parcel_key,
        "source_land_coordinate": land_coordinate,
        "parcel_count_for_land_coordinate": parcel_count,
        "is_ambiguous_land_coordinate": ambiguous,
        "mapping_method": (
            "exact_land_coordinate_1_to_many"
            if ambiguous
            else "exact_land_coordinate_1_to_1"
        ),
    }


def _assessment(
    *,
    land_coordinate: str,
    report_year: int = 2026,
    total_value: float = 300_000.0,
    land_value: float = 100_000.0,
    improvement_value: float = 200_000.0,
    tax_levy: float = 3_000.0,
) -> dict:
    return {
        "source_land_coordinate": land_coordinate,
        "report_year": report_year,
        "tax_assessment_year": report_year,
        "assessment_record_count": 1,
        "distinct_folio_count": 1,
        "distinct_pid_count": 1,
        "current_land_value_sum": land_value,
        "current_improvement_value_sum": improvement_value,
        "current_total_assessed_value_sum": total_value,
        "previous_land_value_sum": 90_000.0,
        "previous_improvement_value_sum": 180_000.0,
        "previous_total_assessed_value_sum": 270_000.0,
        "tax_levy_sum": tax_levy,
        "zoning_district": "RS-1",
        "zoning_classification": "Residential",
        "neighbourhood_code": "001",
        "has_multiple_neighbourhood_codes": False,
    }


def test_exact_one_to_one_mapping_populates_exact_value_fields():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
                parcel_count=1,
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
                report_year=2026,
                total_value=300_000.0,
                land_value=100_000.0,
                improvement_value=200_000.0,
                tax_levy=3_000.0,
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert bool(row["has_parcel_bridge"])
    assert bool(row["has_latest_assessment"])

    assert not bool(
        row["assessment_mapping_ambiguous"]
    )

    assert bool(
        row["assessment_mapping_exact_1_to_1"]
    )

    assert (
        row[
            "land_coordinate_current_total_assessed_value"
        ]
        == pytest.approx(300_000.0)
    )

    assert (
        row["exact_mapped_current_land_value"]
        == pytest.approx(100_000.0)
    )

    assert (
        row[
            "exact_mapped_current_improvement_value"
        ]
        == pytest.approx(200_000.0)
    )

    assert (
        row[
            "exact_mapped_current_total_assessed_value"
        ]
        == pytest.approx(300_000.0)
    )

    assert (
        row["exact_mapped_tax_levy"]
        == pytest.approx(3_000.0)
    )

    assert summary["parcel_input_count"] == 1
    assert summary["output_row_count"] == 1
    assert summary["bridge_parcel_count"] == 1
    assert (
        summary[
            "latest_assessment_parcel_count"
        ]
        == 1
    )
    assert (
        summary[
            "exact_1_to_1_assessment_parcel_count"
        ]
        == 1
    )
    assert (
        summary[
            "ambiguous_assessment_parcel_count"
        ]
        == 0
    )


def test_one_to_many_mapping_keeps_context_but_nulls_exact_values():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
            _parcel(
                parcel_key="parcel_2",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
                parcel_count=2,
            ),
            _bridge(
                parcel_key="parcel_2",
                land_coordinate="100001",
                parcel_count=2,
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
                report_year=2026,
                total_value=900_000.0,
                land_value=300_000.0,
                improvement_value=600_000.0,
                tax_levy=9_000.0,
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    assert len(result) == 2

    assert result["has_parcel_bridge"].all()
    assert result["has_latest_assessment"].all()

    assert result[
        "assessment_mapping_ambiguous"
    ].all()

    assert not result[
        "assessment_mapping_exact_1_to_1"
    ].any()

    # Both parcels may see the shared land-coordinate context.
    assert (
        result["land_coordinate_current_total_assessed_value"] == 900_000.0
    ).all()

    # But the value is not assigned as an exact parcel value.
    assert result[
        "exact_mapped_current_land_value"
    ].isna().all()

    assert result[
        "exact_mapped_current_improvement_value"
    ].isna().all()

    assert result[
        "exact_mapped_current_total_assessed_value"
    ].isna().all()

    assert result[
        "exact_mapped_tax_levy"
    ].isna().all()

    assert summary["bridge_parcel_count"] == 2
    assert (
        summary[
            "latest_assessment_parcel_count"
        ]
        == 2
    )
    assert (
        summary[
            "exact_1_to_1_assessment_parcel_count"
        ]
        == 0
    )
    assert (
        summary[
            "ambiguous_assessment_parcel_count"
        ]
        == 2
    )


def test_parcel_without_bridge_is_retained():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord=None,
            ),
            _parcel(
                parcel_key="parcel_2",
                tax_coord="100002",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_2",
                land_coordinate="100002",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100002",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    assert len(result) == 2

    missing = result[
        result["property_parcel_key"]
        == "parcel_1"
    ].iloc[0]

    assert not bool(
        missing["has_parcel_bridge"]
    )
    assert not bool(
        missing["has_latest_assessment"]
    )
    assert not bool(
        missing["assessment_mapping_exact_1_to_1"]
    )

    assert pd.isna(
        missing[
            "land_coordinate_current_total_assessed_value"
        ]
    )

    assert pd.isna(
        missing[
            "exact_mapped_current_total_assessed_value"
        ]
    )

    assert (
        summary["parcel_without_bridge_count"]
        == 1
    )
    assert summary["output_row_count"] == 2


def test_bridge_without_latest_assessment_is_retained():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
        ]
    )

    # Latest assessment year in the entire input is 2026,
    # but land coordinate 100001 exists only in 2025.
    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
                report_year=2025,
            ),
            _assessment(
                land_coordinate="999999",
                report_year=2026,
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    row = result.iloc[0]

    assert bool(row["has_parcel_bridge"])
    assert not bool(
        row["has_latest_assessment"]
    )

    assert not bool(
        row["assessment_mapping_exact_1_to_1"]
    )

    assert pd.isna(row["report_year"])

    assert pd.isna(
        row[
            "land_coordinate_current_total_assessed_value"
        ]
    )

    assert summary["latest_report_year"] == 2026
    assert summary["bridge_parcel_count"] == 1
    assert (
        summary[
            "latest_assessment_parcel_count"
        ]
        == 0
    )
    assert (
        summary[
            "bridge_without_latest_assessment_count"
        ]
        == 1
    )


def test_only_latest_report_year_is_used():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
                report_year=2025,
                total_value=250_000.0,
                land_value=90_000.0,
                improvement_value=160_000.0,
            ),
            _assessment(
                land_coordinate="100001",
                report_year=2026,
                total_value=300_000.0,
                land_value=100_000.0,
                improvement_value=200_000.0,
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    row = result.iloc[0]

    assert row["report_year"] == 2026

    assert (
        row[
            "land_coordinate_current_total_assessed_value"
        ]
        == pytest.approx(300_000.0)
    )

    assert (
        row[
            "exact_mapped_current_total_assessed_value"
        ]
        == pytest.approx(300_000.0)
    )

    assert summary["latest_report_year"] == 2026


def test_row_conservation_keeps_complete_parcel_universe():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
            _parcel(
                parcel_key="parcel_2",
                tax_coord="100002",
            ),
            _parcel(
                parcel_key="parcel_3",
                tax_coord=None,
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
            _bridge(
                parcel_key="parcel_2",
                land_coordinate="100002",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
            ),
            _assessment(
                land_coordinate="100002",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )
    )

    assert len(result) == len(parcels)
    assert result["property_parcel_key"].is_unique

    assert set(
        result["property_parcel_key"]
    ) == {
        "parcel_1",
        "parcel_2",
        "parcel_3",
    }

    assert summary["parcel_input_count"] == 3
    assert summary["output_row_count"] == 3


def test_duplicate_parcel_keys_are_rejected():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100002",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
            ),
        ]
    )

    with pytest.raises(
        VancouverParcelAssessmentContextError,
        match="Parcel keys must be unique",
    ):
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )


def test_duplicate_bridge_rows_per_parcel_are_rejected():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100002",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
            ),
        ]
    )

    with pytest.raises(
        VancouverParcelAssessmentContextError,
        match="at most one row per parcel",
    ):
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )


def test_duplicate_latest_assessment_land_coordinate_is_rejected():
    parcels = pd.DataFrame(
        [
            _parcel(
                parcel_key="parcel_1",
                tax_coord="100001",
            ),
        ]
    )

    bridge = pd.DataFrame(
        [
            _bridge(
                parcel_key="parcel_1",
                land_coordinate="100001",
            ),
        ]
    )

    assessments = pd.DataFrame(
        [
            _assessment(
                land_coordinate="100001",
                report_year=2026,
                total_value=300_000.0,
            ),
            _assessment(
                land_coordinate="100001",
                report_year=2026,
                total_value=350_000.0,
            ),
        ]
    )

    with pytest.raises(
        VancouverParcelAssessmentContextError,
        match="one row per source_land_coordinate",
    ):
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
        )