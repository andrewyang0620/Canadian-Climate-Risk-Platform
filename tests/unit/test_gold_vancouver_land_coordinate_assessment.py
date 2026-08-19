import pandas as pd
import pytest

from src.gold.city.vancouver_land_coordinate_assessment import (
    VancouverLandCoordinateAssessmentError,
    build_gold_vancouver_land_coordinate_assessment,
)


def _tax_row(
    *,
    land_coordinate: str,
    report_year: int,
    folio: str,
    pid: str,
    current_land_value: float = 100_000.0,
    current_improvement_value: float = 200_000.0,
    previous_land_value: float = 90_000.0,
    previous_improvement_value: float = 180_000.0,
    tax_levy: float = 3_000.0,
    tax_assessment_year: int | None = None,
    zoning_district: str = "RS-1",
    zoning_classification: str = "Residential",
    neighbourhood_code: str = "001",
) -> dict:
    if tax_assessment_year is None:
        tax_assessment_year = report_year

    return {
        "source_land_coordinate": land_coordinate,
        "source_pid": pid,
        "source_folio": folio,
        "current_land_value": current_land_value,
        "current_improvement_value": current_improvement_value,
        "current_total_assessed_value": (
            current_land_value
            + current_improvement_value
        ),
        "previous_land_value": previous_land_value,
        "previous_improvement_value": (
            previous_improvement_value
        ),
        "previous_total_assessed_value": (
            previous_land_value
            + previous_improvement_value
        ),
        "tax_levy": tax_levy,
        "tax_assessment_year": tax_assessment_year,
        "report_year": report_year,
        "zoning_district": zoning_district,
        "zoning_classification": zoning_classification,
        "neighbourhood_code": neighbourhood_code,
    }


def test_multiple_folios_same_land_coordinate_year_are_aggregated():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
                current_land_value=100_000.0,
                current_improvement_value=200_000.0,
                previous_land_value=90_000.0,
                previous_improvement_value=180_000.0,
                tax_levy=3_000.0,
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
                current_land_value=150_000.0,
                current_improvement_value=250_000.0,
                previous_land_value=140_000.0,
                previous_improvement_value=230_000.0,
                tax_levy=4_000.0,
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert row["source_land_coordinate"] == "100001"
    assert row["report_year"] == 2026

    assert row["assessment_record_count"] == 2
    assert row["distinct_folio_count"] == 2
    assert row["distinct_pid_count"] == 2

    assert (
        row["current_land_value_sum"]
        == pytest.approx(250_000.0)
    )
    assert (
        row["current_improvement_value_sum"]
        == pytest.approx(450_000.0)
    )
    assert (
        row["current_total_assessed_value_sum"]
        == pytest.approx(700_000.0)
    )

    assert (
        row["previous_land_value_sum"]
        == pytest.approx(230_000.0)
    )
    assert (
        row["previous_improvement_value_sum"]
        == pytest.approx(410_000.0)
    )
    assert (
        row["previous_total_assessed_value_sum"]
        == pytest.approx(640_000.0)
    )

    assert row["tax_levy_sum"] == pytest.approx(
        7_000.0
    )

    assert summary["input_row_count"] == 2
    assert summary["usable_input_row_count"] == 2
    assert summary["output_row_count"] == 1
    assert summary["multi_record_group_count"] == 1
    assert (
        summary[
            "maximum_assessment_records_per_group"
        ]
        == 2
    )


def test_different_report_years_are_not_aggregated_together():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2025,
                folio="folio_1",
                pid="pid_1",
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    assert len(result) == 2

    assert set(
        result["report_year"].tolist()
    ) == {
        2025,
        2026,
    }

    assert (
        result["source_land_coordinate"]
        == "100001"
    ).all()

    assert (
        result["assessment_record_count"]
        == 1
    ).all()

    assert summary["report_year_min"] == 2025
    assert summary["report_year_max"] == 2026
    assert summary["latest_report_year"] == 2026
    assert (
        summary["latest_report_year_row_count"]
        == 1
    )


def test_multiple_neighbourhood_codes_are_flagged():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
                neighbourhood_code="001",
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
                neighbourhood_code="002",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert pd.isna(row["neighbourhood_code"])
    assert row["neighbourhood_code_count"] == 2
    assert bool(
        row["has_multiple_neighbourhood_codes"]
    )

    assert (
        summary[
            "multiple_neighbourhood_group_count"
        ]
        == 1
    )


def test_single_neighbourhood_code_is_preserved():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
                neighbourhood_code="001",
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
                neighbourhood_code="001",
            ),
        ]
    )

    result, _ = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    row = result.iloc[0]

    assert row["neighbourhood_code"] == "001"
    assert row["neighbourhood_code_count"] == 1
    assert not bool(
        row["has_multiple_neighbourhood_codes"]
    )


def test_multiple_zoning_districts_are_flagged():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
                zoning_district="RS-1",
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
                zoning_district="CD-1",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    row = result.iloc[0]

    assert pd.isna(row["zoning_district"])
    assert row["zoning_district_count"] == 2
    assert bool(
        row["has_multiple_zoning_districts"]
    )

    assert (
        summary["multiple_zoning_group_count"]
        == 1
    )


def test_duplicate_pid_does_not_cause_double_identity_assumption():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="shared_pid",
                current_land_value=100_000.0,
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_2",
                pid="shared_pid",
                current_land_value=150_000.0,
            ),
        ]
    )

    result, _ = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    row = result.iloc[0]

    assert row["assessment_record_count"] == 2
    assert row["distinct_folio_count"] == 2
    assert row["distinct_pid_count"] == 1

    assert (
        row["current_land_value_sum"]
        == pytest.approx(250_000.0)
    )


def test_multiple_tax_assessment_years_in_same_group_are_rejected():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                tax_assessment_year=2026,
                folio="folio_1",
                pid="pid_1",
            ),
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                tax_assessment_year=2025,
                folio="folio_2",
                pid="pid_2",
            ),
        ]
    )

    with pytest.raises(
        VancouverLandCoordinateAssessmentError,
        match="multiple tax_assessment_year",
    ):
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )


def test_blank_land_coordinate_is_excluded():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
            ),
            _tax_row(
                land_coordinate="   ",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
            ),
        ]
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    assert len(result) == 1

    assert (
        result.iloc[0]["source_land_coordinate"]
        == "100001"
    )

    assert summary["input_row_count"] == 2
    assert summary["usable_input_row_count"] == 1


def test_land_coordinate_year_key_is_unique():
    dataframe = pd.DataFrame(
        [
            _tax_row(
                land_coordinate="100001",
                report_year=2026,
                folio="folio_1",
                pid="pid_1",
            ),
            _tax_row(
                land_coordinate="100002",
                report_year=2026,
                folio="folio_2",
                pid="pid_2",
            ),
        ]
    )

    result, _ = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
        )
    )

    assert (
        result[
            "land_coordinate_assessment_key"
        ].notna().all()
    )

    assert (
        result[
            "land_coordinate_assessment_key"
        ].is_unique
    )

    assert not result.duplicated(
        subset=[
            "source_land_coordinate",
            "report_year",
        ]
    ).any()