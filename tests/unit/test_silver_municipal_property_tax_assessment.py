import pandas as pd

from src.silver.municipal_property_tax_assessment import (
    build_address_text,
    build_property_tax_assessment_key,
    safe_int,
    safe_non_negative_float,
    standardize_vancouver_property_tax_chunk,
)


def test_build_address_text_handles_ranges_and_missing_values():
    assert build_address_text("100", "120", "MAIN ST") == "100-120 MAIN ST"
    assert build_address_text("1005", "928", "RICHARDS ST") == "1005 / 928 RICHARDS ST"
    assert build_address_text(None, "120", "MAIN ST") == "120 MAIN ST"
    assert build_address_text(None, None, "MAIN ST") == "MAIN ST"
    assert build_address_text(None, None, None) is None


def test_safe_numeric_helpers():
    assert safe_non_negative_float("1,234.50") == 1234.5
    assert safe_non_negative_float("-1") is None
    assert safe_non_negative_float(None) is None
    assert safe_int("2026") == 2026
    assert safe_int(None) is None


def test_build_property_tax_assessment_key_is_stable():
    first = build_property_tax_assessment_key(
        source_pid="001-001-001",
        source_folio="123",
        source_land_coordinate="456",
        report_year="2026",
        source_row_number=1,
    )
    second = build_property_tax_assessment_key(
        source_pid="001-001-001",
        source_folio="123",
        source_land_coordinate="456",
        report_year="2026",
        source_row_number=1,
    )

    assert first == second
    assert first.startswith("vancouver_")


def test_standardize_vancouver_property_tax_chunk_outputs_expected_columns():
    raw = pd.DataFrame(
        [
            {
                "pid": "001-001-001",
                "legal_type": "LAND",
                "folio": "123",
                "land_coordinate": "456",
                "zoning_district": "C-2",
                "zoning_classification": "Commercial",
                "lot": "1",
                "plan": "VAP1",
                "block": "A",
                "district_lot": "1",
                "from_civic_number": "100",
                "to_civic_number": "120",
                "street_name": "MAIN ST",
                "property_postal_code": "V6A 1A1",
                "narrative_legal_line1": "LOT 1 PLAN VAP1",
                "narrative_legal_line2": "DISTRICT LOT 1",
                "narrative_legal_line3": None,
                "narrative_legal_line4": None,
                "narrative_legal_line5": None,
                "current_land_value": "1000",
                "current_improvement_value": "500",
                "tax_assessment_year": "2026",
                "previous_land_value": "900",
                "previous_improvement_value": "400",
                "year_built": "1999",
                "big_improvement_year": "2000",
                "tax_levy": "10.5",
                "neighbourhood_code": "001",
                "report_year": "2026",
                "note": None,
            }
        ]
    )

    result = standardize_vancouver_property_tax_chunk(raw, source_row_start=1)

    assert len(result) == 1
    assert result.loc[0, "city"] == "vancouver"
    assert result.loc[0, "province"] == "BC"
    assert result.loc[0, "source_name"] == "vancouver_property_tax"
    assert result.loc[0, "source_pid"] == "001-001-001"
    assert result.loc[0, "source_land_coordinate"] == "456"
    assert result.loc[0, "address_text"] == "100-120 MAIN ST"
    assert result.loc[0, "legal_description_text"] == "LOT 1 PLAN VAP1 DISTRICT LOT 1"
    assert result.loc[0, "current_total_assessed_value"] == 1500.0
    assert result.loc[0, "previous_total_assessed_value"] == 1300.0
    assert result.loc[0, "report_year"] == 2026
    assert result.loc[0, "source_row_number"] == 1


def test_standardize_vancouver_property_tax_chunk_sums_available_values_when_one_side_missing():
    raw = pd.DataFrame(
        [
            {
                "pid": "001-001-001",
                "folio": "123",
                "land_coordinate": "456",
                "current_land_value": None,
                "current_improvement_value": "500",
                "previous_land_value": "900",
                "previous_improvement_value": None,
                "report_year": "2026",
            }
        ]
    )

    result = standardize_vancouver_property_tax_chunk(raw, source_row_start=1)

    assert result.loc[0, "current_total_assessed_value"] == 500.0
    assert result.loc[0, "previous_total_assessed_value"] == 900.0
