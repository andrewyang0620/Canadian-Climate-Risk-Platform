import pandas as pd

from src.silver.municipal_property_assessment import (
    deduplicate_property_assessments,
    extract_wkt_bounds,
    safe_float,
    safe_int,
)


def test_extract_wkt_bounds_from_multipolygon():
    wkt = "MULTIPOLYGON (((-114.0 51.0, -114.2 51.0, -114.2 51.2, -114.0 51.0)))"

    result = extract_wkt_bounds(wkt)

    assert result["min_lon"] == -114.2
    assert result["max_lon"] == -114.0
    assert result["min_lat"] == 51.0
    assert result["max_lat"] == 51.2
    assert result["centroid_lon"] == -114.1
    assert result["centroid_lat"] == 51.1


def test_safe_numeric_helpers():
    assert safe_float("1,234.5") == 1234.5
    assert safe_float("bad") is None
    assert safe_int("2026") == 2026
    assert safe_int(None) is None


def test_deduplicate_property_assessments_keeps_high_quality_record():
    dataframe = pd.DataFrame(
        [
            {
                "property_assessment_key": "calgary_1",
                "assessed_value_total": None,
                "geometry_wkt": None,
                "address_text": None,
                "assessment_year": 2026,
            },
            {
                "property_assessment_key": "calgary_1",
                "assessed_value_total": 100000.0,
                "geometry_wkt": "MULTIPOLYGON (((0 0, 1 0, 0 0)))",
                "address_text": "1 TEST ST",
                "assessment_year": 2026,
            },
        ]
    )

    result = deduplicate_property_assessments(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["assessed_value_total"] == 100000.0
    assert result.iloc[0]["source_record_count"] == 2
