import pandas as pd

from src.silver.wildfire_history import (
    deduplicate_wildfire_events,
    infer_province,
    parse_date,
    safe_float,
)


def test_parse_date_handles_compact_and_invalid_dates():
    assert parse_date("20240517") == "2024-05-17"
    assert parse_date("2024-05-17") == "2024-05-17"
    assert parse_date("00000000") is None
    assert parse_date(None) is None


def test_safe_float_handles_invalid_values():
    assert safe_float("12.5") == 12.5
    assert safe_float(3) == 3.0
    assert safe_float("bad") is None
    assert safe_float(None) is None


def test_infer_province_uses_agency_before_coordinate_fallback():
    assert infer_province({"SRC_AGENCY": "BC"}, 49.0, -123.0) == (
        "BC",
        "source_agency",
    )
    assert infer_province({"SRC_AGENCY": "AB"}, 51.0, -114.0) == (
        "AB",
        "source_agency",
    )
    assert infer_province({}, 49.5, -123.0) == ("BC", "coordinate_bbox")
    assert infer_province({}, 51.0, -114.0) == ("AB", "coordinate_bbox")


def test_deduplicate_wildfire_events_keeps_highest_quality_record():
    dataframe = pd.DataFrame(
        [
            {
                "wildfire_event_key": "BC_1",
                "fire_year": 2020,
                "report_date": None,
                "fire_size_ha": None,
                "latitude": 49.0,
                "longitude": -123.0,
            },
            {
                "wildfire_event_key": "BC_1",
                "fire_year": 2020,
                "report_date": "2020-07-01",
                "fire_size_ha": 10.0,
                "latitude": 49.0,
                "longitude": -123.0,
            },
        ]
    )

    result = deduplicate_wildfire_events(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["fire_size_ha"] == 10.0
    assert result.iloc[0]["source_record_count"] == 2


from src.silver.wildfire_history import parse_fire_year


def test_parse_fire_year_handles_sentinel_values():
    assert parse_fire_year("2024") == 2024
    assert parse_fire_year(1946) == 1946
    assert parse_fire_year("-999") is None
    assert parse_fire_year(0) is None
    assert parse_fire_year(None) is None
