import pandas as pd

from src.silver.canadian_disaster_database import (
    deduplicate_disaster_event_months,
    event_month_range,
    parse_date,
    safe_float,
    target_provinces_from_value,
)


def test_target_provinces_from_value_detects_target_provinces():
    assert target_provinces_from_value("British Columbia") == ["BC"]
    assert target_provinces_from_value("Alberta") == ["AB"]
    assert target_provinces_from_value("BC, AB") == ["AB", "BC"]
    assert target_provinces_from_value("Ontario") == []


def test_event_month_range_expands_multi_month_events():
    assert event_month_range("2020-01-15", "2020-03-02") == [
        "2020-01-01",
        "2020-02-01",
        "2020-03-01",
    ]


def test_parse_date_handles_iso_and_excel_serial():
    assert parse_date("2020-01-15") == "2020-01-15"
    assert parse_date(43845) == "2020-01-15"
    assert parse_date(None) is None


def test_safe_float_cleans_currency_and_commas():
    assert safe_float("$1,234.50") == 1234.5
    assert safe_float("bad") is None
    assert safe_float(None) is None


def test_deduplicate_disaster_event_months_keeps_one_row_per_key():
    dataframe = pd.DataFrame(
        [
            {
                "disaster_event_month_key": "event_1_BC_2020-01",
                "disaster_type": None,
                "event_start_date": "2020-01-01",
                "event_end_date": "2020-01-02",
                "location_text": None,
                "source_row_number": 2,
            },
            {
                "disaster_event_month_key": "event_1_BC_2020-01",
                "disaster_type": "Flood",
                "event_start_date": "2020-01-01",
                "event_end_date": "2020-01-02",
                "location_text": "Vancouver",
                "source_row_number": 1,
            },
        ]
    )

    result = deduplicate_disaster_event_months(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["disaster_type"] == "Flood"
    assert result.iloc[0]["source_record_count"] == 2


from src.silver.canadian_disaster_database import build_column_map


def test_build_column_map_matches_actual_cdd_columns():
    import pandas as pd

    dataframe = pd.DataFrame(
        columns=[
            "EVENT_ID",
            "EVENT_CATEGORY_NAME",
            "EVENT_GROUP_NAME",
            "EVENT_SUBGROUP_NAME",
            "EVENT_TYPE",
            "EVENT_TYPE_DESCRIPTION",
            "EVENT_START_DATE",
            "EVENT_END_DATE",
            "DEAD",
            "INJURED",
            "EVACUATED",
            "PLACE",
            "COMMENT",
            "TOTAL_COST",
            "GEOG_OBJ",
            "PROVINCES_AFFECTED / PROVINCES AFFECTÉES",
        ]
    )

    column_map = build_column_map(dataframe)

    assert column_map["event_id"] == "EVENT_ID"
    assert column_map["province"] == "PROVINCES_AFFECTED / PROVINCES AFFECTÉES"
    assert column_map["start_date"] == "EVENT_START_DATE"
    assert column_map["end_date"] == "EVENT_END_DATE"
    assert column_map["event_type_code"] == "EVENT_TYPE"
    assert column_map["event_type_description"] == "EVENT_TYPE_DESCRIPTION"
    assert column_map["fatalities"] == "DEAD"
    assert column_map["estimated_total_cost"] == "TOTAL_COST"


def test_target_provinces_from_value_detects_space_separated_codes():
    assert target_provinces_from_value("BC AB") == ["AB", "BC"]
    assert target_provinces_from_value("ON QC") == []
    assert target_provinces_from_value("BC") == ["BC"]
    assert target_provinces_from_value("AB") == ["AB"]
