import pandas as pd

from src.silver.hydat_archive import (
    deduplicate_hydro_daily,
    normalize_province,
    point_wkt,
    safe_float,
)


def test_normalize_province_maps_target_values():
    assert normalize_province("BC") == "BC"
    assert normalize_province("British Columbia") == "BC"
    assert normalize_province("AB") == "AB"
    assert normalize_province("Alberta") == "AB"
    assert normalize_province("ON") == "ON"
    assert normalize_province(None) is None


def test_point_wkt_handles_valid_and_missing_coordinates():
    assert point_wkt("-123.1", "49.2") == "POINT (-123.1 49.2)"
    assert point_wkt(None, "49.2") is None
    assert point_wkt("-123.1", None) is None


def test_safe_float_handles_invalid_values():
    assert safe_float("12.5") == 12.5
    assert safe_float(3) == 3.0
    assert safe_float("bad") is None
    assert safe_float(None) is None


def test_deduplicate_hydro_daily_keeps_one_row_per_key():
    dataframe = pd.DataFrame(
        [
            {
                "hydro_daily_key": "01_2020-01-01_flow",
                "station_id": "01",
                "observation_date": "2020-01-01",
                "measurement_type": "flow",
                "measurement_value": 1.0,
            },
            {
                "hydro_daily_key": "01_2020-01-01_flow",
                "station_id": "01",
                "observation_date": "2020-01-01",
                "measurement_type": "flow",
                "measurement_value": 2.0,
            },
        ]
    )

    result = deduplicate_hydro_daily(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["measurement_value"] == 2.0
    assert result.iloc[0]["source_record_count"] == 2
