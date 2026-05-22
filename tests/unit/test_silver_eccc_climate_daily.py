import gzip
import json

from src.silver.eccc_climate_daily import (
    clean_str,
    normalize_eccc_climate_feature,
    parse_local_date,
    safe_float,
    standardize_eccc_climate_jsonl_gzip,
)


def test_parse_local_date_extracts_date_part():
    assert parse_local_date("2024-01-02 00:00:00") == "2024-01-02"
    assert parse_local_date("2024-01-02") == "2024-01-02"
    assert parse_local_date(None) is None


def test_safe_float_and_clean_str():
    assert safe_float("12.5") == 12.5
    assert safe_float(None) is None
    assert safe_float("bad") is None

    assert clean_str("  BC ") == "BC"
    assert clean_str("") is None
    assert clean_str(None) is None


def test_normalize_eccc_climate_feature_maps_expected_fields():
    feature = {
        "id": "1066488.2024.1.2",
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [-130.29, 54.32],
        },
        "properties": {
            "STATION_NAME": "PRINCE RUPERT MONT CIRC",
            "CLIMATE_IDENTIFIER": "1066488",
            "LOCAL_DATE": "2024-01-02 00:00:00",
            "PROVINCE_CODE": "BC",
            "LOCAL_YEAR": 2024,
            "LOCAL_MONTH": 1,
            "LOCAL_DAY": 2,
            "MEAN_TEMPERATURE": "4.5",
            "MIN_TEMPERATURE": "2.1",
            "MAX_TEMPERATURE": "7.0",
            "TOTAL_PRECIPITATION": "3.2",
            "TOTAL_RAIN": "3.2",
            "TOTAL_SNOW": "0",
        },
    }

    row = normalize_eccc_climate_feature(feature)

    assert row["climate_daily_key"] == "1066488_2024-01-02"
    assert row["station_id"] == "1066488"
    assert row["province"] == "BC"
    assert row["observation_date"] == "2024-01-02"
    assert row["observation_year"] == 2024
    assert row["latitude"] == 54.32
    assert row["longitude"] == -130.29
    assert row["max_temp_c"] == 7.0
    assert row["total_precip_mm"] == 3.2


def test_normalize_eccc_climate_feature_filters_non_target_province():
    feature = {
        "geometry": {"type": "Point", "coordinates": [-100, 50]},
        "properties": {
            "CLIMATE_IDENTIFIER": "123",
            "LOCAL_DATE": "2024-01-01 00:00:00",
            "PROVINCE_CODE": "SK",
        },
    }

    assert normalize_eccc_climate_feature(feature) is None


def test_standardize_eccc_climate_jsonl_gzip(tmp_path):
    path = tmp_path / "climate.jsonl.gz"

    rows = [
        {
            "id": "1",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-123.1, 49.2]},
            "properties": {
                "STATION_NAME": "VANCOUVER",
                "CLIMATE_IDENTIFIER": "101",
                "LOCAL_DATE": "2024-01-01 00:00:00",
                "PROVINCE_CODE": "BC",
                "LOCAL_YEAR": 2024,
                "LOCAL_MONTH": 1,
                "LOCAL_DAY": 1,
                "MAX_TEMPERATURE": 10.0,
                "TOTAL_PRECIPITATION": 5.0,
            },
        },
        {
            "id": "2",
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-114.1, 51.0]},
            "properties": {
                "STATION_NAME": "CALGARY",
                "CLIMATE_IDENTIFIER": "202",
                "LOCAL_DATE": "2024-01-01 00:00:00",
                "PROVINCE_CODE": "AB",
                "LOCAL_YEAR": 2024,
                "LOCAL_MONTH": 1,
                "LOCAL_DAY": 1,
                "MIN_TEMPERATURE": -5.0,
                "TOTAL_RAIN": 0.0,
            },
        },
    ]

    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")

    dataframe = standardize_eccc_climate_jsonl_gzip(path)

    assert dataframe.shape[0] == 2
    assert set(dataframe["province"]) == {"BC", "AB"}
    assert set(dataframe["observation_year"]) == {2024}
    assert "max_temp_c" in dataframe.columns
    assert "total_precip_mm" in dataframe.columns
