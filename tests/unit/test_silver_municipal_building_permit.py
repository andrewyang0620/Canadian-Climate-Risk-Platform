import pandas as pd

from src.silver.municipal_building_permit import (
    deduplicate_building_permits,
    geom_json_to_point_wkt,
    parse_vancouver_geo_point,
    point_wkt,
    safe_float,
)


def test_parse_vancouver_geo_point():
    assert parse_vancouver_geo_point("49.25105, -123.0447929") == (
        49.25105,
        -123.0447929,
    )
    assert parse_vancouver_geo_point(None) is None
    assert parse_vancouver_geo_point("bad") is None


def test_geom_json_to_point_wkt():
    value = '{"coordinates": [-123.0447929, 49.25105], "type": "Point"}'

    assert geom_json_to_point_wkt(value) == "POINT (-123.0447929 49.25105)"


def test_point_wkt_handles_missing_values():
    assert point_wkt("-114.0", "51.0") == "POINT (-114.0 51.0)"
    assert point_wkt(None, "51.0") is None


def test_safe_float_rejects_invalid_values():
    assert safe_float("1,234.5") == 1234.5
    assert safe_float("bad") is None
    assert safe_float(None) is None


def test_deduplicate_building_permits_keeps_high_quality_record():
    dataframe = pd.DataFrame(
        [
            {
                "building_permit_key": "calgary_1",
                "issue_date": None,
                "address_text": None,
                "geometry_wkt": None,
                "estimated_project_cost": None,
            },
            {
                "building_permit_key": "calgary_1",
                "issue_date": "2026-01-01",
                "address_text": "1 TEST ST",
                "geometry_wkt": "POINT (-114 51)",
                "estimated_project_cost": 1000.0,
            },
        ]
    )

    result = deduplicate_building_permits(dataframe)

    assert len(result) == 1
    assert result.iloc[0]["issue_date"] == "2026-01-01"
    assert result.iloc[0]["source_record_count"] == 2


from src.silver.municipal_building_permit import safe_non_negative_float


def test_safe_non_negative_float_rejects_negative_values():
    assert safe_non_negative_float("1000") == 1000.0
    assert safe_non_negative_float("0") == 0.0
    assert safe_non_negative_float("-1") is None
    assert safe_non_negative_float(None) is None


from src.silver.municipal_building_permit import safe_non_negative_float


def test_safe_non_negative_float_rejects_negative_values():
    assert safe_non_negative_float("1000") == 1000.0
    assert safe_non_negative_float("0") == 0.0
    assert safe_non_negative_float("-1") is None
    assert safe_non_negative_float(None) is None


from src.silver.municipal_building_permit import clean_building_permit_coordinates


def test_clean_building_permit_coordinates_nulls_out_invalid_city_coordinates():
    import pandas as pd

    dataframe = pd.DataFrame(
        [
            {
                "city": "calgary",
                "latitude": 51.0,
                "longitude": -114.0,
                "geometry_wkt": "POINT (-114.0 51.0)",
            },
            {
                "city": "calgary",
                "latitude": 0.0,
                "longitude": 0.0,
                "geometry_wkt": "POINT (0 0)",
            },
            {
                "city": "vancouver",
                "latitude": 49.25,
                "longitude": -123.1,
                "geometry_wkt": "POINT (-123.1 49.25)",
            },
        ]
    )

    result = clean_building_permit_coordinates(dataframe)

    assert result.loc[0, "latitude"] == 51.0
    assert pd.isna(result.loc[1, "latitude"])
    assert pd.isna(result.loc[1, "longitude"])
    assert pd.isna(result.loc[1, "geometry_wkt"])
    assert result.loc[2, "longitude"] == -123.1
