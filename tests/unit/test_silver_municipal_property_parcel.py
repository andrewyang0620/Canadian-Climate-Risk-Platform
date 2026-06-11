from src.silver.municipal_property_parcel import (
    build_address_text,
    clean_property_parcel_coordinates,
    geojson_geometry_to_wkt,
)


def test_build_address_text_combines_civic_number_and_street_name():
    assert build_address_text("963", "E 15TH AV") == "963 E 15TH AV"
    assert build_address_text(None, "E 15TH AV") == "E 15TH AV"
    assert build_address_text(None, None) is None


def test_geojson_geometry_to_wkt_converts_polygon():
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-123.0, 49.0],
                [-123.1, 49.0],
                [-123.1, 49.1],
                [-123.0, 49.0],
            ]
        ],
    }

    assert (
        geojson_geometry_to_wkt(geometry) == "POLYGON ((-123 49, -123.1 49, -123.1 49.1, -123 49))"
    )


def test_clean_property_parcel_coordinates_nulls_out_invalid_coordinates():
    import pandas as pd

    dataframe = pd.DataFrame(
        [
            {
                "latitude": 49.25,
                "longitude": -123.1,
            },
            {
                "latitude": 0.0,
                "longitude": 0.0,
            },
        ]
    )

    result = clean_property_parcel_coordinates(dataframe)

    assert result.loc[0, "latitude"] == 49.25
    assert pd.isna(result.loc[1, "latitude"])
    assert pd.isna(result.loc[1, "longitude"])
