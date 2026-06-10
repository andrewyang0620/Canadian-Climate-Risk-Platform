import pandas as pd

from src.silver.municipal_development_permit import (
    clean_date_series,
    clean_development_permit_coordinates,
    safe_float,
    safe_int,
)


def test_safe_numeric_helpers():
    assert safe_float("1,234.5") == 1234.5
    assert safe_float(None) is None
    assert safe_int("14") == 14
    assert safe_int(None) is None


def test_clean_date_series_removes_out_of_range_dates():
    series = pd.Series(["2026-01-01", "1909-10-27", None])

    result = clean_date_series(series, min_year=1979, max_year=2030)

    assert result.iloc[0].year == 2026
    assert pd.isna(result.iloc[1])
    assert pd.isna(result.iloc[2])


def test_clean_development_permit_coordinates_nulls_out_invalid_coordinates():
    dataframe = pd.DataFrame(
        [
            {
                "latitude": 51.0,
                "longitude": -114.0,
                "geometry_wkt": "POINT (-114.0 51.0)",
            },
            {
                "latitude": 0.0,
                "longitude": 0.0,
                "geometry_wkt": "POINT (0 0)",
            },
        ]
    )

    result = clean_development_permit_coordinates(dataframe)

    assert result.loc[0, "latitude"] == 51.0
    assert pd.isna(result.loc[1, "latitude"])
    assert pd.isna(result.loc[1, "longitude"])
    assert pd.isna(result.loc[1, "geometry_wkt"])
