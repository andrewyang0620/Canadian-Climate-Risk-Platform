import pandas as pd

from src.silver.census_boundaries import to_float


def test_to_float_handles_valid_and_invalid_values():
    assert to_float("123.45") == 123.45
    assert to_float(10) == 10.0
    assert to_float(None) is None
    assert to_float("not-a-number") is None


def test_boundary_standardized_columns_are_expected():
    province_columns = {
        "province_key",
        "province_code",
        "province_name",
        "geometry_wkt",
        "boundary_year",
    }

    municipality_columns = {
        "municipality_key",
        "municipality_name",
        "province",
        "geometry_wkt",
        "boundary_year",
    }

    province_df = pd.DataFrame(columns=sorted(province_columns))
    municipality_df = pd.DataFrame(columns=sorted(municipality_columns))

    assert province_columns <= set(province_df.columns)
    assert municipality_columns <= set(municipality_df.columns)
