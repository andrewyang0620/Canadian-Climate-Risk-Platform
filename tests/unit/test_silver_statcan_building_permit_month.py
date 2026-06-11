from pathlib import Path

import pandas as pd

from src.silver.statcan_building_permit_month import (
    build_statcan_building_permit_month_silver,
)


def test_build_statcan_building_permit_month_silver_filters_target_geos(tmp_path):
    raw_path = Path(tmp_path) / "statcan_building_permits_raw.csv"

    raw = pd.DataFrame(
        [
            {
                "REF_DATE": "2026-01",
                "GEO": "Alberta",
                "DGUID": "2021A000248",
                "Type of building": "Total residential and non-residential",
                "Type of work": "Types of work, total",
                "Variables": "Value of permits",
                "Seasonal adjustment, value type": "Unadjusted, current",
                "UOM": "Dollars",
                "UOM_ID": "81",
                "SCALAR_FACTOR": "thousands",
                "SCALAR_ID": "3",
                "VECTOR": "v1",
                "COORDINATE": "1.1.1.1.1",
                "VALUE": "1234.5",
                "STATUS": None,
                "SYMBOL": None,
                "TERMINATED": None,
                "DECIMALS": "1",
            },
            {
                "REF_DATE": "2026-01",
                "GEO": "Ontario",
                "DGUID": "2021A000235",
                "Type of building": "Total residential and non-residential",
                "Type of work": "Types of work, total",
                "Variables": "Value of permits",
                "Seasonal adjustment, value type": "Unadjusted, current",
                "UOM": "Dollars",
                "UOM_ID": "81",
                "SCALAR_FACTOR": "thousands",
                "SCALAR_ID": "3",
                "VECTOR": "v2",
                "COORDINATE": "2.1.1.1.1",
                "VALUE": "9999",
                "STATUS": None,
                "SYMBOL": None,
                "TERMINATED": None,
                "DECIMALS": "1",
            },
        ]
    )
    raw.to_csv(raw_path, index=False)

    result = build_statcan_building_permit_month_silver(raw_path, chunksize=1)

    assert len(result) == 1
    assert result.loc[0, "geo_name"] == "Alberta"
    assert result.loc[0, "geo_level"] == "province"
    assert result.loc[0, "province_code"] == "AB"
    assert result.loc[0, "value"] == 1234.5
    assert result.loc[0, "value_scaled"] == 1234500.0
    assert result.loc[0, "reference_year"] == 2026
    assert result.loc[0, "reference_month_number"] == 1
    assert result["statcan_building_permit_month_key"].isna().sum() == 0
    assert result["statcan_building_permit_month_key"].duplicated().sum() == 0
    assert result["source_record_count"].tolist() == [1]
