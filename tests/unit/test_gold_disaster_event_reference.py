from __future__ import annotations

import json

import pandas as pd

from src.gold.disaster.reference import build_gold_disaster_event_reference


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "disaster_event_month_key": "100_AB_2016-05",
                "province": "AB",
                "event_month": "2016-05-01",
                "event_year": 2016,
                "event_month_number": 5,
                "source_event_id": "100",
                "disaster_category": "Disaster",
                "disaster_group": "Natural",
                "disaster_subgroup": "Meteorological - Hydrological",
                "event_type_code": "WF",
                "disaster_type": "Wildfire",
                "location_text": "Fort McMurray, AB",
                "source_geometry": "POINT (-111.38 56.72)",
                "event_start_date": "2016-05-01",
                "event_end_date": "2016-05-31",
                "fatalities": 0,
                "injured": 0,
                "evacuated": 88000,
                "estimated_total_cost_cad": 3500000000,
                "normalized_total_cost_cad": 4100000000,
                "description": "Fort McMurray wildfire.",
                "source_row_number": 1,
                "source_name": "canadian_disaster_database",
                "source_record_count": 1,
            },
            {
                "disaster_event_month_key": "200_BC_2021-11",
                "province": "BC",
                "event_month": "2021-11-01",
                "event_year": 2021,
                "event_month_number": 11,
                "source_event_id": "200",
                "disaster_category": "Disaster",
                "disaster_group": "Natural",
                "disaster_subgroup": "Meteorological - Hydrological",
                "event_type_code": "FL",
                "disaster_type": "Flood",
                "location_text": "British Columbia",
                "source_geometry": "POINT (-123.12 49.28)",
                "event_start_date": "2021-11-14",
                "event_end_date": "2021-11-30",
                "fatalities": 5,
                "injured": 0,
                "evacuated": 15000,
                "estimated_total_cost_cad": 1000000000,
                "normalized_total_cost_cad": 1100000000,
                "description": "BC flood event.",
                "source_row_number": 2,
                "source_name": "canadian_disaster_database",
                "source_record_count": 1,
            },
            {
                "disaster_event_month_key": "300_AB_2020-06",
                "province": "AB",
                "event_month": "2020-06-01",
                "event_year": 2020,
                "event_month_number": 6,
                "source_event_id": "300",
                "disaster_category": "Disaster",
                "disaster_group": "Natural",
                "disaster_subgroup": "Meteorological - Hydrological",
                "event_type_code": "ST",
                "disaster_type": "Storms and Severe Thunderstorms",
                "location_text": "Calgary, Alberta",
                "source_geometry": "POINT (-114.07 51.05)",
                "event_start_date": "2020-06-13",
                "event_end_date": "2020-06-13",
                "fatalities": 0,
                "injured": 0,
                "evacuated": 0,
                "estimated_total_cost_cad": 1200000000,
                "normalized_total_cost_cad": 1300000000,
                "description": "Calgary hailstorm.",
                "source_row_number": 3,
                "source_name": "canadian_disaster_database",
                "source_record_count": 1,
            },
        ]
    )


def _location_mapping() -> dict:
    return {
        "version": "test_v1",
        "locations": {
            "Fort McMurray AB": {
                "location_tier": "municipality_region",
                "mapped_geo_level": "CSD",
                "mapped_geo_codes": ["4816037"],
                "mapping_method": "manual_parent_municipality",
                "mapping_confidence": "high",
                "is_grid_backtest_eligible": True,
                "is_province_month_backtest_eligible": True,
            },
            "British Columbia": {
                "location_tier": "province",
                "mapped_geo_level": "PROVINCE",
                "mapped_geo_codes": ["59"],
                "mapping_method": "manual_province_scope",
                "mapping_confidence": "low_for_grid",
                "is_grid_backtest_eligible": False,
                "is_province_month_backtest_eligible": True,
            },
            "Calgary, Alberta": {
                "location_tier": "city",
                "mapped_geo_level": "CSD",
                "mapped_geo_codes": ["4806016"],
                "mapping_method": "manual_exact_bridge_match",
                "mapping_confidence": "high",
                "is_grid_backtest_eligible": True,
                "is_province_month_backtest_eligible": True,
            },
        },
    }


def test_build_gold_disaster_event_reference_preserves_source_rows_and_fields() -> None:
    result, summary = build_gold_disaster_event_reference(
        disaster_event_month=_source_frame(),
        location_mapping=_location_mapping(),
    )

    assert len(result) == 3
    assert summary["source_row_count"] == 3
    assert summary["row_count"] == 3
    assert result["disaster_event_reference_key"].is_unique

    assert "description" in result.columns
    assert "source_geometry" in result.columns
    assert "injured_total" in result.columns
    assert "normalized_total_cost_cad" in result.columns

    assert result["description"].notna().all()
    assert result["source_geometry"].notna().all()


def test_location_mapping_supports_normalized_key_matching() -> None:
    result, summary = build_gold_disaster_event_reference(
        disaster_event_month=_source_frame(),
        location_mapping=_location_mapping(),
    )

    fort = result[result["location_text"].eq("Fort McMurray, AB")].iloc[0]

    assert fort["mapping_confidence"] == "high"
    assert fort["mapped_geo_level"] == "CSD"
    assert json.loads(fort["mapped_geo_codes_json"]) == ["4816037"]
    assert bool(fort["is_grid_backtest_eligible"]) is True

    assert summary["all_grid_backtest_eligible_event_count"] == 2
    assert summary["backtest_window_grid_eligible_event_count"] == 2
    assert summary["backtest_window_province_month_eligible_event_count"] == 3


def test_domain_flags_are_derived_from_disaster_type() -> None:
    result, _ = build_gold_disaster_event_reference(
        disaster_event_month=_source_frame(),
        location_mapping=_location_mapping(),
    )

    wildfire = result[result["location_text"].eq("Fort McMurray, AB")].iloc[0]
    flood = result[result["location_text"].eq("British Columbia")].iloc[0]
    storm = result[result["location_text"].eq("Calgary, Alberta")].iloc[0]

    assert wildfire["disaster_domain"] == "wildfire"
    assert bool(wildfire["is_wildfire_domain_relevant"]) is True
    assert bool(wildfire["is_domain_relevant"]) is True

    assert flood["disaster_domain"] == "flood"
    assert bool(flood["is_flood_domain_relevant"]) is True
    assert bool(flood["is_grid_backtest_eligible"]) is False

    assert storm["disaster_domain"] == "severe_storm_or_climate"
    assert bool(storm["is_climate_domain_relevant"]) is True


def test_backtest_flags_are_set_correctly() -> None:
    result, _ = build_gold_disaster_event_reference(
        disaster_event_month=_source_frame(),
        location_mapping=_location_mapping(),
    )

    assert int(result["is_backtest_window"].sum()) == 3
    assert int(result["is_ab_bc_scope"].sum()) == 3
    assert int(result["is_domain_relevant"].sum()) == 3
    assert int(result["is_backtest_eligible"].sum()) == 3
    assert int(result["is_grid_backtest_eligible"].sum()) == 2
