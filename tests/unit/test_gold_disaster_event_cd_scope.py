from __future__ import annotations

import json

import pandas as pd
import pytest

from src.gold.disaster.event_cd_scope import (
    GoldDisasterEventCDScopeError,
    build_gold_disaster_event_cd_scope_reference,
)
from src.gold.disaster.event_cd_scope_validation import (
    GoldDisasterEventCDScopeValidationError,
    validate_gold_disaster_event_cd_scope_reference,
)


def _event_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "disaster_event_reference_key": "disaster_event_ref__event_1",
                "source_disaster_event_key": "event_1",
                "reference_month": "2016-05",
                "event_year": 2016,
                "event_month_number": 5,
                "province_key": "AB",
                "disaster_domain": "wildfire",
                "location_text": "Fort McMurray AB",
                "location_tier": "municipality_region",
                "mapped_geo_level": "CSD",
                "mapped_geo_codes_json": json.dumps(["4816037"]),
                "mapping_confidence": "high",
                "mapping_method": "manual_parent_municipality",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_2",
                "source_disaster_event_key": "event_2",
                "reference_month": "2021-11",
                "event_year": 2021,
                "event_month_number": 11,
                "province_key": "BC",
                "disaster_domain": "flood",
                "location_text": "Southern British Columbia",
                "location_tier": "region",
                "mapped_geo_level": "CD_GROUP",
                "mapped_geo_codes_json": json.dumps(["5905", "5935"]),
                "mapping_confidence": "medium",
                "mapping_method": "manual_large_region_cd_group",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_3",
                "source_disaster_event_key": "event_3",
                "reference_month": "2020-06",
                "event_year": 2020,
                "event_month_number": 6,
                "province_key": "AB",
                "disaster_domain": "severe_storm_or_climate",
                "location_text": "Calgary, Alberta",
                "location_tier": "city",
                "mapped_geo_level": "CSD",
                "mapped_geo_codes_json": json.dumps(["4806016"]),
                "mapping_confidence": "high",
                "mapping_method": "manual_exact_bridge_match",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_4",
                "source_disaster_event_key": "event_4",
                "reference_month": "1906-06",
                "event_year": 1906,
                "event_month_number": 6,
                "province_key": "AB",
                "disaster_domain": "flood",
                "location_text": "Southern Alberta",
                "location_tier": "region",
                "mapped_geo_level": "CD_GROUP",
                "mapped_geo_codes_json": json.dumps(["4801", "4802"]),
                "mapping_confidence": "medium",
                "mapping_method": "manual_regional_cd_group",
                "is_backtest_window": False,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": False,
            },
        ]
    )


def _cd_spatial_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "census_division_key": "4816",
                "census_division_name": "Wood Buffalo",
                "census_division_type": "CDR",
                "province_key": "AB",
            },
            {
                "census_division_key": "4806",
                "census_division_name": "Calgary",
                "census_division_type": "CDR",
                "province_key": "AB",
            },
            {
                "census_division_key": "5905",
                "census_division_name": "Kootenay Boundary",
                "census_division_type": "RD",
                "province_key": "BC",
            },
            {
                "census_division_key": "5935",
                "census_division_name": "Central Okanagan",
                "census_division_type": "RD",
                "province_key": "BC",
            },
            {
                "census_division_key": "4801",
                "census_division_name": "Medicine Hat",
                "census_division_type": "CDR",
                "province_key": "AB",
            },
            {
                "census_division_key": "4802",
                "census_division_name": "Lethbridge",
                "census_division_type": "CDR",
                "province_key": "AB",
            },
        ]
    )


def test_build_gold_disaster_event_cd_scope_reference_expands_cd_group_and_csd() -> None:
    result, summary = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=_event_reference_frame(),
        cd_spatial_reference=_cd_spatial_reference_frame(),
    )

    assert summary["source_grid_backtest_event_count"] == 3
    assert summary["unique_event_count"] == 3
    assert summary["row_count"] == 4

    assert set(result["resolved_census_division_key"]) == {
        "4816",
        "4806",
        "5905",
        "5935",
    }

    assert result["event_cd_scope_key"].is_unique
    assert (
        result[["disaster_event_reference_key", "resolved_census_division_key"]].duplicated().sum()
        == 0
    )


def test_build_gold_disaster_event_cd_scope_reference_filters_non_backtest_rows() -> None:
    result, _ = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=_event_reference_frame(),
        cd_spatial_reference=_cd_spatial_reference_frame(),
    )

    assert "disaster_event_ref__event_4" not in set(result["disaster_event_reference_key"])
    assert result["is_backtest_window"].all()
    assert result["is_grid_backtest_eligible"].all()


def test_build_gold_disaster_event_cd_scope_reference_marks_csd_parent_cd() -> None:
    result, _ = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=_event_reference_frame(),
        cd_spatial_reference=_cd_spatial_reference_frame(),
    )

    csd_rows = result[result["source_mapped_geo_level"].eq("CSD")]

    assert len(csd_rows) == 2
    assert set(csd_rows["resolution_method"]) == {"csd_parent_cd"}
    assert csd_rows["is_csd_to_cd_approximation"].all()
    assert set(csd_rows["resolved_census_division_key"]) == {"4816", "4806"}


def test_build_gold_disaster_event_cd_scope_reference_rejects_missing_cd() -> None:
    cd_reference = _cd_spatial_reference_frame()
    cd_reference = cd_reference[cd_reference["census_division_key"].ne("4816")].copy()

    with pytest.raises(
        GoldDisasterEventCDScopeError,
        match="CSD parent CD not found",
    ):
        build_gold_disaster_event_cd_scope_reference(
            disaster_event_reference=_event_reference_frame(),
            cd_spatial_reference=cd_reference,
        )


def test_validate_gold_disaster_event_cd_scope_reference_passes_for_valid_table() -> None:
    event_reference = _event_reference_frame()
    cd_reference = _cd_spatial_reference_frame()

    result, _ = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_reference,
    )

    report = validate_gold_disaster_event_cd_scope_reference(
        event_cd_scope=result,
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_reference,
    )

    assert report["validation_status"] == "passed"
    assert report["source_grid_backtest_event_count"] == 3
    assert report["unique_event_count"] == 3
    assert report["row_count"] == 4


def test_validate_gold_disaster_event_cd_scope_reference_rejects_duplicate_event_cd() -> None:
    event_reference = _event_reference_frame()
    cd_reference = _cd_spatial_reference_frame()

    result, _ = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_reference,
    )

    duplicate = result.iloc[[0]].copy()
    result = pd.concat([result, duplicate], ignore_index=True)

    with pytest.raises(
        GoldDisasterEventCDScopeValidationError,
        match="duplicates|duplicate",
    ):
        validate_gold_disaster_event_cd_scope_reference(
            event_cd_scope=result,
            disaster_event_reference=event_reference,
            cd_spatial_reference=cd_reference,
        )


def test_validate_gold_disaster_event_cd_scope_reference_rejects_invalid_resolution() -> None:
    event_reference = _event_reference_frame()
    cd_reference = _cd_spatial_reference_frame()

    result, _ = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_reference,
    )

    csd_index = result[result["source_mapped_geo_level"].eq("CSD")].index[0]
    result.loc[csd_index, "resolution_method"] = "direct_cd"

    with pytest.raises(
        GoldDisasterEventCDScopeValidationError,
        match="CSD rows must use csd_parent_cd",
    ):
        validate_gold_disaster_event_cd_scope_reference(
            event_cd_scope=result,
            disaster_event_reference=event_reference,
            cd_spatial_reference=cd_reference,
        )
