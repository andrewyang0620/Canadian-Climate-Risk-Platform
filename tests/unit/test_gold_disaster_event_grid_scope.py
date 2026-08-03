from __future__ import annotations

import json

import pandas as pd
import pytest
from shapely.geometry import box

from src.gold.disaster.event_grid_scope import (
    GoldDisasterEventGridScopeError,
    build_gold_disaster_event_grid_scope,
)
from src.gold.disaster.event_grid_scope_validation import (
    GoldDisasterEventGridScopeValidationError,
    validate_gold_disaster_event_grid_scope,
)


def _event_cd_scope_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_cd_scope_key": "event_cd_scope__event_1__4801",
                "disaster_event_reference_key": "disaster_event_ref__event_1",
                "source_disaster_event_key": "event_1",
                "reference_month": "2016-05",
                "event_year": 2016,
                "event_month_number": 5,
                "province_key": "AB",
                "disaster_domain": "wildfire",
                "location_text": "Test location 1",
                "location_tier": "regional_district",
                "source_mapped_geo_level": "CD",
                "source_mapped_geo_codes_json": json.dumps(["4801"]),
                "resolved_census_division_key": "4801",
                "census_division_name": "Test CD 4801",
                "census_division_type": "CDR",
                "census_division_province_key": "AB",
                "resolution_method": "direct_cd",
                "is_csd_to_cd_approximation": False,
                "mapping_confidence": "high",
                "mapping_method": "manual_exact_cd",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "event_cd_scope_key": "event_cd_scope__event_2__4801",
                "disaster_event_reference_key": "disaster_event_ref__event_2",
                "source_disaster_event_key": "event_2",
                "reference_month": "2021-11",
                "event_year": 2021,
                "event_month_number": 11,
                "province_key": "AB",
                "disaster_domain": "flood",
                "location_text": "Test regional event",
                "location_tier": "region",
                "source_mapped_geo_level": "CD_GROUP",
                "source_mapped_geo_codes_json": json.dumps(["4801", "4802"]),
                "resolved_census_division_key": "4801",
                "census_division_name": "Test CD 4801",
                "census_division_type": "CDR",
                "census_division_province_key": "AB",
                "resolution_method": "direct_cd",
                "is_csd_to_cd_approximation": False,
                "mapping_confidence": "medium",
                "mapping_method": "manual_cd_group",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "event_cd_scope_key": "event_cd_scope__event_2__4802",
                "disaster_event_reference_key": "disaster_event_ref__event_2",
                "source_disaster_event_key": "event_2",
                "reference_month": "2021-11",
                "event_year": 2021,
                "event_month_number": 11,
                "province_key": "AB",
                "disaster_domain": "flood",
                "location_text": "Test regional event",
                "location_tier": "region",
                "source_mapped_geo_level": "CD_GROUP",
                "source_mapped_geo_codes_json": json.dumps(["4801", "4802"]),
                "resolved_census_division_key": "4802",
                "census_division_name": "Test CD 4802",
                "census_division_type": "CDR",
                "census_division_province_key": "AB",
                "resolution_method": "direct_cd",
                "is_csd_to_cd_approximation": False,
                "mapping_confidence": "medium",
                "mapping_method": "manual_cd_group",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
            {
                "event_cd_scope_key": "event_cd_scope__event_3__4802",
                "disaster_event_reference_key": "disaster_event_ref__event_3",
                "source_disaster_event_key": "event_3",
                "reference_month": "2020-06",
                "event_year": 2020,
                "event_month_number": 6,
                "province_key": "AB",
                "disaster_domain": "severe_storm_or_climate",
                "location_text": "Test city",
                "location_tier": "city",
                "source_mapped_geo_level": "CSD",
                "source_mapped_geo_codes_json": json.dumps(["4802001"]),
                "resolved_census_division_key": "4802",
                "census_division_name": "Test CD 4802",
                "census_division_type": "CDR",
                "census_division_province_key": "AB",
                "resolution_method": "csd_parent_cd",
                "is_csd_to_cd_approximation": True,
                "mapping_confidence": "high",
                "mapping_method": "manual_csd_parent_cd",
                "is_backtest_window": True,
                "is_ab_bc_scope": True,
                "is_domain_relevant": True,
                "is_grid_backtest_eligible": True,
            },
        ]
    )


def _cd_spatial_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "census_division_key": "4801",
                "census_division_name": "Test CD 4801",
                "census_division_type": "CDR",
                "province_key": "AB",
                "geometry_crs_epsg": 3347,
                "geometry_wkt": box(0.0, 0.0, 10.0, 10.0).wkt,
            },
            {
                "census_division_key": "4802",
                "census_division_name": "Test CD 4802",
                "census_division_type": "CDR",
                "province_key": "AB",
                "geometry_crs_epsg": 3347,
                "geometry_wkt": box(10.0, 0.0, 20.0, 10.0).wkt,
            },
        ]
    )


def _grid_row(
    *,
    grid_cell_key: str,
    minimum_x: float,
    maximum_x: float,
    minimum_y: float = 0.0,
    maximum_y: float = 10.0,
    crs_epsg: int = 3347,
) -> dict[str, object]:
    geometry = box(
        minimum_x,
        minimum_y,
        maximum_x,
        maximum_y,
    )

    return {
        "grid_cell_key": grid_cell_key,
        "grid_system": "ab_10km",
        "province_key": "AB",
        "cell_min_x": minimum_x,
        "cell_min_y": minimum_y,
        "cell_max_x": maximum_x,
        "cell_max_y": maximum_y,
        "analysis_area_sq_km": geometry.area / 1_000_000.0,
        "analysis_geometry_wkt": geometry.wkt,
        "crs_epsg": crs_epsg,
    }


def _grid_cell_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # Fully inside CD 4801.
            _grid_row(
                grid_cell_key="ab_10km_left",
                minimum_x=0.0,
                maximum_x=8.0,
            ),
            # Crosses the boundary between CD 4801 and CD 4802.
            _grid_row(
                grid_cell_key="ab_10km_cross",
                minimum_x=8.0,
                maximum_x=12.0,
            ),
            # Fully inside CD 4802.
            _grid_row(
                grid_cell_key="ab_10km_right",
                minimum_x=12.0,
                maximum_x=20.0,
            ),
            # Only touches CD 4802 at x=20 and has zero overlap area.
            _grid_row(
                grid_cell_key="ab_10km_touch_only",
                minimum_x=20.0,
                maximum_x=30.0,
            ),
        ]
    )


def _build_valid_result() -> tuple[
    pd.DataFrame,
    dict[str, object],
]:
    return build_gold_disaster_event_grid_scope(
        event_cd_scope=_event_cd_scope_frame(),
        cd_spatial_reference=_cd_spatial_reference_frame(),
        grid_cell=_grid_cell_frame(),
    )


def test_build_event_grid_scope_expands_intersections_and_deduplicates() -> None:
    result, summary = _build_valid_result()

    assert summary["source_event_cd_scope_row_count"] == 4
    assert summary["source_grid_backtest_event_count"] == 3
    assert summary["unique_event_count"] == 3
    assert summary["unique_grid_cell_count"] == 3
    assert summary["source_census_division_count"] == 2
    assert summary["grid_cd_bridge_row_count"] == 4
    assert summary["row_count"] == 7

    assert result["event_grid_scope_key"].is_unique

    assert (
        result[
            [
                "disaster_event_reference_key",
                "grid_cell_key",
            ]
        ]
        .duplicated()
        .sum()
        == 0
    )

    event_2 = result[result["disaster_event_reference_key"].eq("disaster_event_ref__event_2")]

    assert set(event_2["grid_cell_key"]) == {
        "ab_10km_left",
        "ab_10km_cross",
        "ab_10km_right",
    }

    cross_row = event_2[event_2["grid_cell_key"].eq("ab_10km_cross")].iloc[0]

    assert cross_row["matched_census_division_count"] == 2
    assert json.loads(cross_row["matched_census_division_keys_json"]) == ["4801", "4802"]
    assert len(json.loads(cross_row["source_event_cd_scope_keys_json"])) == 2
    assert cross_row["affected_grid_coverage_ratio"] == pytest.approx(1.0)


def test_build_event_grid_scope_excludes_boundary_only_touch() -> None:
    result, _ = _build_valid_result()

    assert "ab_10km_touch_only" not in set(result["grid_cell_key"])


def test_build_event_grid_scope_preserves_csd_parent_cd_lineage() -> None:
    result, summary = _build_valid_result()

    event_3 = result[result["disaster_event_reference_key"].eq("disaster_event_ref__event_3")]

    assert len(event_3) == 2
    assert set(event_3["grid_cell_key"]) == {
        "ab_10km_cross",
        "ab_10km_right",
    }
    assert event_3["is_csd_to_cd_approximation"].all()

    for value in event_3["resolution_methods_json"]:
        assert json.loads(value) == ["csd_parent_cd"]

    for value in event_3["source_mapped_geo_levels_json"]:
        assert json.loads(value) == ["CSD"]

    assert summary["csd_approximation_event_grid_row_count"] == 2


def test_build_event_grid_scope_rejects_missing_cd_reference() -> None:
    cd_reference = _cd_spatial_reference_frame()
    cd_reference = cd_reference[cd_reference["census_division_key"].ne("4802")].copy()

    with pytest.raises(
        GoldDisasterEventGridScopeError,
        match="keys missing from CD spatial reference",
    ):
        build_gold_disaster_event_grid_scope(
            event_cd_scope=_event_cd_scope_frame(),
            cd_spatial_reference=cd_reference,
            grid_cell=_grid_cell_frame(),
        )


def test_build_event_grid_scope_rejects_wrong_grid_crs() -> None:
    grid_cell = _grid_cell_frame()
    grid_cell["crs_epsg"] = 4326

    with pytest.raises(
        GoldDisasterEventGridScopeError,
        match="Expected grid CRS 3347",
    ):
        build_gold_disaster_event_grid_scope(
            event_cd_scope=_event_cd_scope_frame(),
            cd_spatial_reference=_cd_spatial_reference_frame(),
            grid_cell=grid_cell,
        )


def test_build_event_grid_scope_rejects_cd_without_grid_intersection() -> None:
    cd_reference = _cd_spatial_reference_frame()
    cd_reference.loc[
        cd_reference["census_division_key"].eq("4802"),
        "geometry_wkt",
    ] = box(100.0, 100.0, 110.0, 110.0).wkt

    with pytest.raises(
        GoldDisasterEventGridScopeError,
        match="produced no grid intersections",
    ):
        build_gold_disaster_event_grid_scope(
            event_cd_scope=_event_cd_scope_frame(),
            cd_spatial_reference=cd_reference,
            grid_cell=_grid_cell_frame(),
        )


def test_validate_event_grid_scope_passes_for_valid_output() -> None:
    event_cd_scope = _event_cd_scope_frame()
    cd_reference = _cd_spatial_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_disaster_event_grid_scope(
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_reference,
        grid_cell=grid_cell,
    )

    report = validate_gold_disaster_event_grid_scope(
        event_grid_scope=result,
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_reference,
        grid_cell=grid_cell,
    )

    assert report["validation_status"] == "passed"
    assert report["check_count"] == 14
    assert report["row_count"] == 7
    assert report["unique_event_count"] == 3
    assert report["unique_grid_cell_count"] == 3
    assert report["unique_census_division_count"] == 2


def test_validate_event_grid_scope_rejects_duplicate_event_grid() -> None:
    event_cd_scope = _event_cd_scope_frame()
    cd_reference = _cd_spatial_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_disaster_event_grid_scope(
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_reference,
        grid_cell=grid_cell,
    )

    duplicate = result.iloc[[0]].copy()
    result = pd.concat(
        [result, duplicate],
        ignore_index=True,
    )

    with pytest.raises(
        GoldDisasterEventGridScopeValidationError,
        match="duplicates|Duplicate",
    ):
        validate_gold_disaster_event_grid_scope(
            event_grid_scope=result,
            event_cd_scope=event_cd_scope,
            cd_spatial_reference=cd_reference,
            grid_cell=grid_cell,
        )


def test_validate_event_grid_scope_rejects_unknown_grid() -> None:
    event_cd_scope = _event_cd_scope_frame()
    cd_reference = _cd_spatial_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_disaster_event_grid_scope(
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_reference,
        grid_cell=grid_cell,
    )

    result.loc[0, "grid_cell_key"] = "unknown_grid_cell"

    with pytest.raises(
        GoldDisasterEventGridScopeValidationError,
        match="Unknown grid_cell_key",
    ):
        validate_gold_disaster_event_grid_scope(
            event_grid_scope=result,
            event_cd_scope=event_cd_scope,
            cd_spatial_reference=cd_reference,
            grid_cell=grid_cell,
        )
