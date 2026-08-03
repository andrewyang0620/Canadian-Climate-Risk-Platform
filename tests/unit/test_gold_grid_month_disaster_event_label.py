from __future__ import annotations

import json

import pandas as pd
import pytest

from src.gold.disaster.grid_month_label import (
    GoldGridMonthDisasterEventLabelError,
    build_gold_grid_month_disaster_event_label,
)
from src.gold.disaster.grid_month_label_validation import (
    GoldGridMonthDisasterEventLabelValidationError,
    validate_gold_grid_month_disaster_event_label,
)


def _grid_cell_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test_1",
                "grid_system": "ab_10km",
                "province_key": "AB",
            },
            {
                "grid_cell_key": "bc_10km_test_1",
                "grid_system": "bc_10km",
                "province_key": "BC",
            },
            {
                "grid_cell_key": "ab_other_grid",
                "grid_cell_key": "ab_other_grid",
                "grid_system": "ab_5km",
                "province_key": "AB",
            },
        ]
    )


def _disaster_event_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "disaster_event_reference_key": "disaster_event_ref__event_1",
                "reference_month": "2016-01",
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_2",
                "reference_month": "2016-01",
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_3",
                "reference_month": "2016-03",
            },
        ]
    )


def _event_grid_scope_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "disaster_event_reference_key": "disaster_event_ref__event_1",
                "reference_month": "2016-01",
                "disaster_domain": "wildfire",
                "grid_cell_key": "ab_10km_test_1",
                "resolution_methods_json": json.dumps(["direct_cd"]),
                "source_mapped_geo_levels_json": json.dumps(["CD"]),
                "affected_grid_coverage_ratio": 1.0,
                "is_csd_to_cd_approximation": False,
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_2",
                "reference_month": "2016-01",
                "disaster_domain": "flood",
                "grid_cell_key": "ab_10km_test_1",
                "resolution_methods_json": json.dumps(["csd_parent_cd"]),
                "source_mapped_geo_levels_json": json.dumps(["CSD"]),
                "affected_grid_coverage_ratio": 0.04,
                "is_csd_to_cd_approximation": True,
            },
            {
                "disaster_event_reference_key": "disaster_event_ref__event_3",
                "reference_month": "2016-03",
                "disaster_domain": "severe_storm_or_climate",
                "grid_cell_key": "bc_10km_test_1",
                "resolution_methods_json": json.dumps(["direct_cd"]),
                "source_mapped_geo_levels_json": json.dumps(["CD_GROUP"]),
                "affected_grid_coverage_ratio": 0.80,
                "is_csd_to_cd_approximation": False,
            },
        ]
    )


def _build_valid_result() -> tuple[pd.DataFrame, dict[str, object]]:
    return build_gold_grid_month_disaster_event_label(
        event_grid_scope=_event_grid_scope_frame(),
        disaster_event_reference=_disaster_event_reference_frame(),
        grid_cell=_grid_cell_frame(),
    )


def test_build_grid_month_label_creates_complete_observed_skeleton() -> None:
    result, summary = _build_valid_result()

    assert summary["grid_count"] == 2
    assert summary["month_count"] == 3
    assert summary["row_count"] == 6
    assert summary["minimum_reference_month"] == "2016-01"
    assert summary["maximum_reference_month"] == "2016-03"
    assert summary["source_maximum_reference_month"] == "2016-03"
    assert summary["label_maximum_reference_month"] == "2016-03"

    assert len(result) == 6
    assert result["grid_cell_key"].nunique() == 2
    assert result["reference_month"].nunique() == 3
    assert set(result["reference_month"]) == {
        "2016-01",
        "2016-02",
        "2016-03",
    }

    assert result["label_is_observed"].all()
    assert "ab_other_grid" not in set(result["grid_cell_key"])

    rows_per_grid = result.groupby("grid_cell_key").size()
    assert rows_per_grid.eq(3).all()

    rows_per_month = result.groupby("reference_month").size()
    assert rows_per_month.eq(2).all()


def test_build_grid_month_label_aggregates_multiple_events_per_grid_month() -> None:
    result, summary = _build_valid_result()

    row = result[
        result["grid_cell_key"].eq("ab_10km_test_1") & result["reference_month"].eq("2016-01")
    ].iloc[0]

    assert bool(row["disaster_event_occurred"]) is True
    assert row["disaster_event_count"] == 2
    assert row["wildfire_event_count"] == 1
    assert row["flood_event_count"] == 1
    assert row["storm_or_climate_event_count"] == 0
    assert row["climate_extreme_event_count"] == 0

    assert row["direct_cd_resolution_event_count"] == 1
    assert row["csd_parent_cd_event_count"] == 1
    assert row["cd_scope_event_count"] == 1
    assert row["cd_group_scope_event_count"] == 0
    assert row["csd_scope_event_count"] == 1

    assert row["approximate_event_count"] == 1
    assert row["low_overlap_event_count"] == 1
    assert bool(row["has_csd_parent_cd_approximation"]) is True
    assert bool(row["has_low_overlap_event"]) is True

    assert row["disaster_event_types"] == "flood,wildfire"
    assert json.loads(row["disaster_event_reference_keys_json"]) == [
        "disaster_event_ref__event_1",
        "disaster_event_ref__event_2",
    ]

    assert row["minimum_event_grid_coverage_ratio"] == pytest.approx(0.04)
    assert row["mean_event_grid_coverage_ratio"] == pytest.approx(0.52)
    assert row["maximum_event_grid_coverage_ratio"] == pytest.approx(1.0)

    assert summary["positive_label_row_count"] == 2
    assert summary["negative_label_row_count"] == 4
    assert summary["source_event_grid_row_count"] == 3
    assert summary["total_disaster_event_assignments"] == 3


def test_build_grid_month_label_preserves_domain_counts() -> None:
    result, summary = _build_valid_result()

    storm_row = result[
        result["grid_cell_key"].eq("bc_10km_test_1") & result["reference_month"].eq("2016-03")
    ].iloc[0]

    assert bool(storm_row["disaster_event_occurred"]) is True
    assert storm_row["disaster_event_count"] == 1
    assert storm_row["storm_or_climate_event_count"] == 1
    assert storm_row["cd_group_scope_event_count"] == 1
    assert storm_row["direct_cd_resolution_event_count"] == 1
    assert storm_row["disaster_event_types"] == "severe_storm_or_climate"

    assert summary["total_wildfire_event_assignments"] == 1
    assert summary["total_flood_event_assignments"] == 1
    assert summary["total_storm_or_climate_event_assignments"] == 1
    assert summary["total_climate_extreme_event_assignments"] == 0

    assert (
        result[
            [
                "wildfire_event_count",
                "flood_event_count",
                "storm_or_climate_event_count",
                "climate_extreme_event_count",
            ]
        ]
        .sum()
        .sum()
        == 3
    )


def test_build_grid_month_label_sets_zero_label_semantics() -> None:
    result, _ = _build_valid_result()

    row = result[
        result["grid_cell_key"].eq("bc_10km_test_1") & result["reference_month"].eq("2016-02")
    ].iloc[0]

    assert bool(row["label_is_observed"]) is True
    assert bool(row["disaster_event_occurred"]) is False
    assert row["disaster_event_count"] == 0
    assert row["wildfire_event_count"] == 0
    assert row["flood_event_count"] == 0
    assert row["storm_or_climate_event_count"] == 0
    assert row["climate_extreme_event_count"] == 0

    assert row["disaster_event_types"] == ""
    assert row["disaster_event_reference_keys_json"] == "[]"

    assert pd.isna(row["minimum_event_grid_coverage_ratio"])
    assert pd.isna(row["mean_event_grid_coverage_ratio"])
    assert pd.isna(row["maximum_event_grid_coverage_ratio"])

    assert bool(row["has_csd_parent_cd_approximation"]) is False
    assert bool(row["has_low_overlap_event"]) is False


def test_build_grid_month_label_stops_at_source_coverage_end() -> None:
    result, summary = _build_valid_result()

    assert summary["maximum_reference_month"] == "2016-03"
    assert "2016-04" not in set(result["reference_month"])
    assert "2025-12" not in set(result["reference_month"])


def test_build_grid_month_label_rejects_duplicate_event_grid_rows() -> None:
    event_grid_scope = _event_grid_scope_frame()
    duplicate = event_grid_scope.iloc[[0]].copy()
    event_grid_scope = pd.concat(
        [event_grid_scope, duplicate],
        ignore_index=True,
    )

    with pytest.raises(
        GoldGridMonthDisasterEventLabelError,
        match="duplicate event-grid rows",
    ):
        build_gold_grid_month_disaster_event_label(
            event_grid_scope=event_grid_scope,
            disaster_event_reference=_disaster_event_reference_frame(),
            grid_cell=_grid_cell_frame(),
        )


def test_validate_grid_month_label_passes_for_valid_output() -> None:
    event_grid_scope = _event_grid_scope_frame()
    disaster_reference = _disaster_event_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_grid_month_disaster_event_label(
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_reference,
        grid_cell=grid_cell,
    )

    report = validate_gold_grid_month_disaster_event_label(
        grid_month_label=result,
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_reference,
        grid_cell=grid_cell,
    )

    assert report["validation_status"] == "passed"
    assert report["check_count"] == 14
    assert report["row_count"] == 6
    assert report["grid_count"] == 2
    assert report["month_count"] == 3
    assert report["positive_label_row_count"] == 2
    assert report["negative_label_row_count"] == 4
    assert report["source_event_grid_row_count"] == 3
    assert report["total_disaster_event_assignments"] == 3
    assert report["maximum_events_per_grid_month"] == 2


def test_validate_grid_month_label_rejects_duplicate_grid_month() -> None:
    event_grid_scope = _event_grid_scope_frame()
    disaster_reference = _disaster_event_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_grid_month_disaster_event_label(
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_reference,
        grid_cell=grid_cell,
    )

    duplicate = result.iloc[[0]].copy()
    result = pd.concat(
        [result, duplicate],
        ignore_index=True,
    )

    with pytest.raises(
        GoldGridMonthDisasterEventLabelValidationError,
        match="duplicates|Duplicate|row count mismatch",
    ):
        validate_gold_grid_month_disaster_event_label(
            grid_month_label=result,
            event_grid_scope=event_grid_scope,
            disaster_event_reference=disaster_reference,
            grid_cell=grid_cell,
        )


def test_validate_grid_month_label_rejects_corrupted_event_count() -> None:
    event_grid_scope = _event_grid_scope_frame()
    disaster_reference = _disaster_event_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_grid_month_disaster_event_label(
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_reference,
        grid_cell=grid_cell,
    )

    positive_index = result[result["disaster_event_occurred"]].index[0]

    result.loc[positive_index, "disaster_event_count"] = 99

    with pytest.raises(
        GoldGridMonthDisasterEventLabelValidationError,
        match=("Domain event counts do not sum|" "does not reconcile|" "event assignments"),
    ):
        validate_gold_grid_month_disaster_event_label(
            grid_month_label=result,
            event_grid_scope=event_grid_scope,
            disaster_event_reference=disaster_reference,
            grid_cell=grid_cell,
        )


def test_validate_grid_month_label_rejects_quality_values_on_zero_label() -> None:
    event_grid_scope = _event_grid_scope_frame()
    disaster_reference = _disaster_event_reference_frame()
    grid_cell = _grid_cell_frame()

    result, _ = build_gold_grid_month_disaster_event_label(
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_reference,
        grid_cell=grid_cell,
    )

    negative_index = result[~result["disaster_event_occurred"]].index[0]

    result.loc[
        negative_index,
        "minimum_event_grid_coverage_ratio",
    ] = 0.5

    with pytest.raises(
        GoldGridMonthDisasterEventLabelValidationError,
        match="Zero-label rows must have null coverage-quality metrics",
    ):
        validate_gold_grid_month_disaster_event_label(
            grid_month_label=result,
            event_grid_scope=event_grid_scope,
            disaster_event_reference=disaster_reference,
            grid_cell=grid_cell,
        )
