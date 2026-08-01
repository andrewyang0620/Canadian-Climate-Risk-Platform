from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd


TABLE_NAME = "gold_grid_month_disaster_event_label"

TARGET_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
LABEL_START_MONTH = pd.Period("2016-01", freq="M")
MAXIMUM_LABEL_MONTH = pd.Period("2025-12", freq="M")


class GoldGridMonthDisasterEventLabelError(Exception):
    """Raised when Gold grid-month disaster event label construction fails."""


def build_gold_grid_month_disaster_event_label(
    *,
    event_grid_scope: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _validate_inputs(
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_event_reference,
        grid_cell=grid_cell,
    )

    grids = grid_cell[grid_cell["grid_system"].isin(TARGET_GRID_SYSTEMS)][
        [
            "grid_cell_key",
            "grid_system",
            "province_key",
        ]
    ].copy()

    grids["grid_cell_key"] = grids["grid_cell_key"].astype(str)
    grids["grid_system"] = grids["grid_system"].astype(str)
    grids["province_key"] = grids["province_key"].astype(str)

    if grids["grid_cell_key"].duplicated().any():
        raise GoldGridMonthDisasterEventLabelError("Target grid_cell_key contains duplicates.")

    source_periods = _parse_reference_month(disaster_event_reference["reference_month"])

    if source_periods.isna().any():
        raise GoldGridMonthDisasterEventLabelError(
            "Disaster event reference contains invalid reference_month values."
        )

    source_end_month = source_periods.max()
    label_end_month = min(source_end_month, MAXIMUM_LABEL_MONTH)

    if label_end_month < LABEL_START_MONTH:
        raise GoldGridMonthDisasterEventLabelError(
            "Disaster source coverage ends before the label start month."
        )

    label_months = pd.period_range(
        LABEL_START_MONTH,
        label_end_month,
        freq="M",
    )

    skeleton = pd.MultiIndex.from_product(
        [
            grids["grid_cell_key"],
            label_months.astype(str),
        ],
        names=[
            "grid_cell_key",
            "reference_month",
        ],
    ).to_frame(index=False)

    skeleton = skeleton.merge(
        grids,
        on="grid_cell_key",
        how="left",
        validate="many_to_one",
    )

    events = event_grid_scope.copy()

    events["grid_cell_key"] = events["grid_cell_key"].astype(str)
    events["reference_month"] = events["reference_month"].astype(str)
    events["disaster_event_reference_key"] = events["disaster_event_reference_key"].astype(str)

    duplicate_event_grid = events.duplicated(
        [
            "disaster_event_reference_key",
            "grid_cell_key",
        ]
    )

    if duplicate_event_grid.any():
        raise GoldGridMonthDisasterEventLabelError(
            "Event-grid input contains duplicate event-grid rows."
        )

    known_grid_keys = set(grids["grid_cell_key"])
    event_grid_keys = set(events["grid_cell_key"])

    unknown_grid_keys = sorted(event_grid_keys - known_grid_keys)

    if unknown_grid_keys:
        raise GoldGridMonthDisasterEventLabelError(
            "Event-grid input contains unknown grid keys: " f"{unknown_grid_keys[:20]}"
        )

    events["wildfire_indicator"] = events["disaster_domain"].eq("wildfire").astype("int64")
    events["flood_indicator"] = events["disaster_domain"].eq("flood").astype("int64")
    events["storm_or_climate_indicator"] = (
        events["disaster_domain"].eq("severe_storm_or_climate").astype("int64")
    )
    events["climate_extreme_indicator"] = (
        events["disaster_domain"].eq("climate_extreme").astype("int64")
    )

    events["direct_cd_resolution_indicator"] = events["resolution_methods_json"].map(
        lambda value: int(_json_list_contains(value, "direct_cd"))
    )

    events["csd_parent_cd_resolution_indicator"] = events["resolution_methods_json"].map(
        lambda value: int(_json_list_contains(value, "csd_parent_cd"))
    )

    events["cd_scope_indicator"] = events["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CD"))
    )
    events["cd_group_scope_indicator"] = events["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CD_GROUP"))
    )
    events["csd_scope_indicator"] = events["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CSD"))
    )

    events["approximate_indicator"] = (
        events["is_csd_to_cd_approximation"].fillna(False).astype(bool).astype("int64")
    )

    events["low_overlap_indicator"] = (
        pd.to_numeric(
            events["affected_grid_coverage_ratio"],
            errors="coerce",
        )
        .le(0.05)
        .astype("int64")
    )

    positive_labels = events.groupby(
        [
            "grid_cell_key",
            "reference_month",
        ],
        as_index=False,
        sort=False,
    ).agg(
        disaster_event_count=(
            "disaster_event_reference_key",
            "nunique",
        ),
        wildfire_event_count=(
            "wildfire_indicator",
            "sum",
        ),
        flood_event_count=(
            "flood_indicator",
            "sum",
        ),
        storm_or_climate_event_count=(
            "storm_or_climate_indicator",
            "sum",
        ),
        climate_extreme_event_count=(
            "climate_extreme_indicator",
            "sum",
        ),
        direct_cd_resolution_event_count=(
            "direct_cd_resolution_indicator",
            "sum",
        ),
        csd_parent_cd_event_count=(
            "csd_parent_cd_resolution_indicator",
            "sum",
        ),
        cd_scope_event_count=(
            "cd_scope_indicator",
            "sum",
        ),
        cd_group_scope_event_count=(
            "cd_group_scope_indicator",
            "sum",
        ),
        csd_scope_event_count=(
            "csd_scope_indicator",
            "sum",
        ),
        approximate_event_count=(
            "approximate_indicator",
            "sum",
        ),
        low_overlap_event_count=(
            "low_overlap_indicator",
            "sum",
        ),
        disaster_event_types=(
            "disaster_domain",
            _join_unique,
        ),
        disaster_event_reference_keys_json=(
            "disaster_event_reference_key",
            _json_unique,
        ),
        minimum_event_grid_coverage_ratio=(
            "affected_grid_coverage_ratio",
            "min",
        ),
        mean_event_grid_coverage_ratio=(
            "affected_grid_coverage_ratio",
            "mean",
        ),
        maximum_event_grid_coverage_ratio=(
            "affected_grid_coverage_ratio",
            "max",
        ),
    )

    result = skeleton.merge(
        positive_labels,
        on=[
            "grid_cell_key",
            "reference_month",
        ],
        how="left",
        validate="one_to_one",
    )

    count_columns = [
        "disaster_event_count",
        "wildfire_event_count",
        "flood_event_count",
        "storm_or_climate_event_count",
        "climate_extreme_event_count",
        "direct_cd_resolution_event_count",
        "csd_parent_cd_event_count",
        "cd_scope_event_count",
        "cd_group_scope_event_count",
        "csd_scope_event_count",
        "approximate_event_count",
        "low_overlap_event_count",
    ]

    for column in count_columns:
        result[column] = (
            pd.to_numeric(
                result[column],
                errors="coerce",
            )
            .fillna(0)
            .astype("int64")
        )

    result["disaster_event_occurred"] = result["disaster_event_count"] > 0
    result["has_csd_parent_cd_approximation"] = result["approximate_event_count"] > 0
    result["has_low_overlap_event"] = result["low_overlap_event_count"] > 0

    result["disaster_event_types"] = result["disaster_event_types"].fillna("").astype("string")
    result["disaster_event_reference_keys_json"] = (
        result["disaster_event_reference_keys_json"].fillna("[]").astype("string")
    )

    parsed_month = _parse_reference_month(result["reference_month"])

    result["event_year"] = parsed_month.dt.year.astype("int64")
    result["event_month_number"] = parsed_month.dt.month.astype("int64")
    result["label_is_observed"] = True

    result["grid_month_disaster_label_key"] = (
        "disaster_label__" + result["grid_cell_key"] + "__" + result["reference_month"]
    )

    result = (
        result[
            [
                "grid_month_disaster_label_key",
                "grid_cell_key",
                "reference_month",
                "event_year",
                "event_month_number",
                "province_key",
                "grid_system",
                "label_is_observed",
                "disaster_event_occurred",
                "disaster_event_count",
                "wildfire_event_count",
                "flood_event_count",
                "storm_or_climate_event_count",
                "climate_extreme_event_count",
                "disaster_event_types",
                "disaster_event_reference_keys_json",
                "direct_cd_resolution_event_count",
                "csd_parent_cd_event_count",
                "cd_scope_event_count",
                "cd_group_scope_event_count",
                "csd_scope_event_count",
                "approximate_event_count",
                "low_overlap_event_count",
                "has_csd_parent_cd_approximation",
                "has_low_overlap_event",
                "minimum_event_grid_coverage_ratio",
                "mean_event_grid_coverage_ratio",
                "maximum_event_grid_coverage_ratio",
            ]
        ]
        .sort_values(
            [
                "reference_month",
                "grid_cell_key",
            ]
        )
        .reset_index(drop=True)
    )

    _validate_result(
        result=result,
        event_grid_scope=events,
        expected_grid_count=len(grids),
        expected_month_count=len(label_months),
    )

    summary = _build_summary(
        result=result,
        event_grid_scope=events,
        source_end_month=source_end_month,
        label_end_month=label_end_month,
        grid_count=len(grids),
        month_count=len(label_months),
    )

    return result, summary


def _validate_inputs(
    *,
    event_grid_scope: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> None:
    required_event_grid_columns = {
        "disaster_event_reference_key",
        "reference_month",
        "disaster_domain",
        "grid_cell_key",
        "resolution_methods_json",
        "source_mapped_geo_levels_json",
        "affected_grid_coverage_ratio",
        "is_csd_to_cd_approximation",
    }

    required_reference_columns = {
        "reference_month",
    }

    required_grid_columns = {
        "grid_cell_key",
        "grid_system",
        "province_key",
    }

    missing_event_grid = required_event_grid_columns - set(event_grid_scope.columns)
    missing_reference = required_reference_columns - set(disaster_event_reference.columns)
    missing_grid = required_grid_columns - set(grid_cell.columns)

    if missing_event_grid:
        raise GoldGridMonthDisasterEventLabelError(
            "Missing event-grid columns: " f"{sorted(missing_event_grid)}"
        )

    if missing_reference:
        raise GoldGridMonthDisasterEventLabelError(
            "Missing disaster reference columns: " f"{sorted(missing_reference)}"
        )

    if missing_grid:
        raise GoldGridMonthDisasterEventLabelError(
            "Missing grid columns: " f"{sorted(missing_grid)}"
        )

    if event_grid_scope.empty:
        raise GoldGridMonthDisasterEventLabelError("Event-grid scope input is empty.")

    if disaster_event_reference.empty:
        raise GoldGridMonthDisasterEventLabelError("Disaster event reference input is empty.")

    if grid_cell.empty:
        raise GoldGridMonthDisasterEventLabelError("Grid-cell input is empty.")


def _validate_result(
    *,
    result: pd.DataFrame,
    event_grid_scope: pd.DataFrame,
    expected_grid_count: int,
    expected_month_count: int,
) -> None:
    expected_row_count = expected_grid_count * expected_month_count

    if len(result) != expected_row_count:
        raise GoldGridMonthDisasterEventLabelError(
            "Grid-month skeleton row count mismatch. "
            f"expected={expected_row_count}, actual={len(result)}"
        )

    if result["grid_month_disaster_label_key"].isna().any():
        raise GoldGridMonthDisasterEventLabelError("grid_month_disaster_label_key contains nulls.")

    if result["grid_month_disaster_label_key"].duplicated().any():
        raise GoldGridMonthDisasterEventLabelError(
            "grid_month_disaster_label_key contains duplicates."
        )

    duplicate_grain = result.duplicated(
        [
            "grid_cell_key",
            "reference_month",
        ]
    )

    if duplicate_grain.any():
        raise GoldGridMonthDisasterEventLabelError("Output contains duplicate grid-month rows.")

    if not result["label_is_observed"].astype(bool).all():
        raise GoldGridMonthDisasterEventLabelError(
            "label_is_observed must be true for all output rows."
        )

    event_assignment_count = int(result["disaster_event_count"].sum())

    if event_assignment_count != len(event_grid_scope):
        raise GoldGridMonthDisasterEventLabelError(
            "Disaster event assignment count does not match "
            "event-grid source rows. "
            f"expected={len(event_grid_scope)}, "
            f"actual={event_assignment_count}"
        )

    domain_assignment_count = int(
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
    )

    if domain_assignment_count != len(event_grid_scope):
        raise GoldGridMonthDisasterEventLabelError(
            "Domain event counts do not match event-grid source rows."
        )

    occurred_expected = result["disaster_event_count"].gt(0)

    if not result["disaster_event_occurred"].eq(occurred_expected).all():
        raise GoldGridMonthDisasterEventLabelError(
            "disaster_event_occurred does not match event count."
        )


def _build_summary(
    *,
    result: pd.DataFrame,
    event_grid_scope: pd.DataFrame,
    source_end_month: pd.Period,
    label_end_month: pd.Period,
    grid_count: int,
    month_count: int,
) -> dict[str, Any]:
    positive = result[result["disaster_event_occurred"]]

    return {
        "table_name": TABLE_NAME,
        "row_count": int(len(result)),
        "grid_count": int(grid_count),
        "month_count": int(month_count),
        "minimum_reference_month": str(result["reference_month"].min()),
        "maximum_reference_month": str(result["reference_month"].max()),
        "source_maximum_reference_month": str(source_end_month),
        "label_maximum_reference_month": str(label_end_month),
        "positive_label_row_count": int(len(positive)),
        "negative_label_row_count": int((~result["disaster_event_occurred"]).sum()),
        "unique_positive_grid_count": int(positive["grid_cell_key"].nunique()),
        "source_event_grid_row_count": int(len(event_grid_scope)),
        "total_disaster_event_assignments": int(result["disaster_event_count"].sum()),
        "total_wildfire_event_assignments": int(result["wildfire_event_count"].sum()),
        "total_flood_event_assignments": int(result["flood_event_count"].sum()),
        "total_storm_or_climate_event_assignments": int(
            result["storm_or_climate_event_count"].sum()
        ),
        "total_climate_extreme_event_assignments": int(result["climate_extreme_event_count"].sum()),
        "positive_rows_with_csd_approximation": int(
            positive["has_csd_parent_cd_approximation"].sum()
        ),
        "positive_rows_with_low_overlap_event": int(positive["has_low_overlap_event"].sum()),
        "province_counts": _value_counts(result["province_key"]),
        "grid_system_counts": _value_counts(result["grid_system"]),
    }


def _parse_reference_month(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(
        series.astype("string") + "-01",
        errors="coerce",
    )
    return parsed.dt.to_period("M")


def _json_list_contains(value: Any, expected: str) -> bool:
    try:
        parsed = json.loads(str(value))
    except Exception as exc:
        raise GoldGridMonthDisasterEventLabelError(f"Invalid JSON list value: {value}") from exc

    if not isinstance(parsed, list):
        raise GoldGridMonthDisasterEventLabelError(f"Expected JSON list value: {value}")

    return expected in {str(item) for item in parsed}


def _join_unique(values: Iterable[Any]) -> str:
    normalized = sorted({str(value) for value in values if not pd.isna(value)})
    return ",".join(normalized)


def _json_unique(values: Iterable[Any]) -> str:
    normalized = sorted({str(value) for value in values if not pd.isna(value)})
    return json.dumps(normalized)


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }
