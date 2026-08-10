from __future__ import annotations

import json
from typing import Any, Iterable

import numpy as np
import pandas as pd

from src.gold.disaster.grid_month_label import (
    LABEL_START_MONTH,
    MAXIMUM_LABEL_MONTH,
    TABLE_NAME,
    TARGET_GRID_SYSTEMS,
)


COUNT_COLUMNS = [
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

DOMAIN_COUNT_COLUMNS = [
    "wildfire_event_count",
    "flood_event_count",
    "storm_or_climate_event_count",
    "climate_extreme_event_count",
]

QUALITY_COLUMNS = [
    "minimum_event_grid_coverage_ratio",
    "mean_event_grid_coverage_ratio",
    "maximum_event_grid_coverage_ratio",
]

REQUIRED_COLUMNS = {
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
}


class GoldGridMonthDisasterEventLabelValidationError(Exception):
    """Raised when grid-month disaster label validation fails."""


def validate_gold_grid_month_disaster_event_label(
    *,
    grid_month_label: pd.DataFrame,
    event_grid_scope: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> dict[str, Any]:
    failures: list[str] = []
    checks: list[str] = []

    _check_required_columns(grid_month_label, failures, checks)

    if failures:
        _raise(failures)

    target_grids = grid_cell[grid_cell["grid_system"].isin(TARGET_GRID_SYSTEMS)].copy()

    source_periods = _parse_reference_month(disaster_event_reference["reference_month"])

    if source_periods.isna().any():
        failures.append("Disaster event reference contains invalid reference_month values.")
        _raise(failures)

    source_end_month = source_periods.max()
    expected_end_month = min(
        source_end_month,
        MAXIMUM_LABEL_MONTH,
    )
    expected_months = pd.period_range(
        LABEL_START_MONTH,
        expected_end_month,
        freq="M",
    )

    _check_nonempty(grid_month_label, failures, checks)
    _check_primary_key(grid_month_label, failures, checks)
    _check_grid_month_grain(grid_month_label, failures, checks)
    _check_complete_skeleton(
        grid_month_label,
        target_grids,
        expected_months,
        failures,
        checks,
    )
    _check_grid_reference_alignment(
        grid_month_label,
        target_grids,
        failures,
        checks,
    )
    _check_time_fields(
        grid_month_label,
        expected_months,
        failures,
        checks,
    )
    _check_count_fields(grid_month_label, failures, checks)
    _check_boolean_semantics(grid_month_label, failures, checks)
    _check_zero_label_semantics(grid_month_label, failures, checks)
    _check_positive_label_semantics(
        grid_month_label,
        failures,
        checks,
    )
    _check_json_lineage(grid_month_label, failures, checks)
    _check_quality_metric_semantics(
        grid_month_label,
        failures,
        checks,
    )
    _check_event_grid_reconciliation(
        grid_month_label,
        event_grid_scope,
        failures,
        checks,
    )

    if failures:
        _raise(failures)

    return _build_report(
        grid_month_label=grid_month_label,
        event_grid_scope=event_grid_scope,
        source_end_month=source_end_month,
        expected_end_month=expected_end_month,
        checks=checks,
    )


def _check_required_columns(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(dataframe.columns))

    if missing:
        failures.append(f"Missing required columns: {missing}")
        return

    checks.append("required_columns_present")


def _check_nonempty(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    if dataframe.empty:
        failures.append("Gold grid-month disaster event label is empty.")
        return

    checks.append("row_count_nonzero")


def _check_primary_key(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    key = dataframe["grid_month_disaster_label_key"].astype("string")

    if key.isna().any():
        failures.append("grid_month_disaster_label_key contains nulls.")

    if key.duplicated().any():
        failures.append("grid_month_disaster_label_key contains duplicates.")

    if not key.str.startswith(
        "disaster_label__",
        na=False,
    ).all():
        failures.append("grid_month_disaster_label_key has an unexpected prefix.")

    checks.append("primary_key_valid")


def _check_grid_month_grain(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    duplicates = dataframe.duplicated(
        [
            "grid_cell_key",
            "reference_month",
        ]
    )

    if duplicates.any():
        failures.append("Duplicate grid_cell_key + reference_month rows found.")

    checks.append("grid_month_grain_valid")


def _check_complete_skeleton(
    dataframe: pd.DataFrame,
    target_grids: pd.DataFrame,
    expected_months: pd.PeriodIndex,
    failures: list[str],
    checks: list[str],
) -> None:
    expected_grid_count = int(target_grids["grid_cell_key"].nunique())
    expected_month_count = int(len(expected_months))
    expected_row_count = expected_grid_count * expected_month_count

    if len(dataframe) != expected_row_count:
        failures.append(
            "Grid-month skeleton row count mismatch. "
            f"expected={expected_row_count}, "
            f"actual={len(dataframe)}"
        )

    observed_grid_count = int(dataframe["grid_cell_key"].nunique())
    observed_month_count = int(dataframe["reference_month"].nunique())

    if observed_grid_count != expected_grid_count:
        failures.append(
            "Grid count mismatch. "
            f"expected={expected_grid_count}, "
            f"actual={observed_grid_count}"
        )

    if observed_month_count != expected_month_count:
        failures.append(
            "Month count mismatch. "
            f"expected={expected_month_count}, "
            f"actual={observed_month_count}"
        )

    rows_per_grid = dataframe.groupby("grid_cell_key").size()

    if not rows_per_grid.eq(expected_month_count).all():
        failures.append(
            "Every target grid must contain exactly "
            f"{expected_month_count} observed label months."
        )

    rows_per_month = dataframe.groupby("reference_month").size()

    if not rows_per_month.eq(expected_grid_count).all():
        failures.append(
            "Every observed month must contain exactly " f"{expected_grid_count} target grids."
        )

    checks.append("complete_grid_month_skeleton_valid")


def _check_grid_reference_alignment(
    dataframe: pd.DataFrame,
    target_grids: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    grid_reference = target_grids[
        [
            "grid_cell_key",
            "grid_system",
            "province_key",
        ]
    ].copy()

    if grid_reference["grid_cell_key"].duplicated().any():
        failures.append("Target grid reference contains duplicate grid_cell_key values.")
        return

    expected_grid_keys = set(grid_reference["grid_cell_key"].astype(str))
    observed_grid_keys = set(dataframe["grid_cell_key"].astype(str))

    if observed_grid_keys != expected_grid_keys:
        failures.append(
            "Label grid set does not match target grid reference. "
            f"missing={sorted(expected_grid_keys - observed_grid_keys)[:20]}, "
            f"extra={sorted(observed_grid_keys - expected_grid_keys)[:20]}"
        )

    expected_grid_system = grid_reference.set_index("grid_cell_key")["grid_system"].astype(str)

    expected_province = grid_reference.set_index("grid_cell_key")["province_key"].astype(str)

    observed_grid_system = (
        dataframe[
            [
                "grid_cell_key",
                "grid_system",
            ]
        ]
        .drop_duplicates()
        .set_index("grid_cell_key")["grid_system"]
        .astype(str)
    )

    observed_province = (
        dataframe[
            [
                "grid_cell_key",
                "province_key",
            ]
        ]
        .drop_duplicates()
        .set_index("grid_cell_key")["province_key"]
        .astype(str)
    )

    observed_grid_system = observed_grid_system.reindex(expected_grid_system.index)
    observed_province = observed_province.reindex(expected_province.index)

    if not observed_grid_system.eq(expected_grid_system).all():
        failures.append("Label grid_system values do not match gold_grid_cell.")

    if not observed_province.eq(expected_province).all():
        failures.append("Label province_key values do not match gold_grid_cell.")

    expected_system_from_province = dataframe["province_key"].map(
        {
            "AB": "ab_10km",
            "BC": "bc_10km",
        }
    )

    if not dataframe["grid_system"].eq(expected_system_from_province).all():
        failures.append("grid_system does not match province_key.")

    checks.append("grid_reference_alignment_valid")


def _check_time_fields(
    dataframe: pd.DataFrame,
    expected_months: pd.PeriodIndex,
    failures: list[str],
    checks: list[str],
) -> None:
    parsed_month = _parse_reference_month(dataframe["reference_month"])
    event_year = pd.to_numeric(
        dataframe["event_year"],
        errors="coerce",
    )
    event_month_number = pd.to_numeric(
        dataframe["event_month_number"],
        errors="coerce",
    )

    if parsed_month.isna().any():
        failures.append("reference_month contains invalid values.")

    if event_year.isna().any():
        failures.append("event_year contains invalid values.")

    if event_month_number.isna().any():
        failures.append("event_month_number contains invalid values.")

    observed_months = set(parsed_month.dropna().unique())
    expected_month_set = set(expected_months)

    if observed_months != expected_month_set:
        failures.append(
            "Observed label months do not match expected source coverage. "
            f"missing={sorted(expected_month_set - observed_months)}, "
            f"extra={sorted(observed_months - expected_month_set)}"
        )

    valid = parsed_month.notna() & event_year.notna() & event_month_number.notna()

    if valid.any():
        if (
            not event_year.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.year.astype("Int64"))
            .all()
        ):
            failures.append("event_year does not match reference_month.")

        if (
            not event_month_number.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.month.astype("Int64"))
            .all()
        ):
            failures.append("event_month_number does not match reference_month.")

    checks.append("time_fields_valid")


def _check_count_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    numeric_counts: dict[str, pd.Series] = {}

    for column in COUNT_COLUMNS:
        values = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

        numeric_counts[column] = values

        if values.isna().any():
            failures.append(f"{column} contains invalid numeric values.")
            continue

        if not values.ge(0).all():
            failures.append(f"{column} contains negative values.")

        if not values.eq(np.floor(values)).all():
            failures.append(f"{column} must contain integer values.")

    if failures:
        return

    domain_total = dataframe[DOMAIN_COUNT_COLUMNS].sum(axis=1)

    if not domain_total.eq(dataframe["disaster_event_count"]).all():
        failures.append("Domain event counts do not sum to disaster_event_count.")

    bounded_columns = [
        "direct_cd_resolution_event_count",
        "csd_parent_cd_event_count",
        "cd_scope_event_count",
        "cd_group_scope_event_count",
        "csd_scope_event_count",
        "approximate_event_count",
        "low_overlap_event_count",
    ]

    for column in bounded_columns:
        if not dataframe[column].le(dataframe["disaster_event_count"]).all():
            failures.append(f"{column} cannot exceed disaster_event_count.")

    checks.append("count_fields_valid")


def _check_boolean_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    boolean_columns = [
        "label_is_observed",
        "disaster_event_occurred",
        "has_csd_parent_cd_approximation",
        "has_low_overlap_event",
    ]

    for column in boolean_columns:
        if dataframe[column].isna().any():
            failures.append(f"{column} contains nulls.")

    if not dataframe["label_is_observed"].fillna(False).astype(bool).all():
        failures.append("label_is_observed must be true for all output rows.")

    expected_occurred = dataframe["disaster_event_count"].gt(0)

    if not dataframe["disaster_event_occurred"].eq(expected_occurred).all():
        failures.append("disaster_event_occurred does not match " "disaster_event_count > 0.")

    expected_approximation = dataframe["approximate_event_count"].gt(0)

    if not dataframe["has_csd_parent_cd_approximation"].eq(expected_approximation).all():
        failures.append(
            "has_csd_parent_cd_approximation does not match " "approximate_event_count > 0."
        )

    expected_low_overlap = dataframe["low_overlap_event_count"].gt(0)

    if not dataframe["has_low_overlap_event"].eq(expected_low_overlap).all():
        failures.append("has_low_overlap_event does not match " "low_overlap_event_count > 0.")

    checks.append("boolean_semantics_valid")


def _check_zero_label_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    negative = dataframe[~dataframe["disaster_event_occurred"]]

    if negative.empty:
        failures.append("Label table contains no zero-label rows.")
        return

    if not negative[COUNT_COLUMNS].eq(0).all().all():
        failures.append("Zero-label rows must have zero in every event count column.")

    if not negative["disaster_event_types"].fillna("").eq("").all():
        failures.append("Zero-label rows must have empty disaster_event_types.")

    if not negative["disaster_event_reference_keys_json"].fillna("[]").eq("[]").all():
        failures.append("Zero-label rows must contain an empty event-key JSON list.")

    if not negative[QUALITY_COLUMNS].isna().all().all():
        failures.append("Zero-label rows must have null coverage-quality metrics.")

    if (
        negative[
            [
                "has_csd_parent_cd_approximation",
                "has_low_overlap_event",
            ]
        ]
        .astype(bool)
        .any()
        .any()
    ):
        failures.append("Zero-label rows cannot have approximation or " "low-overlap flags.")

    checks.append("zero_label_semantics_valid")


def _check_positive_label_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    positive = dataframe[dataframe["disaster_event_occurred"]]

    if positive.empty:
        failures.append("Label table contains no positive rows.")
        return

    if not positive["disaster_event_count"].gt(0).all():
        failures.append("Positive label rows must have disaster_event_count > 0.")

    if positive["disaster_event_types"].fillna("").eq("").any():
        failures.append("Positive label rows must contain disaster_event_types.")

    if positive["disaster_event_reference_keys_json"].fillna("[]").eq("[]").any():
        failures.append("Positive label rows must contain event reference keys.")

    if positive[QUALITY_COLUMNS].isna().any().any():
        failures.append("Positive label rows must contain all coverage metrics.")

    checks.append("positive_label_semantics_valid")


def _check_json_lineage(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    positive = dataframe[dataframe["disaster_event_occurred"]]

    for row in positive[
        [
            "grid_cell_key",
            "reference_month",
            "disaster_event_count",
            "disaster_event_reference_keys_json",
        ]
    ].itertuples(index=False):
        try:
            parsed = json.loads(str(row.disaster_event_reference_keys_json))
        except Exception:
            failures.append(
                "Invalid disaster_event_reference_keys_json for "
                f"{row.grid_cell_key}, {row.reference_month}."
            )
            break

        if not isinstance(parsed, list):
            failures.append("disaster_event_reference_keys_json must contain a list.")
            break

        normalized = [str(value) for value in parsed]

        if len(normalized) != len(set(normalized)):
            failures.append("disaster_event_reference_keys_json contains duplicates.")
            break

        if len(normalized) != int(row.disaster_event_count):
            failures.append(
                "Event-key JSON length does not match "
                "disaster_event_count for "
                f"{row.grid_cell_key}, {row.reference_month}."
            )
            break

    checks.append("json_lineage_valid")


def _check_quality_metric_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    positive = dataframe[dataframe["disaster_event_occurred"]]

    minimum = pd.to_numeric(
        positive["minimum_event_grid_coverage_ratio"],
        errors="coerce",
    )
    mean = pd.to_numeric(
        positive["mean_event_grid_coverage_ratio"],
        errors="coerce",
    )
    maximum = pd.to_numeric(
        positive["maximum_event_grid_coverage_ratio"],
        errors="coerce",
    )

    if minimum.isna().any() or mean.isna().any() or maximum.isna().any():
        failures.append("Positive label rows contain invalid coverage metrics.")
        return

    if not minimum.between(
        0.0,
        1.0,
        inclusive="right",
    ).all():
        failures.append("minimum_event_grid_coverage_ratio must be within (0, 1].")

    if not mean.between(
        0.0,
        1.0,
        inclusive="right",
    ).all():
        failures.append("mean_event_grid_coverage_ratio must be within (0, 1].")

    if not maximum.between(
        0.0,
        1.0,
        inclusive="right",
    ).all():
        failures.append("maximum_event_grid_coverage_ratio must be within (0, 1].")

    if not minimum.le(mean).all():
        failures.append("Minimum coverage ratio cannot exceed mean coverage ratio.")

    if not mean.le(maximum).all():
        failures.append("Mean coverage ratio cannot exceed maximum coverage ratio.")

    checks.append("quality_metric_semantics_valid")


def _check_event_grid_reconciliation(
    dataframe: pd.DataFrame,
    event_grid_scope: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    source = event_grid_scope.copy()

    source["grid_cell_key"] = source["grid_cell_key"].astype(str)
    source["reference_month"] = source["reference_month"].astype(str)
    source["disaster_event_reference_key"] = source["disaster_event_reference_key"].astype(str)

    source["wildfire_event_count"] = source["disaster_domain"].eq("wildfire").astype("int64")
    source["flood_event_count"] = source["disaster_domain"].eq("flood").astype("int64")
    source["storm_or_climate_event_count"] = (
        source["disaster_domain"].eq("severe_storm_or_climate").astype("int64")
    )
    source["climate_extreme_event_count"] = (
        source["disaster_domain"].eq("climate_extreme").astype("int64")
    )

    source["direct_cd_resolution_event_count"] = source["resolution_methods_json"].map(
        lambda value: int(_json_list_contains(value, "direct_cd"))
    )
    source["csd_parent_cd_event_count"] = source["resolution_methods_json"].map(
        lambda value: int(_json_list_contains(value, "csd_parent_cd"))
    )
    source["cd_scope_event_count"] = source["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CD"))
    )
    source["cd_group_scope_event_count"] = source["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CD_GROUP"))
    )
    source["csd_scope_event_count"] = source["source_mapped_geo_levels_json"].map(
        lambda value: int(_json_list_contains(value, "CSD"))
    )
    source["approximate_event_count"] = (
        source["is_csd_to_cd_approximation"].fillna(False).astype(bool).astype("int64")
    )
    source["low_overlap_event_count"] = (
        pd.to_numeric(
            source["affected_grid_coverage_ratio"],
            errors="coerce",
        )
        .le(0.05)
        .astype("int64")
    )

    expected = (
        source.groupby(
            [
                "grid_cell_key",
                "reference_month",
            ],
            as_index=False,
            sort=False,
        )
        .agg(
            disaster_event_count=(
                "disaster_event_reference_key",
                "nunique",
            ),
            wildfire_event_count=(
                "wildfire_event_count",
                "sum",
            ),
            flood_event_count=(
                "flood_event_count",
                "sum",
            ),
            storm_or_climate_event_count=(
                "storm_or_climate_event_count",
                "sum",
            ),
            climate_extreme_event_count=(
                "climate_extreme_event_count",
                "sum",
            ),
            direct_cd_resolution_event_count=(
                "direct_cd_resolution_event_count",
                "sum",
            ),
            csd_parent_cd_event_count=(
                "csd_parent_cd_event_count",
                "sum",
            ),
            cd_scope_event_count=(
                "cd_scope_event_count",
                "sum",
            ),
            cd_group_scope_event_count=(
                "cd_group_scope_event_count",
                "sum",
            ),
            csd_scope_event_count=(
                "csd_scope_event_count",
                "sum",
            ),
            approximate_event_count=(
                "approximate_event_count",
                "sum",
            ),
            low_overlap_event_count=(
                "low_overlap_event_count",
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
        .sort_values(
            [
                "grid_cell_key",
                "reference_month",
            ]
        )
        .reset_index(drop=True)
    )

    actual = (
        dataframe[dataframe["disaster_event_occurred"]][
            [
                "grid_cell_key",
                "reference_month",
                *COUNT_COLUMNS,
                "disaster_event_types",
                "disaster_event_reference_keys_json",
                *QUALITY_COLUMNS,
            ]
        ]
        .sort_values(
            [
                "grid_cell_key",
                "reference_month",
            ]
        )
        .reset_index(drop=True)
    )

    if len(actual) != len(expected):
        failures.append(
            "Positive label row count does not match event-grid aggregation. "
            f"expected={len(expected)}, actual={len(actual)}"
        )
        return

    actual_keys = actual[
        [
            "grid_cell_key",
            "reference_month",
        ]
    ]
    expected_keys = expected[
        [
            "grid_cell_key",
            "reference_month",
        ]
    ]

    if not actual_keys.equals(expected_keys):
        failures.append("Positive label grid-month keys do not match event-grid source.")
        return

    for column in COUNT_COLUMNS:
        actual_values = pd.to_numeric(
            actual[column],
            errors="coerce",
        )
        expected_values = pd.to_numeric(
            expected[column],
            errors="coerce",
        )

        if not actual_values.eq(expected_values).all():
            mismatch_count = int((~actual_values.eq(expected_values)).sum())
            failures.append(
                f"{column} does not reconcile to event-grid source " f"for {mismatch_count} rows."
            )

    string_columns = [
        "disaster_event_types",
        "disaster_event_reference_keys_json",
    ]

    for column in string_columns:
        if not actual[column].astype(str).eq(expected[column].astype(str)).all():
            mismatch_count = int(
                (~actual[column].astype(str).eq(expected[column].astype(str))).sum()
            )
            failures.append(
                f"{column} does not reconcile to event-grid source " f"for {mismatch_count} rows."
            )

    for column in QUALITY_COLUMNS:
        actual_values = pd.to_numeric(
            actual[column],
            errors="coerce",
        ).to_numpy()
        expected_values = pd.to_numeric(
            expected[column],
            errors="coerce",
        ).to_numpy()

        if not np.isclose(
            actual_values,
            expected_values,
            rtol=1e-10,
            atol=1e-12,
            equal_nan=True,
        ).all():
            mismatch_count = int(
                (
                    ~np.isclose(
                        actual_values,
                        expected_values,
                        rtol=1e-10,
                        atol=1e-12,
                        equal_nan=True,
                    )
                ).sum()
            )
            failures.append(
                f"{column} does not reconcile to event-grid source " f"for {mismatch_count} rows."
            )

    source_assignment_count = len(event_grid_scope)
    label_assignment_count = int(dataframe["disaster_event_count"].sum())

    if label_assignment_count != source_assignment_count:
        failures.append(
            "Total event assignments do not reconcile. "
            f"expected={source_assignment_count}, "
            f"actual={label_assignment_count}"
        )

    checks.append("event_grid_source_reconciliation_valid")


def _build_report(
    *,
    grid_month_label: pd.DataFrame,
    event_grid_scope: pd.DataFrame,
    source_end_month: pd.Period,
    expected_end_month: pd.Period,
    checks: list[str],
) -> dict[str, Any]:
    positive = grid_month_label[grid_month_label["disaster_event_occurred"]]

    return {
        "table_name": TABLE_NAME,
        "validation_status": "passed",
        "checks_passed": checks,
        "check_count": len(checks),
        "row_count": int(len(grid_month_label)),
        "grid_count": int(grid_month_label["grid_cell_key"].nunique()),
        "month_count": int(grid_month_label["reference_month"].nunique()),
        "minimum_reference_month": str(grid_month_label["reference_month"].min()),
        "maximum_reference_month": str(grid_month_label["reference_month"].max()),
        "source_maximum_reference_month": str(source_end_month),
        "expected_label_end_month": str(expected_end_month),
        "positive_label_row_count": int(len(positive)),
        "negative_label_row_count": int((~grid_month_label["disaster_event_occurred"]).sum()),
        "positive_label_rate": float(len(positive) / len(grid_month_label)),
        "unique_positive_grid_count": int(positive["grid_cell_key"].nunique()),
        "source_event_grid_row_count": int(len(event_grid_scope)),
        "total_disaster_event_assignments": int(grid_month_label["disaster_event_count"].sum()),
        "maximum_events_per_grid_month": int(grid_month_label["disaster_event_count"].max()),
        "total_wildfire_event_assignments": int(grid_month_label["wildfire_event_count"].sum()),
        "total_flood_event_assignments": int(grid_month_label["flood_event_count"].sum()),
        "total_storm_or_climate_event_assignments": int(
            grid_month_label["storm_or_climate_event_count"].sum()
        ),
        "total_climate_extreme_event_assignments": int(
            grid_month_label["climate_extreme_event_count"].sum()
        ),
        "positive_rows_with_csd_approximation": int(
            positive["has_csd_parent_cd_approximation"].sum()
        ),
        "positive_rows_with_low_overlap_event": int(positive["has_low_overlap_event"].sum()),
        "province_counts": _value_counts(grid_month_label["province_key"]),
        "grid_system_counts": _value_counts(grid_month_label["grid_system"]),
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
        raise GoldGridMonthDisasterEventLabelValidationError(
            f"Invalid JSON list value: {value}"
        ) from exc

    if not isinstance(parsed, list):
        raise GoldGridMonthDisasterEventLabelValidationError(f"Expected JSON list value: {value}")

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


def _raise(failures: list[str]) -> None:
    message = "Gold grid-month disaster event label validation failed:\n"
    message += "\n".join(f"- {failure}" for failure in failures)
    raise GoldGridMonthDisasterEventLabelValidationError(message)
