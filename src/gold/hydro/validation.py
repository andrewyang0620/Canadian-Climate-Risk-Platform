from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet

EXPECTED_MONTH_MIN = "2016-01"
EXPECTED_MONTH_MAX = "2025-12"
EXPECTED_MONTH_COUNT = 120

EXPECTED_PROVINCES = {"AB", "BC"}
EXPECTED_MEASUREMENT_TYPES = {"flow", "level"}
EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}

HYDRO_SPATIAL_METHOD_BASIN = "basin_polygon_intersection"
HYDRO_SPATIAL_METHOD_POINT = "station_point_in_cell"
HYDRO_SPATIAL_METHOD_NONE = "no_hydro_coverage"

EXPECTED_SPATIAL_ASSIGNMENT_METHODS = {
    HYDRO_SPATIAL_METHOD_BASIN,
    HYDRO_SPATIAL_METHOD_POINT,
    HYDRO_SPATIAL_METHOD_NONE,
}

EXPECTED_PRODUCTION_GRID_COUNT = 16_508

EXPECTED_METHOD_GRID_COUNTS = {
    HYDRO_SPATIAL_METHOD_BASIN: 13_727,
    HYDRO_SPATIAL_METHOD_POINT: 2,
    HYDRO_SPATIAL_METHOD_NONE: 2_779,
}

VALID_QUALITY_FLAGS = {"high", "medium", "low", "very_low"}

VALUE_ORDER_TOLERANCE = 1e-8

HYDRO_LATITUDE_MIN = 48.0
HYDRO_LATITUDE_MAX = 61.0
HYDRO_LONGITUDE_MIN = -140.0
HYDRO_LONGITUDE_MAX = -109.0

FLOW_VALUE_COLUMNS = [
    "flow_mean_measurement_value",
    "flow_min_measurement_value",
    "flow_max_measurement_value",
    "flow_median_measurement_value",
    "flow_p95_measurement_value",
]

LEVEL_VALUE_COLUMNS = [
    "level_mean_measurement_value",
    "level_min_measurement_value",
    "level_max_measurement_value",
    "level_median_measurement_value",
    "level_p95_measurement_value",
]


class GoldHydroValidationError(Exception):
    """Raised when Gold hydro validation cannot be executed."""


@dataclass(frozen=True)
class GoldHydroValidationCheck:
    name: str
    passed: bool
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "details": self.details,
        }


@dataclass
class GoldHydroValidationReport:
    validation_name: str
    checks: list[GoldHydroValidationCheck] = field(default_factory=list)
    output_paths: dict[str, str] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def add_check(
        self,
        *,
        name: str,
        passed: bool,
        details: dict[str, Any],
    ) -> None:
        self.checks.append(
            GoldHydroValidationCheck(
                name=name,
                passed=bool(passed),
                details=details,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_name": self.validation_name,
            "passed": self.passed,
            "checks": [check.to_dict() for check in self.checks],
            "output_paths": self.output_paths,
        }


def validate_hydro_monthly_feature_dataframes(
    *,
    station_month: pd.DataFrame,
    grid_month: pd.DataFrame | None = None,
    gold_grid_cell: pd.DataFrame | None = None,
) -> GoldHydroValidationReport:
    report = GoldHydroValidationReport(validation_name="gold_hydro_monthly_feature_validation")

    _require_station_month_columns(station_month)
    _validate_station_month(report, station_month)

    if grid_month is None and gold_grid_cell is None:
        return report

    if grid_month is None or gold_grid_cell is None:
        raise GoldHydroValidationError("grid_month and gold_grid_cell must be provided together.")

    _require_grid_month_columns(grid_month)
    _require_grid_cell_columns(gold_grid_cell)
    _validate_grid_month(report, grid_month, gold_grid_cell)

    return report


def validate_hydro_monthly_feature_outputs(
    *,
    gold_root: str | Path = "lakehouse/gold",
    output_json_path: str | Path = (
        "lakehouse/gold/_validation/hydro_monthly_features/latest_validation.json"
    ),
) -> GoldHydroValidationReport:
    station_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_hydro_station_month_feature",
    )
    grid_month_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_hydro_feature",
    )
    grid_cell_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )

    report = validate_hydro_monthly_feature_dataframes(
        station_month=pd.read_parquet(station_path),
        grid_month=pd.read_parquet(grid_month_path),
        gold_grid_cell=pd.read_parquet(grid_cell_path),
    )

    report.output_paths = {
        "gold_hydro_station_month_feature": station_path.as_posix(),
        "gold_grid_month_hydro_feature": grid_month_path.as_posix(),
        "gold_grid_cell": grid_cell_path.as_posix(),
    }

    final_output_path = Path(output_json_path)
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    final_output_path.write_text(
        json.dumps(report.to_dict(), indent=2),
        encoding="utf-8",
    )

    return report


def _validate_station_month(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    row_count = len(station_month)

    report.add_check(
        name="gold_hydro_station_month_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
    )

    _add_key_check(
        report,
        dataframe=station_month,
        key_column="hydro_station_month_key",
        check_name="gold_hydro_station_month_key_valid",
    )
    _add_month_range_check(
        report,
        dataframe=station_month,
        check_name="gold_hydro_station_month_range_valid",
    )
    _add_province_check(
        report,
        dataframe=station_month,
        check_name="gold_hydro_station_month_provinces_valid",
    )
    _add_measurement_type_check(
        report,
        dataframe=station_month,
        check_name="gold_hydro_station_month_measurement_types_valid",
    )

    _add_coordinate_check(report, station_month)
    _add_station_count_consistency_check(report, station_month)
    _add_station_ratio_check(report, station_month)
    _add_station_measurement_value_check(report, station_month)
    _add_station_flow_nonnegative_check(report, station_month)
    _add_station_symbol_count_check(report, station_month)


def _validate_grid_month(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
) -> None:
    row_count = len(grid_month)

    target_grid = gold_grid_cell[gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS)].copy()
    valid_grid_keys = set(target_grid["grid_cell_key"].astype(str))
    expected_row_count = len(valid_grid_keys) * EXPECTED_MONTH_COUNT

    report.add_check(
        name="gold_grid_month_hydro_full_skeleton_row_count_valid",
        passed=row_count == expected_row_count,
        details={
            "row_count": row_count,
            "expected_row_count": expected_row_count,
            "valid_10km_grid_cell_count": len(valid_grid_keys),
            "expected_month_count": EXPECTED_MONTH_COUNT,
        },
    )

    _add_key_check(
        report,
        dataframe=grid_month,
        key_column="grid_month_hydro_feature_key",
        check_name="gold_grid_month_hydro_key_valid",
    )
    _add_month_range_check(
        report,
        dataframe=grid_month,
        check_name="gold_grid_month_hydro_month_range_valid",
    )
    _add_province_check(
        report,
        dataframe=grid_month,
        check_name="gold_grid_month_hydro_provinces_valid",
    )

    _add_grid_system_check(report, grid_month)
    _add_grid_key_coverage_check(report, grid_month, valid_grid_keys)
    _add_grid_spatial_method_check(report, grid_month, len(valid_grid_keys))
    _add_grid_station_count_check(report, grid_month)
    _add_no_hydro_coverage_semantics_check(report, grid_month)
    _add_point_in_cell_semantics_check(report, grid_month)
    _add_basin_semantics_check(report, grid_month)
    _add_hydro_prefix_measurement_checks(report, grid_month, prefix="flow")
    _add_hydro_prefix_measurement_checks(report, grid_month, prefix="level")
    _add_grid_flow_nonnegative_check(report, grid_month)
    _add_grid_ratio_checks(report, grid_month)
    _add_grid_quality_flag_check(report, grid_month)
    _add_grid_value_coverage_check(report, grid_month)


def _add_key_check(
    report: GoldHydroValidationReport,
    *,
    dataframe: pd.DataFrame,
    key_column: str,
    check_name: str,
) -> None:
    key_nulls = int(dataframe[key_column].isna().sum())
    key_duplicates = int(dataframe[key_column].duplicated().sum())

    report.add_check(
        name=check_name,
        passed=key_nulls == 0 and key_duplicates == 0,
        details={
            "null_count": key_nulls,
            "duplicate_count": key_duplicates,
        },
    )


def _add_month_range_check(
    report: GoldHydroValidationReport,
    *,
    dataframe: pd.DataFrame,
    check_name: str,
) -> None:
    months = sorted(dataframe["reference_month"].dropna().astype(str).unique().tolist())

    report.add_check(
        name=check_name,
        passed=(
            len(months) == EXPECTED_MONTH_COUNT
            and months[0] == EXPECTED_MONTH_MIN
            and months[-1] == EXPECTED_MONTH_MAX
        ),
        details={
            "month_count": len(months),
            "minimum_month": months[0] if months else None,
            "maximum_month": months[-1] if months else None,
            "expected_month_count": EXPECTED_MONTH_COUNT,
            "expected_minimum_month": EXPECTED_MONTH_MIN,
            "expected_maximum_month": EXPECTED_MONTH_MAX,
        },
    )


def _add_province_check(
    report: GoldHydroValidationReport,
    *,
    dataframe: pd.DataFrame,
    check_name: str,
) -> None:
    actual_provinces = set(dataframe["province_key"].dropna().astype(str).unique())

    report.add_check(
        name=check_name,
        passed=actual_provinces == EXPECTED_PROVINCES,
        details={
            "actual": sorted(actual_provinces),
            "expected": sorted(EXPECTED_PROVINCES),
            "counts": _value_counts(dataframe["province_key"]),
        },
    )


def _add_measurement_type_check(
    report: GoldHydroValidationReport,
    *,
    dataframe: pd.DataFrame,
    check_name: str,
) -> None:
    actual_measurement_types = set(dataframe["measurement_type"].dropna().astype(str).unique())

    report.add_check(
        name=check_name,
        passed=actual_measurement_types == EXPECTED_MEASUREMENT_TYPES,
        details={
            "actual": sorted(actual_measurement_types),
            "expected": sorted(EXPECTED_MEASUREMENT_TYPES),
            "counts": _value_counts(dataframe["measurement_type"]),
        },
    )


def _add_grid_system_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    actual_grid_systems = set(grid_month["grid_system"].dropna().astype(str).unique())

    report.add_check(
        name="gold_grid_month_hydro_grid_systems_valid",
        passed=actual_grid_systems == EXPECTED_GRID_SYSTEMS,
        details={
            "actual": sorted(actual_grid_systems),
            "expected": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": _value_counts(grid_month["grid_system"]),
        },
    )


def _add_grid_key_coverage_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
    valid_grid_keys: set[str],
) -> None:
    feature_grid_keys = set(grid_month["grid_cell_key"].astype(str))
    unknown_keys = sorted(feature_grid_keys - valid_grid_keys)
    missing_keys = sorted(valid_grid_keys - feature_grid_keys)

    rows_per_grid = grid_month.groupby("grid_cell_key")["reference_month"].nunique()
    invalid_month_count_grids = rows_per_grid[rows_per_grid != EXPECTED_MONTH_COUNT]

    report.add_check(
        name="gold_grid_month_hydro_grid_keys_and_skeleton_valid",
        passed=(
            len(unknown_keys) == 0
            and len(missing_keys) == 0
            and len(invalid_month_count_grids) == 0
        ),
        details={
            "feature_grid_cell_count": len(feature_grid_keys),
            "valid_10km_grid_cell_count": len(valid_grid_keys),
            "unknown_grid_key_count": len(unknown_keys),
            "missing_grid_key_count": len(missing_keys),
            "invalid_month_count_grid_count": int(len(invalid_month_count_grids)),
            "unknown_grid_key_sample": unknown_keys[:20],
            "missing_grid_key_sample": missing_keys[:20],
        },
    )


def _add_grid_spatial_method_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
    valid_grid_cell_count: int,
) -> None:
    actual_methods = set(
        grid_month["hydro_spatial_assignment_method"].dropna().astype(str).unique()
    )
    unknown_methods = sorted(actual_methods - EXPECTED_SPATIAL_ASSIGNMENT_METHODS)

    method_by_grid = grid_month[
        ["grid_cell_key", "hydro_spatial_assignment_method"]
    ].drop_duplicates()

    method_counts_by_grid = {
        str(key): int(value)
        for key, value in method_by_grid["hydro_spatial_assignment_method"]
        .value_counts(dropna=False)
        .to_dict()
        .items()
    }

    method_counts_by_row = _value_counts(grid_month["hydro_spatial_assignment_method"])

    expected_row_counts_from_actual_grid_counts = {
        method: grid_count * EXPECTED_MONTH_COUNT
        for method, grid_count in method_counts_by_grid.items()
    }

    method_count_per_grid = method_by_grid.groupby("grid_cell_key").size()
    multi_method_grid_count = int((method_count_per_grid != 1).sum())

    enforce_production_counts = (
        valid_grid_cell_count == EXPECTED_PRODUCTION_GRID_COUNT
        and grid_month["grid_cell_key"].nunique() == EXPECTED_PRODUCTION_GRID_COUNT
    )

    production_method_counts_valid = True

    if enforce_production_counts:
        production_method_counts_valid = method_counts_by_grid == EXPECTED_METHOD_GRID_COUNTS

    report.add_check(
        name="gold_grid_month_hydro_spatial_assignment_methods_valid",
        passed=(
            len(unknown_methods) == 0
            and method_counts_by_row == expected_row_counts_from_actual_grid_counts
            and multi_method_grid_count == 0
            and production_method_counts_valid
        ),
        details={
            "actual_methods": sorted(actual_methods),
            "unknown_methods": unknown_methods,
            "method_counts_by_grid": method_counts_by_grid,
            "method_counts_by_grid_month": method_counts_by_row,
            "expected_method_counts_by_grid_month_from_actual_grid_counts": (
                expected_row_counts_from_actual_grid_counts
            ),
            "multi_method_grid_count": multi_method_grid_count,
            "enforce_production_counts": enforce_production_counts,
            "expected_production_method_counts_by_grid": EXPECTED_METHOD_GRID_COUNTS,
            "production_method_counts_valid": production_method_counts_valid,
        },
    )


def _add_grid_station_count_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    station_count_columns = [
        "hydro_station_count",
        "hydro_basin_station_count",
        "hydro_point_station_count",
        "flow_station_count",
        "level_station_count",
    ]

    negative_count_rows = int((grid_month[station_count_columns] < 0).any(axis=1).sum())

    inconsistent_hydro_count_rows = int(
        (
            grid_month["hydro_station_count"]
            != (grid_month["hydro_basin_station_count"] + grid_month["hydro_point_station_count"])
        ).sum()
    )

    measurement_count_exceeds_hydro_rows = int(
        (
            (grid_month["flow_station_count"] > grid_month["hydro_station_count"])
            | (grid_month["level_station_count"] > grid_month["hydro_station_count"])
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_station_counts_valid",
        passed=(
            negative_count_rows == 0
            and inconsistent_hydro_count_rows == 0
            and measurement_count_exceeds_hydro_rows == 0
        ),
        details={
            "negative_count_rows": negative_count_rows,
            "inconsistent_hydro_count_rows": inconsistent_hydro_count_rows,
            "measurement_count_exceeds_hydro_rows": measurement_count_exceeds_hydro_rows,
            "maximum_hydro_station_count": int(grid_month["hydro_station_count"].max()),
            "maximum_flow_station_count": int(grid_month["flow_station_count"].max()),
            "maximum_level_station_count": int(grid_month["level_station_count"].max()),
        },
    )


def _add_no_hydro_coverage_semantics_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    no_coverage = grid_month[
        grid_month["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_NONE
    ]

    station_nonzero_rows = int(
        (
            no_coverage[
                [
                    "hydro_station_count",
                    "hydro_basin_station_count",
                    "hydro_point_station_count",
                    "flow_station_count",
                    "level_station_count",
                    "flow_daily_record_count",
                    "flow_observation_day_count",
                    "flow_measurement_observation_count",
                    "flow_zero_day_count",
                    "flow_negative_value_count",
                    "level_daily_record_count",
                    "level_observation_day_count",
                    "level_measurement_observation_count",
                    "level_negative_value_count",
                ]
            ]
            != 0
        )
        .any(axis=1)
        .sum()
    )

    value_nonnull_rows = int(
        no_coverage[
            [
                *FLOW_VALUE_COLUMNS,
                *LEVEL_VALUE_COLUMNS,
                "flow_measurement_completeness_ratio",
                "level_measurement_completeness_ratio",
                "hydro_data_completeness_score",
                "hydro_feature_quality_flag",
                "hydro_basin_intersection_area_sq_km",
                "hydro_basin_grid_coverage_ratio",
            ]
        ]
        .notna()
        .any(axis=1)
        .sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_no_coverage_semantics_valid",
        passed=station_nonzero_rows == 0 and value_nonnull_rows == 0,
        details={
            "no_coverage_row_count": int(len(no_coverage)),
            "station_or_count_nonzero_rows": station_nonzero_rows,
            "value_or_quality_nonnull_rows": value_nonnull_rows,
        },
    )


def _add_point_in_cell_semantics_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    point = grid_month[grid_month["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_POINT]

    point_grid_count = int(point["grid_cell_key"].nunique())
    high_quality_rows = int(point["hydro_feature_quality_flag"].eq("high").sum())

    invalid_station_rows = int(
        (
            (point["hydro_point_station_count"] < 1)
            | (point["hydro_basin_station_count"] != 0)
            | (point["hydro_station_count"] != point["hydro_point_station_count"])
        ).sum()
    )

    basin_field_nonnull_rows = int(
        point[
            [
                "hydro_basin_intersection_area_sq_km",
                "hydro_basin_grid_coverage_ratio",
            ]
        ]
        .notna()
        .any(axis=1)
        .sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_point_in_cell_semantics_valid",
        passed=(
            high_quality_rows == 0 and invalid_station_rows == 0 and basin_field_nonnull_rows == 0
        ),
        details={
            "point_row_count": int(len(point)),
            "point_grid_count": point_grid_count,
            "production_expected_point_grid_count": EXPECTED_METHOD_GRID_COUNTS[
                HYDRO_SPATIAL_METHOD_POINT
            ],
            "high_quality_rows": high_quality_rows,
            "invalid_station_rows": invalid_station_rows,
            "basin_field_nonnull_rows": basin_field_nonnull_rows,
            "quality_counts": _value_counts(point["hydro_feature_quality_flag"]),
        },
    )


def _add_basin_semantics_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    basin = grid_month[grid_month["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_BASIN]

    basin_grid_count = int(basin["grid_cell_key"].nunique())

    invalid_station_rows = int(
        (
            (basin["hydro_basin_station_count"] < 1)
            | (basin["hydro_point_station_count"] != 0)
            | (basin["hydro_station_count"] != basin["hydro_basin_station_count"])
        ).sum()
    )

    invalid_basin_area_rows = int(
        (
            basin["hydro_basin_intersection_area_sq_km"].isna()
            | (basin["hydro_basin_intersection_area_sq_km"] <= 0)
            | basin["hydro_basin_grid_coverage_ratio"].isna()
            | ~basin["hydro_basin_grid_coverage_ratio"].between(
                0,
                1,
                inclusive="both",
            )
        ).sum()
    )

    max_ratio = float(basin["hydro_basin_grid_coverage_ratio"].max()) if not basin.empty else None

    report.add_check(
        name="gold_grid_month_hydro_basin_semantics_valid",
        passed=invalid_station_rows == 0 and invalid_basin_area_rows == 0,
        details={
            "basin_row_count": int(len(basin)),
            "basin_grid_count": basin_grid_count,
            "production_expected_basin_grid_count": EXPECTED_METHOD_GRID_COUNTS[
                HYDRO_SPATIAL_METHOD_BASIN
            ],
            "invalid_station_rows": invalid_station_rows,
            "invalid_basin_area_or_ratio_rows": invalid_basin_area_rows,
            "maximum_hydro_basin_grid_coverage_ratio": max_ratio,
        },
    )


def _add_hydro_prefix_measurement_checks(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
    *,
    prefix: str,
) -> None:
    value_columns = [
        f"{prefix}_mean_measurement_value",
        f"{prefix}_min_measurement_value",
        f"{prefix}_max_measurement_value",
        f"{prefix}_median_measurement_value",
        f"{prefix}_p95_measurement_value",
    ]
    completeness_column = f"{prefix}_measurement_completeness_ratio"

    count_columns = [
        f"{prefix}_station_count",
        f"{prefix}_daily_record_count",
        f"{prefix}_observation_day_count",
        f"{prefix}_measurement_observation_count",
        f"{prefix}_negative_value_count",
    ]

    if prefix == "flow":
        count_columns.append("flow_zero_day_count")

    station_count = grid_month[f"{prefix}_station_count"]
    zero_station = station_count == 0
    positive_station = station_count > 0

    zero_station_nonzero_count_rows = int(
        (grid_month.loc[zero_station, count_columns] != 0).any(axis=1).sum()
    )

    zero_station_nonnull_value_rows = int(
        grid_month.loc[
            zero_station,
            [*value_columns, completeness_column],
        ]
        .notna()
        .any(axis=1)
        .sum()
    )

    positive_station_null_value_rows = int(
        grid_month.loc[
            positive_station,
            [*value_columns, completeness_column],
        ]
        .isna()
        .any(axis=1)
        .sum()
    )

    count_relationship_invalid_rows = int(
        (
            positive_station
            & (
                (grid_month[f"{prefix}_daily_record_count"] < 1)
                | (grid_month[f"{prefix}_observation_day_count"] < 1)
                | (
                    grid_month[f"{prefix}_measurement_observation_count"]
                    > grid_month[f"{prefix}_daily_record_count"]
                )
            )
        ).sum()
    )

    tolerance = VALUE_ORDER_TOLERANCE

    value_order_invalid_rows = int(
        (
            positive_station
            & (
                (
                    grid_month[f"{prefix}_min_measurement_value"]
                    > grid_month[f"{prefix}_mean_measurement_value"] + tolerance
                )
                | (
                    grid_month[f"{prefix}_mean_measurement_value"]
                    > grid_month[f"{prefix}_max_measurement_value"] + tolerance
                )
                | (
                    grid_month[f"{prefix}_min_measurement_value"]
                    > grid_month[f"{prefix}_median_measurement_value"] + tolerance
                )
                | (
                    grid_month[f"{prefix}_median_measurement_value"]
                    > grid_month[f"{prefix}_max_measurement_value"] + tolerance
                )
                | (
                    grid_month[f"{prefix}_min_measurement_value"]
                    > grid_month[f"{prefix}_p95_measurement_value"] + tolerance
                )
                | (
                    grid_month[f"{prefix}_p95_measurement_value"]
                    > grid_month[f"{prefix}_max_measurement_value"] + tolerance
                )
            )
        ).sum()
    )

    completeness_out_of_range_rows = int(
        (
            grid_month[completeness_column].notna()
            & ~grid_month[completeness_column].between(0, 1, inclusive="both")
        ).sum()
    )

    report.add_check(
        name=f"gold_grid_month_hydro_{prefix}_measurement_semantics_valid",
        passed=(
            zero_station_nonzero_count_rows == 0
            and zero_station_nonnull_value_rows == 0
            and positive_station_null_value_rows == 0
            and count_relationship_invalid_rows == 0
            and value_order_invalid_rows == 0
            and completeness_out_of_range_rows == 0
        ),
        details={
            f"{prefix}_zero_station_rows": int(zero_station.sum()),
            f"{prefix}_positive_station_rows": int(positive_station.sum()),
            "zero_station_nonzero_count_rows": zero_station_nonzero_count_rows,
            "zero_station_nonnull_value_rows": zero_station_nonnull_value_rows,
            "positive_station_null_value_rows": positive_station_null_value_rows,
            "count_relationship_invalid_rows": count_relationship_invalid_rows,
            "value_order_invalid_rows": value_order_invalid_rows,
            "completeness_out_of_range_rows": completeness_out_of_range_rows,
            f"{prefix}_non_null_mean_value_rows": int(
                grid_month[f"{prefix}_mean_measurement_value"].notna().sum()
            ),
        },
    )


def _add_grid_flow_nonnegative_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    flow_positive = grid_month["flow_station_count"] > 0

    flow_negative_rows = int(
        (
            flow_positive
            & (
                (grid_month["flow_negative_value_count"] > 0)
                | (grid_month["flow_min_measurement_value"] < 0)
            )
        ).sum()
    )

    level_negative_rows = int(
        (
            (grid_month["level_station_count"] > 0) & (grid_month["level_negative_value_count"] > 0)
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_flow_nonnegative",
        passed=flow_negative_rows == 0,
        details={
            "flow_negative_rows": flow_negative_rows,
            "level_negative_rows_allowed": level_negative_rows,
        },
    )


def _add_grid_ratio_checks(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    score = grid_month["hydro_data_completeness_score"]

    score_out_of_range_rows = int((score.notna() & ~score.between(0, 1, inclusive="both")).sum())

    basin_ratio_out_of_range_rows = int(
        (
            grid_month["hydro_basin_grid_coverage_ratio"].notna()
            & ~grid_month["hydro_basin_grid_coverage_ratio"].between(
                0,
                1,
                inclusive="both",
            )
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_ratio_fields_valid",
        passed=score_out_of_range_rows == 0 and basin_ratio_out_of_range_rows == 0,
        details={
            "hydro_data_completeness_score_out_of_range_rows": score_out_of_range_rows,
            "hydro_basin_grid_coverage_ratio_out_of_range_rows": basin_ratio_out_of_range_rows,
            "hydro_data_completeness_score_min": (
                float(score.min()) if score.notna().any() else None
            ),
            "hydro_data_completeness_score_max": (
                float(score.max()) if score.notna().any() else None
            ),
            "hydro_basin_grid_coverage_ratio_max": (
                float(grid_month["hydro_basin_grid_coverage_ratio"].max())
                if grid_month["hydro_basin_grid_coverage_ratio"].notna().any()
                else None
            ),
        },
    )


def _add_grid_quality_flag_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    actual_quality_flags = set(
        grid_month["hydro_feature_quality_flag"].dropna().astype(str).unique()
    )

    no_coverage_quality_nonnull_rows = int(
        (
            grid_month["hydro_spatial_assignment_method"].eq(HYDRO_SPATIAL_METHOD_NONE)
            & grid_month["hydro_feature_quality_flag"].notna()
        ).sum()
    )

    score_null_quality_nonnull_rows = int(
        (
            grid_month["hydro_data_completeness_score"].isna()
            & grid_month["hydro_feature_quality_flag"].notna()
        ).sum()
    )

    score_nonnull_quality_null_rows = int(
        (
            grid_month["hydro_data_completeness_score"].notna()
            & grid_month["hydro_spatial_assignment_method"].ne(HYDRO_SPATIAL_METHOD_NONE)
            & grid_month["hydro_feature_quality_flag"].isna()
        ).sum()
    )

    point_high_quality_rows = int(
        (
            grid_month["hydro_spatial_assignment_method"].eq(HYDRO_SPATIAL_METHOD_POINT)
            & grid_month["hydro_feature_quality_flag"].eq("high")
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_quality_flags_valid",
        passed=(
            actual_quality_flags <= VALID_QUALITY_FLAGS
            and no_coverage_quality_nonnull_rows == 0
            and score_null_quality_nonnull_rows == 0
            and score_nonnull_quality_null_rows == 0
            and point_high_quality_rows == 0
        ),
        details={
            "actual": sorted(actual_quality_flags),
            "expected_allowed": sorted(VALID_QUALITY_FLAGS),
            "counts": _value_counts(grid_month["hydro_feature_quality_flag"]),
            "no_coverage_quality_nonnull_rows": no_coverage_quality_nonnull_rows,
            "score_null_quality_nonnull_rows": score_null_quality_nonnull_rows,
            "score_nonnull_quality_null_rows": score_nonnull_quality_null_rows,
            "point_high_quality_rows": point_high_quality_rows,
        },
    )


def _add_grid_value_coverage_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    flow_nonnull = int(grid_month["flow_mean_measurement_value"].notna().sum())
    level_nonnull = int(grid_month["level_mean_measurement_value"].notna().sum())

    report.add_check(
        name="gold_grid_month_hydro_value_coverage_nonzero",
        passed=flow_nonnull > 0 and level_nonnull > 0,
        details={
            "row_count": int(len(grid_month)),
            "flow_mean_measurement_value_nonnull": flow_nonnull,
            "level_mean_measurement_value_nonnull": level_nonnull,
            "flow_mean_measurement_value_coverage": flow_nonnull / len(grid_month),
            "level_mean_measurement_value_coverage": level_nonnull / len(grid_month),
        },
    )


def _add_coordinate_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    coordinate_nulls = int(station_month[["latitude", "longitude"]].isna().any(axis=1).sum())
    coordinate_out_of_range = int(
        (
            ~station_month["latitude"].between(
                HYDRO_LATITUDE_MIN,
                HYDRO_LATITUDE_MAX,
                inclusive="both",
            )
            | ~station_month["longitude"].between(
                HYDRO_LONGITUDE_MIN,
                HYDRO_LONGITUDE_MAX,
                inclusive="both",
            )
        ).sum()
    )

    report.add_check(
        name="gold_hydro_station_coordinates_valid",
        passed=coordinate_nulls == 0 and coordinate_out_of_range == 0,
        details={
            "coordinate_null_count": coordinate_nulls,
            "coordinate_out_of_range_count": coordinate_out_of_range,
            "latitude_min": float(station_month["latitude"].min()),
            "latitude_max": float(station_month["latitude"].max()),
            "longitude_min": float(station_month["longitude"].min()),
            "longitude_max": float(station_month["longitude"].max()),
            "allowed_latitude_min": HYDRO_LATITUDE_MIN,
            "allowed_latitude_max": HYDRO_LATITUDE_MAX,
            "allowed_longitude_min": HYDRO_LONGITUDE_MIN,
            "allowed_longitude_max": HYDRO_LONGITUDE_MAX,
        },
    )


def _add_station_count_consistency_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    invalid_rows = int(
        (
            (station_month["daily_record_count"] < 1)
            | (station_month["observation_day_count"] < 1)
            | (station_month["observation_day_count"] > station_month["days_in_month"])
            | (station_month["measurement_observation_count"] > station_month["daily_record_count"])
        ).sum()
    )

    report.add_check(
        name="gold_hydro_station_month_counts_valid",
        passed=invalid_rows == 0,
        details={
            "invalid_count_rows": invalid_rows,
            "minimum_daily_record_count": int(station_month["daily_record_count"].min()),
            "maximum_daily_record_count": int(station_month["daily_record_count"].max()),
        },
    )


def _add_station_ratio_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    _add_required_ratio_check(
        report,
        dataframe=station_month,
        column="measurement_completeness_ratio",
        check_name="gold_hydro_station_month_completeness_ratio_valid",
    )


def _add_station_measurement_value_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    invalid_rows = int(
        (
            station_month[
                [
                    "mean_measurement_value",
                    "min_measurement_value",
                    "max_measurement_value",
                    "median_measurement_value",
                    "p95_measurement_value",
                ]
            ]
            .isna()
            .any(axis=1)
            | (station_month["min_measurement_value"] > station_month["mean_measurement_value"])
            | (station_month["mean_measurement_value"] > station_month["max_measurement_value"])
            | (station_month["min_measurement_value"] > station_month["median_measurement_value"])
            | (station_month["median_measurement_value"] > station_month["max_measurement_value"])
            | (station_month["min_measurement_value"] > station_month["p95_measurement_value"])
            | (station_month["p95_measurement_value"] > station_month["max_measurement_value"])
        ).sum()
    )

    report.add_check(
        name="gold_hydro_station_month_measurement_values_valid",
        passed=invalid_rows == 0,
        details={
            "invalid_value_rows": invalid_rows,
            "minimum_mean_measurement_value": float(station_month["mean_measurement_value"].min()),
            "maximum_mean_measurement_value": float(station_month["mean_measurement_value"].max()),
        },
    )


def _add_station_flow_nonnegative_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    flow_rows = station_month[station_month["measurement_type"] == "flow"]
    level_rows = station_month[station_month["measurement_type"] == "level"]

    flow_negative_rows = int(
        ((flow_rows["negative_value_count"] > 0) | (flow_rows["min_measurement_value"] < 0)).sum()
    )

    level_negative_rows = int((level_rows["negative_value_count"] > 0).sum())

    report.add_check(
        name="gold_hydro_station_month_flow_nonnegative",
        passed=flow_negative_rows == 0,
        details={
            "flow_negative_rows": flow_negative_rows,
            "level_negative_rows_allowed": level_negative_rows,
        },
    )


def _add_station_symbol_count_check(
    report: GoldHydroValidationReport,
    station_month: pd.DataFrame,
) -> None:
    invalid_rows = int(
        (
            (station_month["measurement_symbol_count"] < 0)
            | (station_month["measurement_symbol_count"] > station_month["daily_record_count"])
            | (
                (station_month["estimated_symbol_count"] + station_month["approved_symbol_count"])
                > station_month["measurement_symbol_count"]
            )
        ).sum()
    )

    report.add_check(
        name="gold_hydro_station_month_symbol_counts_valid",
        passed=invalid_rows == 0,
        details={
            "invalid_symbol_count_rows": invalid_rows,
            "measurement_symbol_count_total": int(station_month["measurement_symbol_count"].sum()),
            "estimated_symbol_count_total": int(station_month["estimated_symbol_count"].sum()),
            "approved_symbol_count_total": int(station_month["approved_symbol_count"].sum()),
        },
    )


def _add_required_ratio_check(
    report: GoldHydroValidationReport,
    *,
    dataframe: pd.DataFrame,
    column: str,
    check_name: str,
) -> None:
    null_count = int(dataframe[column].isna().sum())
    out_of_range_count = int((~dataframe[column].between(0, 1, inclusive="both")).sum())

    report.add_check(
        name=check_name,
        passed=null_count == 0 and out_of_range_count == 0,
        details={
            "null_count": null_count,
            "out_of_range_count": out_of_range_count,
            "minimum": float(dataframe[column].min()),
            "maximum": float(dataframe[column].max()),
        },
    )


def _require_station_month_columns(station_month: pd.DataFrame) -> None:
    _require_columns(
        station_month,
        {
            "hydro_station_month_key",
            "province_key",
            "station_id",
            "station_name",
            "measurement_type",
            "reference_month",
            "latitude",
            "longitude",
            "drainage_area_gross",
            "drainage_area_effect",
            "rhbn",
            "real_time",
            "daily_record_count",
            "observation_day_count",
            "measurement_observation_count",
            "days_in_month",
            "measurement_completeness_ratio",
            "mean_measurement_value",
            "min_measurement_value",
            "max_measurement_value",
            "median_measurement_value",
            "p95_measurement_value",
            "measurement_symbol_count",
            "estimated_symbol_count",
            "approved_symbol_count",
            "flow_zero_day_count",
            "negative_value_count",
        },
        "gold_hydro_station_month_feature",
    )


def _require_grid_month_columns(grid_month: pd.DataFrame) -> None:
    _require_columns(
        grid_month,
        {
            "grid_month_hydro_feature_key",
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
            "hydro_spatial_assignment_method",
            "hydro_station_count",
            "hydro_basin_station_count",
            "hydro_point_station_count",
            "hydro_basin_intersection_area_sq_km",
            "hydro_basin_grid_coverage_ratio",
            "flow_station_count",
            "flow_daily_record_count",
            "flow_observation_day_count",
            "flow_measurement_observation_count",
            "flow_mean_measurement_value",
            "flow_min_measurement_value",
            "flow_max_measurement_value",
            "flow_median_measurement_value",
            "flow_p95_measurement_value",
            "flow_measurement_completeness_ratio",
            "flow_zero_day_count",
            "flow_negative_value_count",
            "level_station_count",
            "level_daily_record_count",
            "level_observation_day_count",
            "level_measurement_observation_count",
            "level_mean_measurement_value",
            "level_min_measurement_value",
            "level_max_measurement_value",
            "level_median_measurement_value",
            "level_p95_measurement_value",
            "level_measurement_completeness_ratio",
            "level_negative_value_count",
            "hydro_data_completeness_score",
            "hydro_feature_quality_flag",
        },
        "gold_grid_month_hydro_feature",
    )


def _require_grid_cell_columns(gold_grid_cell: pd.DataFrame) -> None:
    _require_columns(
        gold_grid_cell,
        {
            "grid_cell_key",
            "grid_system",
            "province_key",
            "crs_epsg",
        },
        "gold_grid_cell",
    )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldHydroValidationError(
            f"{table_name} is missing columns: {sorted(missing_columns)}"
        )


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }
