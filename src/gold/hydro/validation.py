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
VALID_QUALITY_FLAGS = {"high", "medium", "low", "very_low"}

HYDRO_LATITUDE_MIN = 48.0
HYDRO_LATITUDE_MAX = 61.0
HYDRO_LONGITUDE_MIN = -140.0
HYDRO_LONGITUDE_MAX = -109.0

MAX_REASONABLE_STATION_GRID_DISTANCE_KM = 50.0


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
        "lakehouse/gold/_validation/" "hydro_monthly_features/latest_validation.json"
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

    report.add_check(
        name="gold_grid_month_hydro_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
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

    _add_measurement_type_check(
        report,
        dataframe=grid_month,
        check_name="gold_grid_month_hydro_measurement_types_valid",
    )

    actual_grid_systems = set(grid_month["grid_system"].dropna().astype(str).unique())

    report.add_check(
        name="gold_grid_month_hydro_grid_systems_valid",
        passed=actual_grid_systems == EXPECTED_GRID_SYSTEMS,
        details={
            "actual": sorted(actual_grid_systems),
            "expected": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": {
                str(key): int(value)
                for key, value in grid_month["grid_system"].value_counts().to_dict().items()
            },
        },
    )

    valid_grid_keys = set(
        gold_grid_cell.loc[
            gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS),
            "grid_cell_key",
        ].astype(str)
    )
    feature_grid_keys = set(grid_month["grid_cell_key"].astype(str))
    unknown_keys = sorted(feature_grid_keys - valid_grid_keys)

    report.add_check(
        name="gold_grid_month_hydro_grid_keys_known",
        passed=len(unknown_keys) == 0,
        details={
            "feature_grid_cell_count": len(feature_grid_keys),
            "valid_10km_grid_cell_count": len(valid_grid_keys),
            "unknown_grid_key_count": len(unknown_keys),
            "unknown_grid_key_sample": unknown_keys[:20],
        },
    )

    station_count_invalid = int((grid_month["station_count"] < 1).sum())

    report.add_check(
        name="gold_grid_month_hydro_station_count_valid",
        passed=station_count_invalid == 0,
        details={
            "invalid_station_count_rows": station_count_invalid,
            "minimum_station_count": int(grid_month["station_count"].min()),
            "maximum_station_count": int(grid_month["station_count"].max()),
        },
    )

    _add_grid_count_consistency_check(report, grid_month)
    _add_grid_ratio_check(report, grid_month)
    _add_grid_measurement_value_check(report, grid_month)
    _add_grid_flow_nonnegative_check(report, grid_month)
    _add_grid_distance_check(report, grid_month)
    _add_grid_quality_flag_check(report, grid_month)


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
            "counts": {
                str(key): int(value)
                for key, value in dataframe["province_key"].value_counts().to_dict().items()
            },
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
            "counts": {
                str(key): int(value)
                for key, value in dataframe["measurement_type"].value_counts().to_dict().items()
            },
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
    _add_ratio_check(
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


def _add_grid_count_consistency_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    invalid_rows = int(
        (
            (grid_month["daily_record_count"] < 1)
            | (grid_month["observation_day_count"] < 1)
            | (grid_month["measurement_observation_count"] > grid_month["daily_record_count"])
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_counts_valid",
        passed=invalid_rows == 0,
        details={
            "invalid_count_rows": invalid_rows,
            "minimum_daily_record_count": int(grid_month["daily_record_count"].min()),
            "maximum_daily_record_count": int(grid_month["daily_record_count"].max()),
        },
    )


def _add_grid_ratio_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    _add_ratio_check(
        report,
        dataframe=grid_month,
        column="mean_measurement_completeness_ratio",
        check_name="gold_grid_month_hydro_completeness_ratio_valid",
    )


def _add_grid_measurement_value_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    invalid_rows = int(
        (
            grid_month[
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
            | (grid_month["min_measurement_value"] > grid_month["mean_measurement_value"])
            | (grid_month["mean_measurement_value"] > grid_month["max_measurement_value"])
            | (grid_month["min_measurement_value"] > grid_month["median_measurement_value"])
            | (grid_month["median_measurement_value"] > grid_month["max_measurement_value"])
            | (grid_month["min_measurement_value"] > grid_month["p95_measurement_value"])
            | (grid_month["p95_measurement_value"] > grid_month["max_measurement_value"])
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_measurement_values_valid",
        passed=invalid_rows == 0,
        details={
            "invalid_value_rows": invalid_rows,
            "minimum_mean_measurement_value": float(grid_month["mean_measurement_value"].min()),
            "maximum_mean_measurement_value": float(grid_month["mean_measurement_value"].max()),
        },
    )


def _add_grid_flow_nonnegative_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    flow_rows = grid_month[grid_month["measurement_type"] == "flow"]
    level_rows = grid_month[grid_month["measurement_type"] == "level"]

    flow_negative_rows = int(
        ((flow_rows["negative_value_count"] > 0) | (flow_rows["min_measurement_value"] < 0)).sum()
    )

    level_negative_rows = int((level_rows["negative_value_count"] > 0).sum())

    report.add_check(
        name="gold_grid_month_hydro_flow_nonnegative",
        passed=flow_negative_rows == 0,
        details={
            "flow_negative_rows": flow_negative_rows,
            "level_negative_rows_allowed": level_negative_rows,
        },
    )


def _add_grid_distance_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    maximum_nearest_distance = float(grid_month["nearest_station_distance_km"].max())
    maximum_mean_distance = float(grid_month["mean_station_distance_km"].max())

    invalid_distance_rows = int(
        (
            (grid_month["nearest_station_distance_km"] < 0)
            | (grid_month["mean_station_distance_km"] < 0)
        ).sum()
    )

    excessive_distance_rows = int(
        (
            (grid_month["nearest_station_distance_km"] > MAX_REASONABLE_STATION_GRID_DISTANCE_KM)
            | (grid_month["mean_station_distance_km"] > MAX_REASONABLE_STATION_GRID_DISTANCE_KM)
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_hydro_distance_valid",
        passed=invalid_distance_rows == 0 and excessive_distance_rows == 0,
        details={
            "invalid_distance_rows": invalid_distance_rows,
            "excessive_distance_rows": excessive_distance_rows,
            "maximum_nearest_station_distance_km": maximum_nearest_distance,
            "maximum_mean_station_distance_km": maximum_mean_distance,
            "maximum_allowed_station_grid_distance_km": (MAX_REASONABLE_STATION_GRID_DISTANCE_KM),
        },
    )


def _add_grid_quality_flag_check(
    report: GoldHydroValidationReport,
    grid_month: pd.DataFrame,
) -> None:
    actual_quality_flags = set(
        grid_month["hydro_feature_quality_flag"].dropna().astype(str).unique()
    )

    report.add_check(
        name="gold_grid_month_hydro_quality_flags_valid",
        passed=actual_quality_flags <= VALID_QUALITY_FLAGS,
        details={
            "actual": sorted(actual_quality_flags),
            "expected_allowed": sorted(VALID_QUALITY_FLAGS),
            "counts": {
                str(key): int(value)
                for key, value in grid_month["hydro_feature_quality_flag"]
                .value_counts(dropna=False)
                .to_dict()
                .items()
            },
        },
    )


def _add_ratio_check(
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
            "measurement_type",
            "reference_month",
            "station_count",
            "daily_record_count",
            "observation_day_count",
            "measurement_observation_count",
            "mean_measurement_value",
            "min_measurement_value",
            "max_measurement_value",
            "median_measurement_value",
            "p95_measurement_value",
            "mean_measurement_completeness_ratio",
            "flow_zero_day_count",
            "negative_value_count",
            "nearest_station_distance_km",
            "mean_station_distance_km",
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
