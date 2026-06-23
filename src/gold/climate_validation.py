from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.validation import latest_table_parquet


EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
EXPECTED_MONTH_MIN = "2016-01"
EXPECTED_MONTH_MAX = "2025-12"
EXPECTED_MONTH_COUNT = 120


class GoldClimateValidationError(Exception):
    """Raised when Gold climate validation cannot be executed."""


@dataclass(frozen=True)
class GoldClimateValidationCheck:
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
class GoldClimateValidationReport:
    validation_name: str
    checks: list[GoldClimateValidationCheck] = field(default_factory=list)
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
            GoldClimateValidationCheck(
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


def validate_climate_monthly_feature_dataframes(
    *,
    station_month: pd.DataFrame,
    grid_month: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
) -> GoldClimateValidationReport:
    report = GoldClimateValidationReport(validation_name="gold_climate_monthly_feature_validation")

    _require_columns(
        station_month,
        {
            "climate_station_month_key",
            "station_id",
            "station_name",
            "province_key",
            "reference_month",
            "latitude",
            "longitude",
            "daily_record_count",
            "temperature_observation_count",
            "precipitation_observation_count",
            "total_precip_mm",
            "total_rain_mm",
            "total_snow",
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
        },
        "gold_climate_station_month_feature",
    )

    _require_columns(
        grid_month,
        {
            "grid_month_climate_feature_key",
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
            "station_count",
            "nearest_station_distance_km",
            "mean_station_distance_km",
            "total_precip_mm",
            "total_rain_mm",
            "total_snow",
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
            "climate_data_completeness_score",
            "climate_feature_quality_flag",
        },
        "gold_grid_month_climate_feature",
    )

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

    _validate_station_month(report, station_month)
    _validate_grid_month(report, grid_month, gold_grid_cell)

    return report


def validate_climate_monthly_feature_outputs(
    *,
    gold_root: str | Path = "lakehouse/gold",
    output_json_path: str | Path = (
        "lakehouse/gold/_validation/" "climate_monthly_features/latest_validation.json"
    ),
) -> GoldClimateValidationReport:
    station_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_climate_station_month_feature",
    )
    grid_month_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_climate_feature",
    )
    grid_cell_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )

    report = validate_climate_monthly_feature_dataframes(
        station_month=pd.read_parquet(station_path),
        grid_month=pd.read_parquet(grid_month_path),
        gold_grid_cell=pd.read_parquet(grid_cell_path),
    )

    report.output_paths = {
        "gold_climate_station_month_feature": station_path.as_posix(),
        "gold_grid_month_climate_feature": grid_month_path.as_posix(),
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
    report: GoldClimateValidationReport,
    station_month: pd.DataFrame,
) -> None:
    row_count = len(station_month)

    report.add_check(
        name="gold_climate_station_month_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
    )

    key_nulls = int(station_month["climate_station_month_key"].isna().sum())
    key_duplicates = int(station_month["climate_station_month_key"].duplicated().sum())

    report.add_check(
        name="gold_climate_station_month_key_valid",
        passed=key_nulls == 0 and key_duplicates == 0,
        details={
            "null_count": key_nulls,
            "duplicate_count": key_duplicates,
        },
    )

    months = sorted(station_month["reference_month"].dropna().unique().tolist())

    report.add_check(
        name="gold_climate_station_month_range_valid",
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

    province_values = sorted(station_month["province_key"].dropna().unique().tolist())

    report.add_check(
        name="gold_climate_station_month_provinces_valid",
        passed=set(province_values) == {"AB", "BC"},
        details={
            "actual": province_values,
            "expected": ["AB", "BC"],
            "counts": {
                str(key): int(value)
                for key, value in station_month["province_key"].value_counts().to_dict().items()
            },
        },
    )

    coordinate_nulls = int(station_month[["latitude", "longitude"]].isna().any(axis=1).sum())
    coordinate_out_of_range = int(
        (
            ~station_month["latitude"].between(48, 60, inclusive="both")
            | ~station_month["longitude"].between(-140, -110, inclusive="both")
        ).sum()
    )

    report.add_check(
        name="gold_climate_station_coordinates_valid",
        passed=coordinate_nulls == 0 and coordinate_out_of_range == 0,
        details={
            "coordinate_null_count": coordinate_nulls,
            "coordinate_out_of_range_count": coordinate_out_of_range,
            "latitude_min": float(station_month["latitude"].min()),
            "latitude_max": float(station_month["latitude"].max()),
            "longitude_min": float(station_month["longitude"].min()),
            "longitude_max": float(station_month["longitude"].max()),
        },
    )

    _add_nonnegative_check(
        report,
        dataframe=station_month,
        check_name="gold_climate_station_month_precipitation_nonnegative",
        columns=["total_precip_mm", "total_rain_mm", "total_snow"],
    )

    _add_ratio_check(
        report,
        dataframe=station_month,
        check_name="gold_climate_station_month_completeness_ratios_valid",
        columns=[
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
        ],
    )


def _validate_grid_month(
    report: GoldClimateValidationReport,
    grid_month: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
) -> None:
    row_count = len(grid_month)

    report.add_check(
        name="gold_grid_month_climate_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
    )

    key_nulls = int(grid_month["grid_month_climate_feature_key"].isna().sum())
    key_duplicates = int(grid_month["grid_month_climate_feature_key"].duplicated().sum())

    report.add_check(
        name="gold_grid_month_climate_key_valid",
        passed=key_nulls == 0 and key_duplicates == 0,
        details={
            "null_count": key_nulls,
            "duplicate_count": key_duplicates,
        },
    )

    actual_systems = set(grid_month["grid_system"].dropna().unique())

    report.add_check(
        name="gold_grid_month_climate_grid_systems_valid",
        passed=actual_systems == EXPECTED_GRID_SYSTEMS,
        details={
            "actual": sorted(actual_systems),
            "expected": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": {
                str(key): int(value)
                for key, value in grid_month["grid_system"].value_counts().to_dict().items()
            },
        },
    )

    months = sorted(grid_month["reference_month"].dropna().unique().tolist())

    report.add_check(
        name="gold_grid_month_climate_month_range_valid",
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

    valid_grid_keys = set(
        gold_grid_cell.loc[
            gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS),
            "grid_cell_key",
        ].astype(str)
    )
    feature_grid_keys = set(grid_month["grid_cell_key"].astype(str))

    unknown_keys = sorted(feature_grid_keys - valid_grid_keys)

    report.add_check(
        name="gold_grid_month_climate_grid_keys_known",
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
        name="gold_grid_month_climate_station_count_valid",
        passed=station_count_invalid == 0,
        details={
            "invalid_station_count_rows": station_count_invalid,
            "minimum_station_count": int(grid_month["station_count"].min()),
            "maximum_station_count": int(grid_month["station_count"].max()),
        },
    )

    distance_invalid = int(
        (
            (grid_month["nearest_station_distance_km"] < 0)
            | (grid_month["mean_station_distance_km"] < 0)
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_climate_distance_valid",
        passed=distance_invalid == 0,
        details={
            "invalid_distance_rows": distance_invalid,
            "maximum_nearest_station_distance_km": float(
                grid_month["nearest_station_distance_km"].max()
            ),
            "maximum_mean_station_distance_km": float(grid_month["mean_station_distance_km"].max()),
        },
    )

    _add_nonnegative_check(
        report,
        dataframe=grid_month,
        check_name="gold_grid_month_climate_precipitation_nonnegative",
        columns=["total_precip_mm", "total_rain_mm", "total_snow"],
    )

    _add_ratio_check(
        report,
        dataframe=grid_month,
        check_name="gold_grid_month_climate_completeness_ratios_valid",
        columns=[
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
            "climate_data_completeness_score",
        ],
    )

    valid_quality_flags = {"high", "medium", "low", "very_low"}
    actual_quality_flags = set(grid_month["climate_feature_quality_flag"].dropna().unique())

    report.add_check(
        name="gold_grid_month_climate_quality_flags_valid",
        passed=actual_quality_flags <= valid_quality_flags,
        details={
            "actual": sorted(actual_quality_flags),
            "expected_allowed": sorted(valid_quality_flags),
            "counts": {
                str(key): int(value)
                for key, value in grid_month["climate_feature_quality_flag"]
                .value_counts(dropna=False)
                .to_dict()
                .items()
            },
        },
    )


def _add_nonnegative_check(
    report: GoldClimateValidationReport,
    *,
    dataframe: pd.DataFrame,
    check_name: str,
    columns: list[str],
) -> None:
    details = {}
    passed = True

    for column in columns:
        negative_count = int((dataframe[column].dropna() < 0).sum())
        null_count = int(dataframe[column].isna().sum())

        details[column] = {
            "negative_count": negative_count,
            "null_count": null_count,
        }

        if negative_count > 0:
            passed = False

    report.add_check(
        name=check_name,
        passed=passed,
        details=details,
    )


def _add_ratio_check(
    report: GoldClimateValidationReport,
    *,
    dataframe: pd.DataFrame,
    check_name: str,
    columns: list[str],
) -> None:
    details = {}
    passed = True

    for column in columns:
        null_count = int(dataframe[column].isna().sum())
        out_of_range_count = int((~dataframe[column].between(0, 1, inclusive="both")).sum())

        details[column] = {
            "null_count": null_count,
            "out_of_range_count": out_of_range_count,
            "minimum": float(dataframe[column].min()),
            "maximum": float(dataframe[column].max()),
        }

        if null_count > 0 or out_of_range_count > 0:
            passed = False

    report.add_check(
        name=check_name,
        passed=passed,
        details=details,
    )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldClimateValidationError(
            f"{table_name} is missing columns: {sorted(missing_columns)}"
        )
