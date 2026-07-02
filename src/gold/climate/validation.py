from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet


EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
EXPECTED_MONTH_MIN = "2016-01"
EXPECTED_MONTH_MAX = "2025-12"
EXPECTED_MONTH_COUNT = 120
TARGET_CRS_EPSG = 3347
IDW_RADIUS_KM = 150.0
RATIO_TOLERANCE = 1e-9

CLIMATE_LATITUDE_MIN = 48.0
CLIMATE_LATITUDE_MAX = 61.0
CLIMATE_LONGITUDE_MIN = -140.0
CLIMATE_LONGITUDE_MAX = -109.0

CLIMATE_MAPPING_METHODS = {
    "direct_station_in_cell",
    "direct_station_average_in_cell",
    "idw_interpolated",
    "no_station_within_radius",
}

CLIMATE_QUALITY_FLAGS = {
    "direct",
    "high",
    "medium",
    "low",
    "very_low",
}

CLIMATE_VALUE_COLUMNS = [
    "daily_record_count",
    "temperature_observation_count",
    "precipitation_observation_count",
    "mean_temp_c",
    "min_temp_c",
    "max_temp_c",
    "observed_min_temp_c",
    "observed_max_temp_c",
    "total_precip_mm",
    "total_rain_mm",
    "total_snow",
    "precipitation_days",
    "heavy_precipitation_days",
    "extreme_heat_days",
    "extreme_cold_days",
    "freeze_thaw_days",
    "temperature_completeness_ratio",
    "precipitation_completeness_ratio",
]


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
    expected_month_min: str = EXPECTED_MONTH_MIN,
    expected_month_max: str = EXPECTED_MONTH_MAX,
    expected_month_count: int = EXPECTED_MONTH_COUNT,
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
            "climate_mapping_method",
            "climate_station_count",
            "climate_nearest_station_distance_km",
            "climate_mean_station_distance_km",
            "climate_max_station_distance_km",
            "climate_idw_confidence_score",
            *CLIMATE_VALUE_COLUMNS,
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

    _validate_station_month(
        report=report,
        station_month=station_month,
        expected_month_min=expected_month_min,
        expected_month_max=expected_month_max,
        expected_month_count=expected_month_count,
    )
    _validate_grid_month(
        report=report,
        grid_month=grid_month,
        gold_grid_cell=gold_grid_cell,
        expected_month_min=expected_month_min,
        expected_month_max=expected_month_max,
        expected_month_count=expected_month_count,
    )

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
    *,
    report: GoldClimateValidationReport,
    station_month: pd.DataFrame,
    expected_month_min: str,
    expected_month_max: str,
    expected_month_count: int,
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
            len(months) == expected_month_count
            and months[0] == expected_month_min
            and months[-1] == expected_month_max
        ),
        details={
            "month_count": len(months),
            "minimum_month": months[0] if months else None,
            "maximum_month": months[-1] if months else None,
            "expected_month_count": expected_month_count,
            "expected_minimum_month": expected_month_min,
            "expected_maximum_month": expected_month_max,
        },
    )

    province_values = sorted(station_month["province_key"].dropna().unique().tolist())

    report.add_check(
        name="gold_climate_station_month_provinces_valid",
        passed=set(province_values) <= {"AB", "BC"} and bool(province_values),
        details={
            "actual": province_values,
            "expected_allowed": ["AB", "BC"],
            "counts": {
                str(key): int(value)
                for key, value in station_month["province_key"].value_counts().to_dict().items()
            },
        },
    )

    coordinate_nulls = int(station_month[["latitude", "longitude"]].isna().any(axis=1).sum())
    coordinate_out_of_range = int(
        (
            ~station_month["latitude"].between(
                CLIMATE_LATITUDE_MIN,
                CLIMATE_LATITUDE_MAX,
                inclusive="both",
            )
            | ~station_month["longitude"].between(
                CLIMATE_LONGITUDE_MIN,
                CLIMATE_LONGITUDE_MAX,
                inclusive="both",
            )
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
            "allowed_latitude_min": CLIMATE_LATITUDE_MIN,
            "allowed_latitude_max": CLIMATE_LATITUDE_MAX,
            "allowed_longitude_min": CLIMATE_LONGITUDE_MIN,
            "allowed_longitude_max": CLIMATE_LONGITUDE_MAX,
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
        allow_null=False,
    )


def _validate_grid_month(
    *,
    report: GoldClimateValidationReport,
    grid_month: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
    expected_month_min: str,
    expected_month_max: str,
    expected_month_count: int,
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

    target_grid = gold_grid_cell[gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS)].copy()
    valid_grid_keys = set(target_grid["grid_cell_key"].astype(str))
    feature_grid_keys = set(grid_month["grid_cell_key"].astype(str))
    unknown_keys = sorted(feature_grid_keys - valid_grid_keys)
    missing_keys = sorted(valid_grid_keys - feature_grid_keys)

    months = sorted(grid_month["reference_month"].dropna().unique().tolist())
    expected_row_count = len(valid_grid_keys) * expected_month_count

    report.add_check(
        name="gold_grid_month_climate_complete_skeleton",
        passed=(
            len(grid_month) == expected_row_count
            and len(feature_grid_keys) == len(valid_grid_keys)
            and not unknown_keys
            and not missing_keys
            and len(months) == expected_month_count
        ),
        details={
            "row_count": int(len(grid_month)),
            "expected_row_count": int(expected_row_count),
            "feature_grid_cell_count": len(feature_grid_keys),
            "valid_10km_grid_cell_count": len(valid_grid_keys),
            "unknown_grid_key_count": len(unknown_keys),
            "missing_grid_key_count": len(missing_keys),
            "unknown_grid_key_sample": unknown_keys[:20],
            "missing_grid_key_sample": missing_keys[:20],
            "month_count": len(months),
            "expected_month_count": expected_month_count,
        },
    )

    actual_systems = set(grid_month["grid_system"].dropna().unique())

    report.add_check(
        name="gold_grid_month_climate_grid_systems_valid",
        passed=actual_systems <= EXPECTED_GRID_SYSTEMS and bool(actual_systems),
        details={
            "actual": sorted(actual_systems),
            "expected_allowed": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": {
                str(key): int(value)
                for key, value in grid_month["grid_system"].value_counts().to_dict().items()
            },
        },
    )

    report.add_check(
        name="gold_grid_month_climate_month_range_valid",
        passed=(
            len(months) == expected_month_count
            and months[0] == expected_month_min
            and months[-1] == expected_month_max
        ),
        details={
            "month_count": len(months),
            "minimum_month": months[0] if months else None,
            "maximum_month": months[-1] if months else None,
            "expected_month_count": expected_month_count,
            "expected_minimum_month": expected_month_min,
            "expected_maximum_month": expected_month_max,
        },
    )

    crs_values = sorted(target_grid["crs_epsg"].dropna().unique().tolist())

    report.add_check(
        name="gold_grid_month_climate_grid_crs_valid",
        passed=crs_values == [TARGET_CRS_EPSG],
        details={
            "actual_crs_values": crs_values,
            "expected_crs_epsg": TARGET_CRS_EPSG,
        },
    )

    actual_methods = set(grid_month["climate_mapping_method"].dropna().unique())
    unexpected_methods = sorted(actual_methods - CLIMATE_MAPPING_METHODS)

    report.add_check(
        name="gold_grid_month_climate_mapping_methods_valid",
        passed=not unexpected_methods,
        details={
            "actual": sorted(actual_methods),
            "expected_allowed": sorted(CLIMATE_MAPPING_METHODS),
            "unexpected_methods": unexpected_methods,
            "counts": {
                str(key): int(value)
                for key, value in grid_month["climate_mapping_method"]
                .value_counts(dropna=False)
                .to_dict()
                .items()
            },
        },
    )

    direct_mask = grid_month["climate_mapping_method"].isin(
        ["direct_station_in_cell", "direct_station_average_in_cell"]
    )
    idw_mask = grid_month["climate_mapping_method"] == "idw_interpolated"
    no_station_mask = grid_month["climate_mapping_method"] == "no_station_within_radius"
    has_climate_value_mask = direct_mask | idw_mask

    station_count_invalid = int(
        (
            (direct_mask & (grid_month["climate_station_count"] < 1))
            | (idw_mask & (grid_month["climate_station_count"] < 1))
            | (no_station_mask & (grid_month["climate_station_count"] != 0))
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_climate_station_count_valid",
        passed=station_count_invalid == 0,
        details={
            "invalid_station_count_rows": station_count_invalid,
            "minimum_station_count": int(grid_month["climate_station_count"].min()),
            "maximum_station_count": int(grid_month["climate_station_count"].max()),
        },
    )

    direct_distance_invalid = int(
        (
            direct_mask
            & (
                grid_month["climate_nearest_station_distance_km"].ne(0)
                | grid_month["climate_mean_station_distance_km"].ne(0)
                | grid_month["climate_max_station_distance_km"].ne(0)
            )
        ).sum()
    )
    idw_distance_invalid = int(
        (
            idw_mask
            & (
                grid_month["climate_nearest_station_distance_km"].isna()
                | grid_month["climate_mean_station_distance_km"].isna()
                | grid_month["climate_max_station_distance_km"].isna()
                | (grid_month["climate_nearest_station_distance_km"] < 0)
                | (grid_month["climate_mean_station_distance_km"] < 0)
                | (grid_month["climate_max_station_distance_km"] < 0)
                | (grid_month["climate_nearest_station_distance_km"] > IDW_RADIUS_KM)
                | (grid_month["climate_mean_station_distance_km"] > IDW_RADIUS_KM)
                | (grid_month["climate_max_station_distance_km"] > IDW_RADIUS_KM)
                | (
                    grid_month["climate_nearest_station_distance_km"]
                    > grid_month["climate_mean_station_distance_km"]
                )
                | (
                    grid_month["climate_mean_station_distance_km"]
                    > grid_month["climate_max_station_distance_km"]
                )
            )
        ).sum()
    )
    no_station_distance_invalid = int(
        (
            no_station_mask
            & (
                grid_month["climate_nearest_station_distance_km"].notna()
                | grid_month["climate_mean_station_distance_km"].notna()
                | grid_month["climate_max_station_distance_km"].notna()
            )
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_climate_distance_valid",
        passed=(
            direct_distance_invalid == 0
            and idw_distance_invalid == 0
            and no_station_distance_invalid == 0
        ),
        details={
            "direct_distance_invalid_rows": direct_distance_invalid,
            "idw_distance_invalid_rows": idw_distance_invalid,
            "no_station_distance_invalid_rows": no_station_distance_invalid,
            "idw_radius_km": IDW_RADIUS_KM,
        },
    )

    confidence_invalid = int(
        (
            (grid_month["climate_idw_confidence_score"] < 0)
            | (grid_month["climate_idw_confidence_score"] > 1)
            | (direct_mask & grid_month["climate_idw_confidence_score"].ne(1.0))
            | (idw_mask & grid_month["climate_idw_confidence_score"].le(0))
            | (no_station_mask & grid_month["climate_idw_confidence_score"].ne(0.0))
        ).sum()
    )

    report.add_check(
        name="gold_grid_month_climate_confidence_valid",
        passed=confidence_invalid == 0,
        details={
            "invalid_confidence_rows": confidence_invalid,
            "minimum_confidence": float(grid_month["climate_idw_confidence_score"].min()),
            "maximum_confidence": float(grid_month["climate_idw_confidence_score"].max()),
        },
    )

    no_station_nonnull_value_counts = {
        column: int(grid_month.loc[no_station_mask, column].notna().sum())
        for column in [*CLIMATE_VALUE_COLUMNS, "climate_data_completeness_score"]
    }

    report.add_check(
        name="gold_grid_month_climate_no_station_null_semantics",
        passed=sum(no_station_nonnull_value_counts.values()) == 0,
        details={
            "nonnull_counts_for_no_station_rows": no_station_nonnull_value_counts,
            "no_station_row_count": int(no_station_mask.sum()),
        },
    )

    mapped_completeness_null_count = int(
        grid_month.loc[
            has_climate_value_mask,
            "climate_data_completeness_score",
        ]
        .isna()
        .sum()
    )

    report.add_check(
        name="gold_grid_month_climate_mapped_completeness_present",
        passed=mapped_completeness_null_count == 0,
        details={
            "mapped_completeness_null_count": mapped_completeness_null_count,
            "mapped_grid_month_count": int(has_climate_value_mask.sum()),
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
            "climate_idw_confidence_score",
        ],
        allow_null=True,
    )

    actual_quality_flags = set(grid_month["climate_feature_quality_flag"].dropna().unique())
    unexpected_quality_flags = sorted(actual_quality_flags - CLIMATE_QUALITY_FLAGS)

    direct_quality_invalid = int(
        (direct_mask & grid_month["climate_feature_quality_flag"].ne("direct")).sum()
    )
    idw_quality_invalid = int(
        (
            idw_mask
            & ~grid_month["climate_feature_quality_flag"].isin(
                ["high", "medium", "low", "very_low"]
            )
        ).sum()
    )
    no_station_quality_invalid = int(
        (no_station_mask & grid_month["climate_feature_quality_flag"].notna()).sum()
    )

    report.add_check(
        name="gold_grid_month_climate_quality_flags_valid",
        passed=(
            not unexpected_quality_flags
            and direct_quality_invalid == 0
            and idw_quality_invalid == 0
            and no_station_quality_invalid == 0
        ),
        details={
            "actual": sorted(actual_quality_flags),
            "expected_allowed_non_null": sorted(CLIMATE_QUALITY_FLAGS),
            "unexpected_quality_flags": unexpected_quality_flags,
            "direct_quality_invalid_rows": direct_quality_invalid,
            "idw_quality_invalid_rows": idw_quality_invalid,
            "no_station_quality_invalid_rows": no_station_quality_invalid,
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
    allow_null: bool,
) -> None:
    details = {}
    passed = True

    for column in columns:
        non_null = dataframe[column].dropna()
        null_count = int(dataframe[column].isna().sum())
        out_of_range_count = int(
            ((non_null < -RATIO_TOLERANCE) | (non_null > 1 + RATIO_TOLERANCE)).sum()
        )

        details[column] = {
            "null_count": null_count,
            "out_of_range_count": out_of_range_count,
            "minimum": float(non_null.min()) if not non_null.empty else None,
            "maximum": float(non_null.max()) if not non_null.empty else None,
        }

        if out_of_range_count > 0 or (null_count > 0 and not allow_null):
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
