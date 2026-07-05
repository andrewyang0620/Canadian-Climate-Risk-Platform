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

EXPECTED_CLIMATE_MAPPING_METHODS = {
    "direct_station_in_cell",
    "direct_station_average_in_cell",
    "idw_interpolated",
    "no_station_within_radius",
}
EXPECTED_HYDRO_SPATIAL_METHODS = {
    "basin_polygon_intersection",
    "station_point_in_cell",
    "no_hydro_coverage",
}
EXPECTED_WILDFIRE_TEMPORAL_METHODS = {
    "polygon_fire_month",
    "no_observed_perimeter_overlap",
}

VALID_CLIMATE_QUALITY_FLAGS = {"direct", "high", "medium", "low", "very_low"}
VALID_HYDRO_QUALITY_FLAGS = {"high", "medium", "low", "very_low"}

NO_CLIMATE_COVERAGE_METHOD = "no_station_within_radius"
NO_HYDRO_COVERAGE_METHOD = "no_hydro_coverage"
NO_WILDFIRE_OVERLAP_METHOD = "no_observed_perimeter_overlap"

CLIMATE_VALUE_COLUMNS = [
    "climate_mean_temp_c",
    "climate_min_temp_c",
    "climate_max_temp_c",
    "climate_observed_min_temp_c",
    "climate_observed_max_temp_c",
    "climate_total_precip_mm",
    "climate_total_rain_mm",
    "climate_total_snow",
    "climate_precipitation_days",
    "climate_heavy_precipitation_days",
    "climate_extreme_heat_days",
    "climate_extreme_cold_days",
    "climate_freeze_thaw_days",
]

HYDRO_FLOW_VALUE_COLUMNS = [
    "flow_mean_measurement_value",
    "flow_min_measurement_value",
    "flow_max_measurement_value",
    "flow_median_measurement_value",
    "flow_p95_measurement_value",
]

HYDRO_LEVEL_VALUE_COLUMNS = [
    "level_mean_measurement_value",
    "level_min_measurement_value",
    "level_max_measurement_value",
    "level_median_measurement_value",
    "level_p95_measurement_value",
]

WILDFIRE_VALUE_COLUMNS = [
    "wildfire_perimeter_count",
    "wildfire_intersection_area_sq_km",
    "wildfire_intersection_area_ha",
    "wildfire_intersection_area_ratio_of_grid",
    "wildfire_max_source_size_ha",
    "wildfire_max_calculated_size_ha",
    "wildfire_cause_n_polygon_count",
    "wildfire_cause_h_polygon_count",
    "wildfire_cause_u_polygon_count",
    "wildfire_cause_prescribed_burn_polygon_count",
    "wildfire_cause_other_polygon_count",
]


class GoldRiskMartValidationError(Exception):
    """Raised when Gold risk mart validation cannot be executed."""


@dataclass(frozen=True)
class GoldRiskMartValidationCheck:
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
class GoldRiskMartValidationReport:
    validation_name: str
    checks: list[GoldRiskMartValidationCheck] = field(default_factory=list)
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
            GoldRiskMartValidationCheck(
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


def validate_risk_monthly_grid_dataframes(
    *,
    mart: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
    wildfire_grid_month: pd.DataFrame,
) -> GoldRiskMartValidationReport:
    report = GoldRiskMartValidationReport(
        validation_name="gold_grid_month_risk_feature_mart_validation"
    )

    _require_mart_columns(mart)
    _require_grid_columns(gold_grid_cell)
    _require_climate_columns(climate_grid_month)
    _require_hydro_columns(hydro_grid_month)
    _require_wildfire_columns(wildfire_grid_month)

    _add_row_count_check(report, mart, gold_grid_cell)
    _add_key_check(report, mart)
    _add_grid_system_check(report, mart)
    _add_month_range_check(report, mart)
    _add_grid_key_coverage_check(report, mart, gold_grid_cell)
    _add_primary_municipality_check(report, mart)

    _add_source_grain_checks(
        report,
        climate_grid_month=climate_grid_month,
        hydro_grid_month=hydro_grid_month,
        wildfire_grid_month=wildfire_grid_month,
    )

    _add_climate_method_and_semantics_checks(
        report,
        mart=mart,
        climate_grid_month=climate_grid_month,
    )
    _add_hydro_method_and_semantics_checks(
        report,
        mart=mart,
        hydro_grid_month=hydro_grid_month,
    )
    _add_wildfire_method_and_semantics_checks(
        report,
        mart=mart,
        wildfire_grid_month=wildfire_grid_month,
    )

    _add_feature_flag_checks(report, mart)
    _add_quality_flag_checks(report, mart)
    _add_ratio_checks(report, mart)
    _add_value_coverage_check(report, mart)

    return report


def validate_risk_monthly_grid_outputs(
    *,
    gold_root: str | Path = "lakehouse/gold",
    output_json_path: str | Path = (
        "lakehouse/gold/_validation/risk_monthly_grid_mart/latest_validation.json"
    ),
) -> GoldRiskMartValidationReport:
    mart_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_risk_feature_mart",
    )
    grid_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )
    climate_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_climate_feature",
    )
    hydro_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_hydro_feature",
    )
    wildfire_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_month_wildfire_perimeter_feature",
    )

    report = validate_risk_monthly_grid_dataframes(
        mart=pd.read_parquet(mart_path),
        gold_grid_cell=pd.read_parquet(grid_path),
        climate_grid_month=pd.read_parquet(climate_path),
        hydro_grid_month=pd.read_parquet(hydro_path),
        wildfire_grid_month=pd.read_parquet(wildfire_path),
    )

    report.output_paths = {
        "gold_grid_month_risk_feature_mart": mart_path.as_posix(),
        "gold_grid_cell": grid_path.as_posix(),
        "gold_grid_month_climate_feature": climate_path.as_posix(),
        "gold_grid_month_hydro_feature": hydro_path.as_posix(),
        "gold_grid_month_wildfire_perimeter_feature": wildfire_path.as_posix(),
    }

    final_output_path = Path(output_json_path)
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    final_output_path.write_text(
        json.dumps(report.to_dict(), indent=2),
        encoding="utf-8",
    )

    return report


def _add_row_count_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
) -> None:
    expected_grid_count = int(
        gold_grid_cell.loc[
            gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS),
            "grid_cell_key",
        ].nunique()
    )
    expected_row_count = expected_grid_count * EXPECTED_MONTH_COUNT

    report.add_check(
        name="gold_risk_mart_row_count_matches_grid_month_skeleton",
        passed=len(mart) == expected_row_count,
        details={
            "row_count": int(len(mart)),
            "expected_row_count": expected_row_count,
            "expected_grid_count": expected_grid_count,
            "expected_month_count": EXPECTED_MONTH_COUNT,
        },
    )


def _add_key_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    null_count = int(mart["grid_month_risk_feature_key"].isna().sum())
    duplicate_count = int(mart["grid_month_risk_feature_key"].duplicated().sum())
    grain_duplicate_count = int(
        mart[["grid_cell_key", "reference_month"]].duplicated().sum()
    )

    report.add_check(
        name="gold_risk_mart_key_valid",
        passed=null_count == 0 and duplicate_count == 0 and grain_duplicate_count == 0,
        details={
            "null_count": null_count,
            "duplicate_key_count": duplicate_count,
            "duplicate_grid_month_count": grain_duplicate_count,
            "unique_key_count": int(mart["grid_month_risk_feature_key"].nunique()),
        },
    )


def _add_grid_system_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    actual_grid_systems = set(mart["grid_system"].dropna().astype(str).unique())

    report.add_check(
        name="gold_risk_mart_grid_systems_valid",
        passed=actual_grid_systems == EXPECTED_GRID_SYSTEMS,
        details={
            "actual": sorted(actual_grid_systems),
            "expected": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": _value_counts(mart["grid_system"]),
        },
    )


def _add_month_range_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    months = sorted(mart["reference_month"].dropna().astype(str).unique().tolist())

    report.add_check(
        name="gold_risk_mart_month_range_valid",
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


def _add_grid_key_coverage_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
    gold_grid_cell: pd.DataFrame,
) -> None:
    expected_grid_keys = set(
        gold_grid_cell.loc[
            gold_grid_cell["grid_system"].isin(EXPECTED_GRID_SYSTEMS),
            "grid_cell_key",
        ].astype(str)
    )
    actual_grid_keys = set(mart["grid_cell_key"].astype(str))

    missing_grid_keys = sorted(expected_grid_keys - actual_grid_keys)
    unexpected_grid_keys = sorted(actual_grid_keys - expected_grid_keys)

    rows_per_grid = mart.groupby("grid_cell_key")["reference_month"].nunique()
    invalid_month_count_grids = rows_per_grid[rows_per_grid != EXPECTED_MONTH_COUNT]

    report.add_check(
        name="gold_risk_mart_grid_key_coverage_valid",
        passed=(
            len(missing_grid_keys) == 0
            and len(unexpected_grid_keys) == 0
            and len(invalid_month_count_grids) == 0
        ),
        details={
            "expected_grid_cell_count": len(expected_grid_keys),
            "actual_grid_cell_count": len(actual_grid_keys),
            "missing_grid_key_count": len(missing_grid_keys),
            "unexpected_grid_key_count": len(unexpected_grid_keys),
            "invalid_month_count_grid_count": int(len(invalid_month_count_grids)),
            "missing_grid_key_sample": missing_grid_keys[:20],
            "unexpected_grid_key_sample": unexpected_grid_keys[:20],
        },
    )


def _add_primary_municipality_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    primary_null_rows = int(mart["primary_municipality_key"].isna().sum())
    match_count_invalid_rows = int((mart["municipality_match_count"] < 1).sum())

    report.add_check(
        name="gold_risk_mart_primary_municipality_valid",
        passed=primary_null_rows == 0 and match_count_invalid_rows == 0,
        details={
            "primary_municipality_null_rows": primary_null_rows,
            "municipality_match_count_invalid_rows": match_count_invalid_rows,
            "primary_municipality_count": int(
                mart["primary_municipality_key"].nunique()
            ),
        },
    )


def _add_source_grain_checks(
    report: GoldRiskMartValidationReport,
    *,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
    wildfire_grid_month: pd.DataFrame,
) -> None:
    climate_duplicate_count = int(
        climate_grid_month[["grid_cell_key", "reference_month"]].duplicated().sum()
    )
    hydro_duplicate_count = int(
        hydro_grid_month[["grid_cell_key", "reference_month"]].duplicated().sum()
    )
    wildfire_duplicate_count = int(
        wildfire_grid_month[["grid_cell_key", "reference_month"]].duplicated().sum()
    )

    report.add_check(
        name="gold_risk_mart_source_grains_valid",
        passed=(
            climate_duplicate_count == 0
            and hydro_duplicate_count == 0
            and wildfire_duplicate_count == 0
        ),
        details={
            "climate_duplicate_grid_month_count": climate_duplicate_count,
            "hydro_duplicate_grid_month_count": hydro_duplicate_count,
            "wildfire_duplicate_grid_month_count": wildfire_duplicate_count,
            "climate_row_count": int(len(climate_grid_month)),
            "hydro_row_count": int(len(hydro_grid_month)),
            "wildfire_row_count": int(len(wildfire_grid_month)),
        },
    )


def _add_climate_method_and_semantics_checks(
    report: GoldRiskMartValidationReport,
    *,
    mart: pd.DataFrame,
    climate_grid_month: pd.DataFrame,
) -> None:
    actual_methods = set(mart["climate_mapping_method"].dropna().astype(str).unique())
    invalid_methods = sorted(actual_methods - EXPECTED_CLIMATE_MAPPING_METHODS)

    source_counts = _value_counts(climate_grid_month["climate_mapping_method"])
    mart_counts = _value_counts(mart["climate_mapping_method"])

    no_climate = mart["climate_mapping_method"].eq(NO_CLIMATE_COVERAGE_METHOD)

    no_climate_value_nonnull_rows = int(
        mart.loc[
            no_climate,
            [
                *CLIMATE_VALUE_COLUMNS,
                "climate_temperature_completeness_ratio",
                "climate_precipitation_completeness_ratio",
                "climate_data_completeness_score",
                "climate_feature_quality_flag",
            ],
        ]
        .notna()
        .any(axis=1)
        .sum()
    )

    no_climate_confidence_not_zero_rows = int(
        mart.loc[
            no_climate,
            "climate_idw_confidence_score",
        ]
        .fillna(-1)
        .ne(0)
        .sum()
    )

    no_climate_station_count_nonzero_rows = int(
        mart.loc[
            no_climate,
            "climate_station_count",
        ]
        .fillna(0)
        .ne(0)
        .sum()
    )

    flag_mismatch_rows = int(
        (
            mart["has_climate_feature"].astype(bool)
            != mart["climate_mapping_method"].ne(NO_CLIMATE_COVERAGE_METHOD)
        ).sum()
    )

    report.add_check(
        name="gold_risk_mart_climate_methods_and_semantics_valid",
        passed=(
            len(invalid_methods) == 0
            and source_counts == mart_counts
            and no_climate_value_nonnull_rows == 0
            and no_climate_confidence_not_zero_rows == 0
            and no_climate_station_count_nonzero_rows == 0
            and flag_mismatch_rows == 0
        ),
        details={
            "actual_methods": sorted(actual_methods),
            "invalid_methods": invalid_methods,
            "source_method_counts": source_counts,
            "mart_method_counts": mart_counts,
            "no_climate_row_count": int(no_climate.sum()),
            "no_climate_value_or_quality_nonnull_rows": no_climate_value_nonnull_rows,
            "no_climate_confidence_not_zero_rows": no_climate_confidence_not_zero_rows,
            "no_climate_station_count_nonzero_rows": no_climate_station_count_nonzero_rows,
            "has_climate_feature_flag_mismatch_rows": flag_mismatch_rows,
        },
    )


def _add_hydro_method_and_semantics_checks(
    report: GoldRiskMartValidationReport,
    *,
    mart: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
) -> None:
    actual_methods = set(
        mart["hydro_spatial_assignment_method"].dropna().astype(str).unique()
    )
    invalid_methods = sorted(actual_methods - EXPECTED_HYDRO_SPATIAL_METHODS)

    source_counts = _value_counts(hydro_grid_month["hydro_spatial_assignment_method"])
    mart_counts = _value_counts(mart["hydro_spatial_assignment_method"])

    no_hydro = mart["hydro_spatial_assignment_method"].eq(NO_HYDRO_COVERAGE_METHOD)

    no_hydro_count_nonzero_rows = int(
        (
            mart.loc[
                no_hydro,
                [
                    "hydro_station_count",
                    "hydro_basin_station_count",
                    "hydro_point_station_count",
                    "flow_station_count",
                    "flow_daily_record_count",
                    "flow_observation_day_count",
                    "flow_measurement_observation_count",
                    "flow_zero_day_count",
                    "flow_negative_value_count",
                    "level_station_count",
                    "level_daily_record_count",
                    "level_observation_day_count",
                    "level_measurement_observation_count",
                    "level_negative_value_count",
                ],
            ]
            != 0
        )
        .any(axis=1)
        .sum()
    )

    no_hydro_value_nonnull_rows = int(
        mart.loc[
            no_hydro,
            [
                *HYDRO_FLOW_VALUE_COLUMNS,
                *HYDRO_LEVEL_VALUE_COLUMNS,
                "flow_measurement_completeness_ratio",
                "level_measurement_completeness_ratio",
                "hydro_basin_intersection_area_sq_km",
                "hydro_basin_grid_coverage_ratio",
                "hydro_data_completeness_score",
                "hydro_feature_quality_flag",
            ],
        ]
        .notna()
        .any(axis=1)
        .sum()
    )

    spatial_flag_mismatch_rows = int(
        (
            mart["has_hydro_spatial_coverage"].astype(bool)
            != mart["hydro_spatial_assignment_method"].ne(NO_HYDRO_COVERAGE_METHOD)
        ).sum()
    )

    flow_flag_mismatch_rows = int(
        (
            mart["has_hydro_flow_feature"].astype(bool)
            != mart["flow_mean_measurement_value"].notna()
        ).sum()
    )
    level_flag_mismatch_rows = int(
        (
            mart["has_hydro_level_feature"].astype(bool)
            != mart["level_mean_measurement_value"].notna()
        ).sum()
    )
    hydro_flag_mismatch_rows = int(
        (
            mart["has_hydro_feature"].astype(bool)
            != (
                mart["has_hydro_flow_feature"].astype(bool)
                | mart["has_hydro_level_feature"].astype(bool)
            )
        ).sum()
    )

    report.add_check(
        name="gold_risk_mart_hydro_methods_and_semantics_valid",
        passed=(
            len(invalid_methods) == 0
            and source_counts == mart_counts
            and no_hydro_count_nonzero_rows == 0
            and no_hydro_value_nonnull_rows == 0
            and spatial_flag_mismatch_rows == 0
            and flow_flag_mismatch_rows == 0
            and level_flag_mismatch_rows == 0
            and hydro_flag_mismatch_rows == 0
        ),
        details={
            "actual_methods": sorted(actual_methods),
            "invalid_methods": invalid_methods,
            "source_method_counts": source_counts,
            "mart_method_counts": mart_counts,
            "no_hydro_row_count": int(no_hydro.sum()),
            "no_hydro_count_nonzero_rows": no_hydro_count_nonzero_rows,
            "no_hydro_value_or_quality_nonnull_rows": no_hydro_value_nonnull_rows,
            "has_hydro_spatial_coverage_flag_mismatch_rows": spatial_flag_mismatch_rows,
            "has_hydro_flow_feature_flag_mismatch_rows": flow_flag_mismatch_rows,
            "has_hydro_level_feature_flag_mismatch_rows": level_flag_mismatch_rows,
            "has_hydro_feature_flag_mismatch_rows": hydro_flag_mismatch_rows,
        },
    )


def _add_wildfire_method_and_semantics_checks(
    report: GoldRiskMartValidationReport,
    *,
    mart: pd.DataFrame,
    wildfire_grid_month: pd.DataFrame,
) -> None:
    actual_methods = set(
        mart["wildfire_temporal_assignment_method"].dropna().astype(str).unique()
    )
    invalid_methods = sorted(actual_methods - EXPECTED_WILDFIRE_TEMPORAL_METHODS)

    source_counts = _value_counts(wildfire_grid_month["wildfire_temporal_assignment_method"])
    mart_counts = _value_counts(mart["wildfire_temporal_assignment_method"])

    no_overlap = mart["wildfire_temporal_assignment_method"].eq(NO_WILDFIRE_OVERLAP_METHOD)
    overlap = mart["wildfire_temporal_assignment_method"].eq("polygon_fire_month")

    joined_flag_mismatch_rows = int(
        (
            mart["has_wildfire_perimeter_feature"].astype(bool)
            != mart["wildfire_perimeter_count"].notna()
        ).sum()
    )

    overlap_flag_mismatch_rows = int(
        (
            mart["has_wildfire_observed_perimeter_overlap"].astype(bool)
            != mart["wildfire_has_observed_perimeter_overlap"].astype(bool)
        ).sum()
    )

    no_overlap_nonzero_rows = int(
        (
            mart.loc[
                no_overlap,
                WILDFIRE_VALUE_COLUMNS,
            ].fillna(0)
            != 0
        )
        .any(axis=1)
        .sum()
    )

    overlap_invalid_rows = int(
        (
            overlap
            & (
                (mart["wildfire_perimeter_count"] < 1)
                | (mart["wildfire_intersection_area_sq_km"] <= 0)
                | (mart["wildfire_intersection_area_ha"] <= 0)
                | ~mart["wildfire_intersection_area_ratio_of_grid"].between(
                    0,
                    1,
                    inclusive="both",
                )
            )
        ).sum()
    )

    report.add_check(
        name="gold_risk_mart_wildfire_methods_and_semantics_valid",
        passed=(
            len(invalid_methods) == 0
            and source_counts == mart_counts
            and joined_flag_mismatch_rows == 0
            and overlap_flag_mismatch_rows == 0
            and no_overlap_nonzero_rows == 0
            and overlap_invalid_rows == 0
        ),
        details={
            "actual_methods": sorted(actual_methods),
            "invalid_methods": invalid_methods,
            "source_method_counts": source_counts,
            "mart_method_counts": mart_counts,
            "joined_flag_mismatch_rows": joined_flag_mismatch_rows,
            "overlap_flag_mismatch_rows": overlap_flag_mismatch_rows,
            "no_overlap_row_count": int(no_overlap.sum()),
            "no_overlap_nonzero_value_rows": no_overlap_nonzero_rows,
            "overlap_row_count": int(overlap.sum()),
            "overlap_invalid_rows": overlap_invalid_rows,
        },
    )


def _add_feature_flag_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    flag_columns = [
        "has_climate_feature",
        "has_hydro_spatial_coverage",
        "has_hydro_flow_feature",
        "has_hydro_level_feature",
        "has_hydro_feature",
        "has_wildfire_perimeter_feature",
        "has_wildfire_observed_perimeter_overlap",
    ]

    invalid_flag_rows = 0

    for column in flag_columns:
        invalid_flag_rows += int(mart[column].isna().sum())

    report.add_check(
        name="gold_risk_mart_feature_flags_valid",
        passed=invalid_flag_rows == 0,
        details={
            "invalid_flag_null_rows_total": invalid_flag_rows,
            "flag_true_counts": {
                column: int(mart[column].astype(bool).sum()) for column in flag_columns
            },
        },
    )


def _add_quality_flag_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    climate_values = set(mart["climate_feature_quality_flag"].dropna().astype(str).unique())
    hydro_values = set(mart["hydro_feature_quality_flag"].dropna().astype(str).unique())

    invalid_climate_values = sorted(climate_values - VALID_CLIMATE_QUALITY_FLAGS)
    invalid_hydro_values = sorted(hydro_values - VALID_HYDRO_QUALITY_FLAGS)

    no_climate_quality_rows = int(
        (
            mart["climate_mapping_method"].eq(NO_CLIMATE_COVERAGE_METHOD)
            & mart["climate_feature_quality_flag"].notna()
        ).sum()
    )

    no_hydro_quality_rows = int(
        (
            mart["hydro_spatial_assignment_method"].eq(NO_HYDRO_COVERAGE_METHOD)
            & mart["hydro_feature_quality_flag"].notna()
        ).sum()
    )

    report.add_check(
        name="gold_risk_mart_quality_flags_valid",
        passed=(
            len(invalid_climate_values) == 0
            and len(invalid_hydro_values) == 0
            and no_climate_quality_rows == 0
            and no_hydro_quality_rows == 0
        ),
        details={
            "climate_actual": sorted(climate_values),
            "hydro_actual": sorted(hydro_values),
            "invalid_climate_values": invalid_climate_values,
            "invalid_hydro_values": invalid_hydro_values,
            "allowed_climate": sorted(VALID_CLIMATE_QUALITY_FLAGS),
            "allowed_hydro": sorted(VALID_HYDRO_QUALITY_FLAGS),
            "no_climate_quality_rows": no_climate_quality_rows,
            "no_hydro_quality_rows": no_hydro_quality_rows,
            "climate_quality_counts": _value_counts(mart["climate_feature_quality_flag"]),
            "hydro_quality_counts": _value_counts(mart["hydro_feature_quality_flag"]),
        },
    )


def _add_ratio_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    ratio_columns = [
        "boundary_coverage_ratio",
        "primary_municipality_grid_coverage_ratio",
        "primary_municipality_coverage_ratio",
        "climate_idw_confidence_score",
        "climate_temperature_completeness_ratio",
        "climate_precipitation_completeness_ratio",
        "climate_data_completeness_score",
        "hydro_basin_grid_coverage_ratio",
        "flow_measurement_completeness_ratio",
        "level_measurement_completeness_ratio",
        "hydro_data_completeness_score",
        "wildfire_intersection_area_ratio_of_grid",
    ]

    invalid_counts = {}

    for column in ratio_columns:
        non_null = mart[column].dropna()
        out_of_range_count = int((~non_null.between(0, 1, inclusive="both")).sum())
        invalid_counts[column] = out_of_range_count

    report.add_check(
        name="gold_risk_mart_ratio_fields_valid",
        passed=all(count == 0 for count in invalid_counts.values()),
        details={
            "out_of_range_counts": invalid_counts,
        },
    )


def _add_value_coverage_check(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    climate_nonnull = int(mart["climate_mean_temp_c"].notna().sum())
    hydro_flow_nonnull = int(mart["flow_mean_measurement_value"].notna().sum())
    hydro_level_nonnull = int(mart["level_mean_measurement_value"].notna().sum())
    wildfire_overlap = int(mart["has_wildfire_observed_perimeter_overlap"].sum())

    report.add_check(
        name="gold_risk_mart_domain_value_coverage_nonzero",
        passed=(
            climate_nonnull > 0
            and hydro_flow_nonnull > 0
            and hydro_level_nonnull > 0
            and wildfire_overlap > 0
        ),
        details={
            "row_count": int(len(mart)),
            "climate_mean_temp_c_nonnull": climate_nonnull,
            "flow_mean_measurement_value_nonnull": hydro_flow_nonnull,
            "level_mean_measurement_value_nonnull": hydro_level_nonnull,
            "wildfire_overlap_rows": wildfire_overlap,
        },
    )


def _require_mart_columns(mart: pd.DataFrame) -> None:
    _require_columns(
        mart,
        {
            "grid_month_risk_feature_key",
            "grid_cell_key",
            "reference_month",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "province_code",
            "province_name",
            "centroid_longitude",
            "centroid_latitude",
            "analysis_area_sq_km",
            "boundary_coverage_ratio",
            "primary_municipality_key",
            "primary_municipality_name",
            "primary_municipality_type",
            "primary_municipality_grid_coverage_ratio",
            "primary_municipality_coverage_ratio",
            "municipality_match_count",
            "grid_month_climate_feature_key",
            "climate_mapping_method",
            "climate_station_count",
            "climate_nearest_station_distance_km",
            "climate_mean_station_distance_km",
            "climate_max_station_distance_km",
            "climate_idw_confidence_score",
            "climate_daily_record_count",
            "climate_temperature_observation_count",
            "climate_precipitation_observation_count",
            "climate_mean_temp_c",
            "climate_min_temp_c",
            "climate_max_temp_c",
            "climate_observed_min_temp_c",
            "climate_observed_max_temp_c",
            "climate_total_precip_mm",
            "climate_total_rain_mm",
            "climate_total_snow",
            "climate_precipitation_days",
            "climate_heavy_precipitation_days",
            "climate_extreme_heat_days",
            "climate_extreme_cold_days",
            "climate_freeze_thaw_days",
            "climate_temperature_completeness_ratio",
            "climate_precipitation_completeness_ratio",
            "climate_data_completeness_score",
            "climate_feature_quality_flag",
            "grid_month_hydro_feature_key",
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
            "wildfire_grid_month_key",
            "wildfire_perimeter_count",
            "wildfire_intersection_area_sq_km",
            "wildfire_intersection_area_ha",
            "wildfire_intersection_area_ratio_of_grid",
            "wildfire_has_observed_perimeter_overlap",
            "wildfire_temporal_assignment_method",
            "has_climate_feature",
            "has_hydro_spatial_coverage",
            "has_hydro_flow_feature",
            "has_hydro_level_feature",
            "has_hydro_feature",
            "has_wildfire_perimeter_feature",
            "has_wildfire_observed_perimeter_overlap",
        },
        "gold_grid_month_risk_feature_mart",
    )


def _require_grid_columns(gold_grid_cell: pd.DataFrame) -> None:
    _require_columns(
        gold_grid_cell,
        {
            "grid_cell_key",
            "grid_system",
        },
        "gold_grid_cell",
    )


def _require_climate_columns(climate_grid_month: pd.DataFrame) -> None:
    _require_columns(
        climate_grid_month,
        {
            "grid_cell_key",
            "reference_month",
            "climate_mapping_method",
        },
        "gold_grid_month_climate_feature",
    )


def _require_hydro_columns(hydro_grid_month: pd.DataFrame) -> None:
    _require_columns(
        hydro_grid_month,
        {
            "grid_cell_key",
            "reference_month",
            "hydro_spatial_assignment_method",
        },
        "gold_grid_month_hydro_feature",
    )


def _require_wildfire_columns(wildfire_grid_month: pd.DataFrame) -> None:
    _require_columns(
        wildfire_grid_month,
        {
            "grid_cell_key",
            "reference_month",
            "wildfire_temporal_assignment_method",
            "wildfire_perimeter_count",
            "wildfire_has_observed_perimeter_overlap",
        },
        "gold_grid_month_wildfire_perimeter_feature",
    )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldRiskMartValidationError(
            f"{table_name} is missing columns: {sorted(missing_columns)}"
        )


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in series.value_counts(dropna=False).to_dict().items()
    }
