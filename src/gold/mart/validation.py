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
VALID_QUALITY_FLAGS = {"high", "medium", "low", "very_low"}


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
) -> GoldRiskMartValidationReport:
    report = GoldRiskMartValidationReport(
        validation_name="gold_grid_month_risk_feature_mart_validation"
    )

    _require_mart_columns(mart)
    _require_grid_columns(gold_grid_cell)
    _require_climate_columns(climate_grid_month)
    _require_hydro_columns(hydro_grid_month)

    _add_row_count_check(report, mart, gold_grid_cell)
    _add_key_check(report, mart)
    _add_grid_system_check(report, mart)
    _add_month_range_check(report, mart)
    _add_grid_key_coverage_check(report, mart, gold_grid_cell)
    _add_feature_coverage_checks(report, mart, climate_grid_month, hydro_grid_month)
    _add_quality_flag_checks(report, mart)
    _add_completeness_ratio_checks(report, mart)
    _add_source_grain_checks(report, climate_grid_month, hydro_grid_month)

    return report


def validate_risk_monthly_grid_outputs(
    *,
    gold_root: str | Path = "lakehouse/gold",
    output_json_path: str | Path = (
        "lakehouse/gold/_validation/" "risk_monthly_grid_mart/latest_validation.json"
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

    report = validate_risk_monthly_grid_dataframes(
        mart=pd.read_parquet(mart_path),
        gold_grid_cell=pd.read_parquet(grid_path),
        climate_grid_month=pd.read_parquet(climate_path),
        hydro_grid_month=pd.read_parquet(hydro_path),
    )

    report.output_paths = {
        "gold_grid_month_risk_feature_mart": mart_path.as_posix(),
        "gold_grid_cell": grid_path.as_posix(),
        "gold_grid_month_climate_feature": climate_path.as_posix(),
        "gold_grid_month_hydro_feature": hydro_path.as_posix(),
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

    grain_duplicate_count = int(mart[["grid_cell_key", "reference_month"]].duplicated().sum())

    report.add_check(
        name="gold_risk_mart_key_valid",
        passed=(null_count == 0 and duplicate_count == 0 and grain_duplicate_count == 0),
        details={
            "null_count": null_count,
            "duplicate_key_count": duplicate_count,
            "duplicate_grid_month_count": grain_duplicate_count,
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
            "counts": {
                str(key): int(value)
                for key, value in mart["grid_system"].value_counts().to_dict().items()
            },
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

    report.add_check(
        name="gold_risk_mart_grid_key_coverage_valid",
        passed=len(missing_grid_keys) == 0 and len(unexpected_grid_keys) == 0,
        details={
            "expected_grid_cell_count": len(expected_grid_keys),
            "actual_grid_cell_count": len(actual_grid_keys),
            "missing_grid_key_count": len(missing_grid_keys),
            "unexpected_grid_key_count": len(unexpected_grid_keys),
            "missing_grid_key_sample": missing_grid_keys[:20],
            "unexpected_grid_key_sample": unexpected_grid_keys[:20],
        },
    )


def _add_feature_coverage_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
) -> None:
    mart_keys = _grid_month_key_series(mart)

    climate_keys = set(_grid_month_key_series(climate_grid_month))
    climate_expected = mart_keys.isin(climate_keys)
    climate_actual = mart["has_climate_feature"].astype(bool)
    climate_mismatch_count = int((climate_expected != climate_actual).sum())

    report.add_check(
        name="gold_risk_mart_climate_coverage_flags_valid",
        passed=climate_mismatch_count == 0,
        details={
            "mismatch_count": climate_mismatch_count,
            "expected_true_count": int(climate_expected.sum()),
            "actual_true_count": int(climate_actual.sum()),
            "source_climate_row_count": int(len(climate_grid_month)),
        },
    )

    for measurement_type in ["flow", "level"]:
        source = hydro_grid_month[
            hydro_grid_month["measurement_type"].astype(str) == measurement_type
        ]
        source_keys = set(_grid_month_key_series(source))
        expected = mart_keys.isin(source_keys)
        flag_column = f"has_hydro_{measurement_type}_feature"
        actual = mart[flag_column].astype(bool)
        mismatch_count = int((expected != actual).sum())

        report.add_check(
            name=f"gold_risk_mart_hydro_{measurement_type}_coverage_flags_valid",
            passed=mismatch_count == 0,
            details={
                "mismatch_count": mismatch_count,
                "expected_true_count": int(expected.sum()),
                "actual_true_count": int(actual.sum()),
                f"source_hydro_{measurement_type}_row_count": int(len(source)),
            },
        )


def _add_quality_flag_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    quality_columns = [
        "climate_feature_quality_flag",
        "flow_feature_quality_flag",
        "level_feature_quality_flag",
    ]

    for column in quality_columns:
        actual_values = set(mart[column].dropna().astype(str).unique())
        invalid_values = sorted(actual_values - VALID_QUALITY_FLAGS)

        report.add_check(
            name=f"gold_risk_mart_{column}_valid",
            passed=len(invalid_values) == 0,
            details={
                "actual": sorted(actual_values),
                "invalid": invalid_values,
                "allowed": sorted(VALID_QUALITY_FLAGS),
            },
        )


def _add_completeness_ratio_checks(
    report: GoldRiskMartValidationReport,
    mart: pd.DataFrame,
) -> None:
    ratio_columns = [
        "temperature_completeness_ratio",
        "precipitation_completeness_ratio",
        "climate_data_completeness_score",
        "flow_mean_measurement_completeness_ratio",
        "level_mean_measurement_completeness_ratio",
    ]

    for column in ratio_columns:
        non_null = mart[column].dropna()
        out_of_range_count = int((~non_null.between(0, 1, inclusive="both")).sum())

        report.add_check(
            name=f"gold_risk_mart_{column}_valid",
            passed=out_of_range_count == 0,
            details={
                "non_null_count": int(non_null.count()),
                "out_of_range_count": out_of_range_count,
                "minimum": float(non_null.min()) if not non_null.empty else None,
                "maximum": float(non_null.max()) if not non_null.empty else None,
            },
        )


def _add_source_grain_checks(
    report: GoldRiskMartValidationReport,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
) -> None:
    climate_duplicate_count = int(
        climate_grid_month[["grid_cell_key", "reference_month"]].duplicated().sum()
    )
    hydro_duplicate_count = int(
        hydro_grid_month[["grid_cell_key", "reference_month", "measurement_type"]]
        .duplicated()
        .sum()
    )

    hydro_measurement_types = set(
        hydro_grid_month["measurement_type"].dropna().astype(str).unique()
    )

    report.add_check(
        name="gold_risk_mart_source_grains_valid",
        passed=(
            climate_duplicate_count == 0
            and hydro_duplicate_count == 0
            and hydro_measurement_types <= {"flow", "level"}
        ),
        details={
            "climate_duplicate_grid_month_count": climate_duplicate_count,
            "hydro_duplicate_grid_month_measurement_count": hydro_duplicate_count,
            "hydro_measurement_types": sorted(hydro_measurement_types),
        },
    )


def _grid_month_key_series(dataframe: pd.DataFrame) -> pd.Series:
    return dataframe["grid_cell_key"].astype(str) + "__" + dataframe["reference_month"].astype(str)


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
            "primary_municipality_key",
            "primary_municipality_name",
            "municipality_match_count",
            "climate_station_count",
            "climate_feature_quality_flag",
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
            "climate_data_completeness_score",
            "flow_station_count",
            "flow_mean_measurement_value",
            "flow_mean_measurement_completeness_ratio",
            "flow_feature_quality_flag",
            "level_station_count",
            "level_mean_measurement_value",
            "level_mean_measurement_completeness_ratio",
            "level_feature_quality_flag",
            "has_climate_feature",
            "has_hydro_flow_feature",
            "has_hydro_level_feature",
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
        },
        "gold_grid_month_climate_feature",
    )


def _require_hydro_columns(hydro_grid_month: pd.DataFrame) -> None:
    _require_columns(
        hydro_grid_month,
        {
            "grid_cell_key",
            "reference_month",
            "measurement_type",
        },
        "gold_grid_month_hydro_feature",
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
