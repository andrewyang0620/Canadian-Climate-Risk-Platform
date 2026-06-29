from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


TARGET_TABLE = "gold_grid_month_wildfire_perimeter_feature"
EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
EXPECTED_REFERENCE_MONTH_START = "2016-01"
EXPECTED_REFERENCE_MONTH_END = "2025-12"
EXPECTED_MONTH_COUNT = 120
EXPECTED_GRID_CELL_COUNT = 16508
TARGET_CRS_EPSG = 3347


@dataclass
class WildfirePerimeterGoldValidationCheck:
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
class WildfirePerimeterGoldValidationReport:
    passed: bool
    checks: list[WildfirePerimeterGoldValidationCheck]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "summary": self.summary,
            "checks": [check.to_dict() for check in self.checks],
        }


def validate_gold_wildfire_perimeter_grid_features(
    *,
    gold_root: str | Path = "lakehouse/gold",
    expected_grid_cell_count: int = EXPECTED_GRID_CELL_COUNT,
    expected_month_count: int = EXPECTED_MONTH_COUNT,
    expected_reference_month_start: str = EXPECTED_REFERENCE_MONTH_START,
    expected_reference_month_end: str = EXPECTED_REFERENCE_MONTH_END,
    output_json_path: str | Path | None = (
        "lakehouse/gold/_validation/wildfire_perimeter_grid_features/latest_validation.json"
    ),
) -> WildfirePerimeterGoldValidationReport:
    gold_root = Path(gold_root)
    table_path = latest_table_path(gold_root / TARGET_TABLE, TARGET_TABLE)
    dataframe = pd.read_parquet(table_path)

    checks: list[WildfirePerimeterGoldValidationCheck] = []

    required_columns = {
        "wildfire_grid_month_key",
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "province_key",
        "reference_month",
        "crs_epsg",
        "grid_analysis_area_sq_km",
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
        "wildfire_has_observed_perimeter_overlap",
        "wildfire_temporal_assignment_method",
    }

    forbidden_columns = {
        "wildfire_total_source_size_ha",
        "wildfire_total_calculated_size_ha",
    }

    missing_columns = sorted(required_columns - set(dataframe.columns))
    unexpected_forbidden_columns = sorted(forbidden_columns & set(dataframe.columns))

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_required_columns",
            passed=not missing_columns and not unexpected_forbidden_columns,
            details={
                "missing_columns": missing_columns,
                "unexpected_forbidden_columns": unexpected_forbidden_columns,
            },
        )
    )

    grid_cell_count = int(dataframe["grid_cell_key"].nunique())
    month_count = int(dataframe["reference_month"].nunique())
    expected_row_count = grid_cell_count * month_count

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_complete_grid_month_skeleton",
            passed=(
                grid_cell_count == expected_grid_cell_count
                and month_count == expected_month_count
                and len(dataframe) == expected_row_count
            ),
            details={
                "row_count": int(len(dataframe)),
                "expected_row_count": int(expected_row_count),
                "grid_cell_count": grid_cell_count,
                "expected_grid_cell_count": expected_grid_cell_count,
                "month_count": month_count,
                "expected_month_count": expected_month_count,
            },
        )
    )

    duplicate_key_count = int(dataframe["wildfire_grid_month_key"].duplicated().sum())
    null_key_count = int(dataframe["wildfire_grid_month_key"].isna().sum())

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_key_quality",
            passed=null_key_count == 0 and duplicate_key_count == 0,
            details={
                "null_key_count": null_key_count,
                "duplicate_key_count": duplicate_key_count,
                "key_unique_count": int(dataframe["wildfire_grid_month_key"].nunique()),
            },
        )
    )

    actual_grid_systems = sorted(dataframe["grid_system"].dropna().unique().tolist())
    unexpected_grid_systems = sorted(set(actual_grid_systems) - EXPECTED_GRID_SYSTEMS)

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_grid_scope",
            passed=not unexpected_grid_systems,
            details={
                "actual_grid_systems": actual_grid_systems,
                "unexpected_grid_systems": unexpected_grid_systems,
            },
        )
    )

    reference_month_min = str(dataframe["reference_month"].min())
    reference_month_max = str(dataframe["reference_month"].max())

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_reference_month_range",
            passed=(
                reference_month_min == expected_reference_month_start
                and reference_month_max == expected_reference_month_end
                and month_count == expected_month_count
            ),
            details={
                "reference_month_min": reference_month_min,
                "reference_month_max": reference_month_max,
                "expected_reference_month_start": expected_reference_month_start,
                "expected_reference_month_end": expected_reference_month_end,
                "month_count": month_count,
            },
        )
    )

    actual_crs_values = sorted(dataframe["crs_epsg"].dropna().unique().tolist())

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_crs",
            passed=actual_crs_values == [TARGET_CRS_EPSG],
            details={"actual_crs_values": actual_crs_values},
        )
    )

    metric_columns = [
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

    null_metric_counts = {column: int(dataframe[column].isna().sum()) for column in metric_columns}
    negative_metric_counts = {
        column: int((dataframe[column] < 0).sum()) for column in metric_columns
    }

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_metric_quality",
            passed=(
                sum(null_metric_counts.values()) == 0 and sum(negative_metric_counts.values()) == 0
            ),
            details={
                "null_metric_counts": null_metric_counts,
                "negative_metric_counts": negative_metric_counts,
            },
        )
    )

    cause_sum = (
        dataframe["wildfire_cause_n_polygon_count"]
        + dataframe["wildfire_cause_h_polygon_count"]
        + dataframe["wildfire_cause_u_polygon_count"]
        + dataframe["wildfire_cause_prescribed_burn_polygon_count"]
        + dataframe["wildfire_cause_other_polygon_count"]
    )

    cause_count_mismatch = int((cause_sum != dataframe["wildfire_perimeter_count"]).sum())

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_cause_count_consistency",
            passed=cause_count_mismatch == 0,
            details={"cause_count_mismatch": cause_count_mismatch},
        )
    )

    expected_overlap = dataframe["wildfire_perimeter_count"] > 0
    overlap_mismatch_count = int(
        (dataframe["wildfire_has_observed_perimeter_overlap"] != expected_overlap).sum()
    )

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_overlap_flag_consistency",
            passed=overlap_mismatch_count == 0,
            details={"overlap_mismatch_count": overlap_mismatch_count},
        )
    )

    no_overlap = dataframe["wildfire_perimeter_count"] == 0
    no_overlap_area_nonzero_count = int(
        (
            no_overlap
            & (
                dataframe["wildfire_intersection_area_ha"].ne(0)
                | dataframe["wildfire_intersection_area_sq_km"].ne(0)
                | dataframe["wildfire_intersection_area_ratio_of_grid"].ne(0)
            )
        ).sum()
    )

    checks.append(
        WildfirePerimeterGoldValidationCheck(
            name="wildfire_gold_zero_semantics",
            passed=no_overlap_area_nonzero_count == 0,
            details={
                "no_overlap_area_nonzero_count": no_overlap_area_nonzero_count,
                "zero_semantics": (
                    "Zero wildfire metrics mean no observed NFDB polygon perimeter "
                    "overlap for that grid-month, not zero physical wildfire risk."
                ),
            },
        )
    )

    summary = {
        "table": TARGET_TABLE,
        "table_path": table_path.as_posix(),
        "row_count": int(len(dataframe)),
        "grid_cell_count": grid_cell_count,
        "month_count": month_count,
        "reference_month_min": reference_month_min,
        "reference_month_max": reference_month_max,
        "nonzero_grid_month_count": int(dataframe["wildfire_has_observed_perimeter_overlap"].sum()),
        "total_intersection_area_ha": float(dataframe["wildfire_intersection_area_ha"].sum()),
        "max_intersection_area_ha": float(dataframe["wildfire_intersection_area_ha"].max()),
        "prescribed_burn_grid_month_count": int(
            (dataframe["wildfire_cause_prescribed_burn_polygon_count"] > 0).sum()
        ),
    }

    report = WildfirePerimeterGoldValidationReport(
        passed=all(check.passed for check in checks),
        checks=checks,
        summary=summary,
    )

    if output_json_path is not None:
        output_json_path = Path(output_json_path)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(
            json.dumps(report.to_dict(), indent=2),
            encoding="utf-8",
        )

    return report


def latest_table_path(table_root: Path, table_name: str) -> Path:
    if not table_root.exists():
        raise FileNotFoundError(f"Gold table root does not exist: {table_root}")

    candidates = sorted(table_root.rglob(f"{table_name}.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No parquet files found for {table_name}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Gold wildfire perimeter grid-month features."
    )

    parser.add_argument(
        "--gold-root",
        default="lakehouse/gold",
        help="Gold output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/gold/_validation/wildfire_perimeter_grid_features/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_gold_wildfire_perimeter_grid_features(
        gold_root=args.gold_root,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Gold wildfire perimeter grid feature validation failed.")

    print(
        "[OK] Gold wildfire perimeter grid feature validation passed | "
        f"checks={len(report.checks)}"
    )


if __name__ == "__main__":
    main()
