from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

TARGET_TABLE = "silver_wildfire_perimeter_polygon"
TARGET_PROVINCES = {"BC", "AB"}
SOURCE_CRS_NAME = "NAD_1983_Lambert_Conformal_Conic"


@dataclass
class WildfirePerimeterValidationCheck:
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
class WildfirePerimeterValidationReport:
    passed: bool
    checks: list[WildfirePerimeterValidationCheck]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "summary": self.summary,
            "checks": [check.to_dict() for check in self.checks],
        }


def validate_wildfire_perimeter_polygon_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = (
        "lakehouse/silver/_validation/wildfire_perimeter_polygon/latest_validation.json"
    ),
) -> WildfirePerimeterValidationReport:
    silver_root = Path(silver_root)
    dataframe = read_latest_silver_table(silver_root=silver_root, table_name=TARGET_TABLE)

    checks: list[WildfirePerimeterValidationCheck] = []

    required_columns = {
        "wildfire_perimeter_key",
        "cfs_ref_id",
        "source_fire_id",
        "source_key",
        "province",
        "source_agency",
        "fire_year",
        "fire_month",
        "fire_day",
        "report_date",
        "out_date",
        "source_size_ha",
        "calculated_size_ha",
        "fire_cause",
        "map_source",
        "map_method",
        "geometry_type",
        "geometry_wkt",
        "geometry_original_is_valid",
        "geometry_was_repaired",
        "geometry_is_valid",
        "source_crs",
        "source_name",
        "source_layer",
        "source_file",
        "source_record_number",
    }

    missing_columns = sorted(required_columns - set(dataframe.columns))

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_required_columns",
            passed=not missing_columns,
            details={"missing_columns": missing_columns},
        )
    )

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_non_empty",
            passed=len(dataframe) > 0,
            details={"row_count": int(len(dataframe))},
        )
    )

    duplicate_key_count = int(dataframe["wildfire_perimeter_key"].duplicated().sum())
    null_key_count = int(dataframe["wildfire_perimeter_key"].isna().sum())

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_key_quality",
            passed=null_key_count == 0 and duplicate_key_count == 0,
            details={
                "null_key_count": null_key_count,
                "duplicate_key_count": duplicate_key_count,
                "key_unique_count": int(dataframe["wildfire_perimeter_key"].nunique()),
            },
        )
    )

    null_cfs_ref_id_count = int(
        dataframe["cfs_ref_id"].isna().sum()
        + dataframe["cfs_ref_id"].astype(str).str.strip().eq("").sum()
    )

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_cfs_ref_id_quality",
            passed=null_cfs_ref_id_count == 0,
            details={"null_or_blank_cfs_ref_id_count": null_cfs_ref_id_count},
        )
    )

    invalid_province_count = int((~dataframe["province"].isin(TARGET_PROVINCES)).sum())

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_bc_ab_filter",
            passed=invalid_province_count == 0,
            details={
                "actual_provinces": sorted(dataframe["province"].dropna().unique().tolist()),
                "invalid_province_count": invalid_province_count,
            },
        )
    )

    null_year_count = int(dataframe["fire_year"].isna().sum())

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_year_presence",
            passed=null_year_count == 0,
            details={
                "fire_year_min": safe_int(dataframe["fire_year"].min()),
                "fire_year_max": safe_int(dataframe["fire_year"].max()),
                "null_year_count": null_year_count,
            },
        )
    )

    null_geometry_count = int(
        dataframe["geometry_wkt"].isna().sum()
        + dataframe["geometry_wkt"].astype(str).str.strip().eq("").sum()
    )
    invalid_geometry_count = int((~dataframe["geometry_is_valid"]).sum())

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_geometry_quality",
            passed=null_geometry_count == 0 and invalid_geometry_count == 0,
            details={
                "null_geometry_count": null_geometry_count,
                "invalid_geometry_count": invalid_geometry_count,
                "geometry_repaired_count": int(dataframe["geometry_was_repaired"].sum()),
            },
        )
    )

    allowed_geometry_types = {"Polygon", "MultiPolygon"}
    actual_geometry_types = sorted(dataframe["geometry_type"].dropna().unique().tolist())
    unexpected_geometry_types = sorted(set(actual_geometry_types) - allowed_geometry_types)

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_geometry_type",
            passed=not unexpected_geometry_types,
            details={
                "actual_geometry_types": actual_geometry_types,
                "unexpected_geometry_types": unexpected_geometry_types,
                "allowed_geometry_types": sorted(allowed_geometry_types),
            },
        )
    )

    actual_source_crs = sorted(dataframe["source_crs"].dropna().unique().tolist())

    checks.append(
        WildfirePerimeterValidationCheck(
            name="wildfire_perimeter_source_crs",
            passed=actual_source_crs == [SOURCE_CRS_NAME],
            details={"actual_source_crs": actual_source_crs},
        )
    )

    summary = {
        "table": TARGET_TABLE,
        "row_count": int(len(dataframe)),
        "key_unique_count": int(dataframe["wildfire_perimeter_key"].nunique()),
        "province_values": sorted(dataframe["province"].dropna().unique().tolist()),
        "fire_year_min": safe_int(dataframe["fire_year"].min()),
        "fire_year_max": safe_int(dataframe["fire_year"].max()),
        "geometry_repaired_count": int(dataframe["geometry_was_repaired"].sum()),
    }

    report = WildfirePerimeterValidationReport(
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


def read_latest_silver_table(*, silver_root: Path, table_name: str) -> pd.DataFrame:
    table_root = silver_root / table_name

    if not table_root.exists():
        raise FileNotFoundError(f"Silver table root does not exist: {table_root}")

    candidates = sorted(table_root.rglob(f"{table_name}.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No parquet files found for {table_name}")

    latest = max(candidates, key=lambda path: path.stat().st_mtime)

    return pd.read_parquet(latest)


def safe_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Silver wildfire perimeter polygon outputs."
    )

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/wildfire_perimeter_polygon/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_wildfire_perimeter_polygon_silver_outputs(
        silver_root=args.silver_root,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver wildfire perimeter polygon validation failed.")

    print(
        "[OK] Silver wildfire perimeter polygon validation passed | " f"checks={len(report.checks)}"
    )


if __name__ == "__main__":
    main()
