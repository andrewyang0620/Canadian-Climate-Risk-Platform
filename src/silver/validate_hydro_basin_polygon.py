from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.silver.common import write_json


EXPECTED_TABLES = [
    "silver_hydro_basin_polygon",
    "silver_hydro_basin_pour_point",
    "silver_hydro_basin_station_point",
]


@dataclass(frozen=True)
class HydroBasinValidationCheck:
    name: str
    passed: bool
    details: dict[str, Any]


@dataclass(frozen=True)
class HydroBasinValidationReport:
    passed: bool
    checks: list[HydroBasinValidationCheck]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "summary": self.summary,
            "checks": [
                {
                    "name": check.name,
                    "passed": check.passed,
                    "details": check.details,
                }
                for check in self.checks
            ],
        }


def validate_hydro_basin_polygon_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = (
        "lakehouse/silver/_validation/hydro_basin_polygon/latest_validation.json"
    ),
) -> HydroBasinValidationReport:
    silver_root = Path(silver_root)

    tables = {
        table_name: read_latest_silver_table(silver_root=silver_root, table_name=table_name)
        for table_name in EXPECTED_TABLES
    }

    checks: list[HydroBasinValidationCheck] = []

    checks.extend(validate_required_columns(tables))
    checks.extend(validate_non_empty_tables(tables))
    checks.extend(validate_station_id_quality(tables))
    checks.extend(validate_geometry_quality(tables))
    checks.extend(validate_expected_geometry_types(tables))
    checks.extend(validate_cross_layer_station_alignment(tables))
    checks.extend(validate_source_crs(tables))

    summary = {
        "tables": {
            table_name: {
                "row_count": int(len(dataframe)),
                "station_id_count": (
                    int(dataframe["station_id"].nunique())
                    if "station_id" in dataframe.columns
                    else 0
                ),
            }
            for table_name, dataframe in tables.items()
        },
        "expected_tables": EXPECTED_TABLES,
    }

    report = HydroBasinValidationReport(
        passed=all(check.passed for check in checks),
        checks=checks,
        summary=summary,
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def read_latest_silver_table(
    *,
    silver_root: Path,
    table_name: str,
) -> pd.DataFrame:
    table_root = silver_root / table_name

    if not table_root.exists():
        raise FileNotFoundError(f"Silver table root does not exist: {table_root}")

    files = sorted(table_root.rglob(f"{table_name}.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found for {table_name} under {table_root}")

    latest_file = max(files, key=lambda path: path.stat().st_mtime)

    return pd.read_parquet(latest_file)


def validate_required_columns(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    base_required = {
        "station_id",
        "station_name",
        "status",
        "status_fr",
        "geometry_type",
        "geometry_wkt",
        "geometry_original_is_valid",
        "geometry_was_repaired",
        "geometry_is_valid",
        "source_crs",
        "source_name",
        "source_layer",
        "source_file",
        "mda_adp_region",
    }

    table_specific_required = {
        "silver_hydro_basin_polygon": {
            "hydro_basin_polygon_key",
            "basin_area_sq_km",
            "basin_area_sq_km_fr",
            "source_version",
            "source_revision_date",
            "shape_length_m",
            "shape_area_sq_m",
        },
        "silver_hydro_basin_pour_point": {
            "hydro_basin_pour_point_key",
            "province_or_territory",
        },
        "silver_hydro_basin_station_point": {
            "hydro_basin_station_point_key",
            "province_or_territory",
            "hydat_version",
        },
    }

    checks = []

    for table_name, dataframe in tables.items():
        required = base_required | table_specific_required[table_name]
        missing = sorted(required - set(dataframe.columns))

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_required_columns",
                passed=not missing,
                details={"missing_columns": missing},
            )
        )

    return checks


def validate_non_empty_tables(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    return [
        HydroBasinValidationCheck(
            name=f"{table_name}_non_empty",
            passed=len(dataframe) > 0,
            details={"row_count": int(len(dataframe))},
        )
        for table_name, dataframe in tables.items()
    ]


def validate_station_id_quality(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    checks = []

    for table_name, dataframe in tables.items():
        null_count = int(dataframe["station_id"].isna().sum())
        duplicate_count = int(dataframe["station_id"].duplicated().sum())
        invalid_format_count = int(
            (
                ~dataframe["station_id"]
                .astype(str)
                .str.match(r"^\d{2}[A-Z]{2,3}\d{2,3}$", na=False)
            ).sum()
        )

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_station_id_quality",
                passed=null_count == 0 and duplicate_count == 0 and invalid_format_count == 0,
                details={
                    "null_count": null_count,
                    "duplicate_count": duplicate_count,
                    "invalid_format_count": invalid_format_count,
                },
            )
        )

    return checks


def validate_geometry_quality(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    checks = []

    for table_name, dataframe in tables.items():
        null_geometry_count = int(dataframe["geometry_wkt"].isna().sum())
        invalid_geometry_count = int((~dataframe["geometry_is_valid"]).sum())

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_geometry_quality",
                passed=null_geometry_count == 0 and invalid_geometry_count == 0,
                details={
                    "null_geometry_count": null_geometry_count,
                    "invalid_geometry_count": invalid_geometry_count,
                },
            )
        )

    return checks


def validate_expected_geometry_types(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    expected = {
        "silver_hydro_basin_polygon": {"Polygon", "MultiPolygon"},
        "silver_hydro_basin_pour_point": {"Point", "MultiPoint"},
        "silver_hydro_basin_station_point": {"Point", "MultiPoint"},
    }

    checks = []

    for table_name, dataframe in tables.items():
        actual = set(dataframe["geometry_type"].dropna().astype(str))
        unexpected = sorted(actual - expected[table_name])

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_geometry_type",
                passed=not unexpected,
                details={
                    "actual_geometry_types": sorted(actual),
                    "unexpected_geometry_types": unexpected,
                    "allowed_geometry_types": sorted(expected[table_name]),
                },
            )
        )

    return checks


def validate_cross_layer_station_alignment(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    polygon_ids = set(tables["silver_hydro_basin_polygon"]["station_id"].astype(str))

    checks = []

    for table_name in [
        "silver_hydro_basin_pour_point",
        "silver_hydro_basin_station_point",
    ]:
        other_ids = set(tables[table_name]["station_id"].astype(str))
        polygon_only = sorted(polygon_ids - other_ids)
        other_only = sorted(other_ids - polygon_ids)

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_station_alignment_with_polygon",
                passed=not polygon_only and not other_only,
                details={
                    "polygon_only_count": len(polygon_only),
                    "other_only_count": len(other_only),
                    "polygon_only_sample": polygon_only[:20],
                    "other_only_sample": other_only[:20],
                },
            )
        )

    return checks


def validate_source_crs(
    tables: dict[str, pd.DataFrame],
) -> list[HydroBasinValidationCheck]:
    checks = []

    for table_name, dataframe in tables.items():
        actual = set(dataframe["source_crs"].dropna().astype(str))

        checks.append(
            HydroBasinValidationCheck(
                name=f"{table_name}_source_crs",
                passed=actual == {"EPSG:4326"},
                details={"actual_source_crs": sorted(actual)},
            )
        )

    return checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Silver Hydro basin polygon outputs.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/hydro_basin_polygon/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_hydro_basin_polygon_silver_outputs(
        silver_root=args.silver_root,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver Hydro basin polygon validation failed.")

    print("[OK] Silver Hydro basin polygon validation passed | " f"checks={len(report.checks)}")


if __name__ == "__main__":
    main()
