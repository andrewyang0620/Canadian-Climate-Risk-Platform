from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SilverValidationCheck:
    name: str
    passed: bool
    details: dict[str, Any]


@dataclass(frozen=True)
class SilverValidationReport:
    validation_name: str
    passed: bool
    checks: list[SilverValidationCheck]
    output_paths: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_name": self.validation_name,
            "passed": self.passed,
            "checks": [asdict(check) for check in self.checks],
            "output_paths": self.output_paths,
        }


def validate_census_boundary_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver boundary outputs for BC + Alberta Census boundaries."""
    silver_root = Path(silver_root)

    province_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_boundary_province",
    )
    municipality_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_boundary_municipality",
    )

    province_df = pd.read_parquet(province_path)
    municipality_df = pd.read_parquet(municipality_path)

    checks = [
        check_exact_province_rows(province_df),  # 2 rows
        check_exact_province_keys(province_df),  # BC/AB
        check_geometry_not_null(  # No Null Vals
            province_df,
            check_name="province_geometry_wkt_not_null",
        ),
        check_crs_present(
            province_df,
            check_name="province_crs_present",
        ),
        check_municipality_rows(municipality_df),
        check_municipality_province_keys(municipality_df),
        check_municipality_key_not_null(municipality_df),
        check_municipality_key_unique(municipality_df),
        check_geometry_not_null(
            municipality_df,
            check_name="municipality_geometry_wkt_not_null",
        ),
        check_crs_present(
            municipality_df,
            check_name="municipality_crs_present",
        ),
    ]

    report = SilverValidationReport(
        validation_name="census_boundary_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={
            "silver_boundary_province": province_path.as_posix(),
            "silver_boundary_municipality": municipality_path.as_posix(),
        },
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def latest_table_parquet(
    *,
    silver_root: Path,
    table_name: str,
) -> Path:
    files = sorted(silver_root.glob(f"{table_name}/**/{table_name}.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No Silver parquet output found for table={table_name} under {silver_root}"
        )

    return files[-1]


def check_exact_province_rows(dataframe: pd.DataFrame) -> SilverValidationCheck:
    row_count = int(len(dataframe))

    return SilverValidationCheck(
        name="province_row_count_is_2",
        passed=row_count == 2,
        details={"row_count": row_count, "expected_row_count": 2},
    )


def check_exact_province_keys(dataframe: pd.DataFrame) -> SilverValidationCheck:
    actual = sorted(dataframe["province_key"].dropna().unique().tolist())
    expected = ["AB", "BC"]

    return SilverValidationCheck(
        name="province_keys_are_ab_bc",
        passed=actual == expected,
        details={"actual": actual, "expected": expected},
    )


def check_municipality_rows(dataframe: pd.DataFrame) -> SilverValidationCheck:
    row_count = int(len(dataframe))

    return SilverValidationCheck(
        name="municipality_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
    )


def check_municipality_province_keys(
    dataframe: pd.DataFrame,
) -> SilverValidationCheck:
    actual = sorted(dataframe["province_key"].dropna().unique().tolist())
    expected = ["AB", "BC"]

    return SilverValidationCheck(
        name="municipality_province_keys_are_ab_bc",
        passed=actual == expected,
        details={"actual": actual, "expected": expected},
    )


def check_municipality_key_not_null(
    dataframe: pd.DataFrame,
) -> SilverValidationCheck:
    null_count = int(dataframe["municipality_key"].isna().sum())

    return SilverValidationCheck(
        name="municipality_key_not_null",
        passed=null_count == 0,
        details={"null_count": null_count},
    )


def check_municipality_key_unique(
    dataframe: pd.DataFrame,
) -> SilverValidationCheck:
    duplicated_count = int(dataframe["municipality_key"].duplicated().sum())

    return SilverValidationCheck(
        name="municipality_key_unique",
        passed=duplicated_count == 0,
        details={"duplicated_count": duplicated_count},
    )


def check_geometry_not_null(
    dataframe: pd.DataFrame,
    *,
    check_name: str,
) -> SilverValidationCheck:
    null_count = int(dataframe["geometry_wkt"].isna().sum())
    empty_count = int((dataframe["geometry_wkt"].fillna("").str.len() == 0).sum())

    return SilverValidationCheck(
        name=check_name,
        passed=null_count == 0 and empty_count == 0,
        details={
            "null_count": null_count,
            "empty_count": empty_count,
            "avg_wkt_length": safe_int_mean(dataframe["geometry_wkt"].dropna().str.len()),
            "max_wkt_length": safe_int_max(dataframe["geometry_wkt"].dropna().str.len()),
        },
    )


def check_crs_present(
    dataframe: pd.DataFrame,
    *,
    check_name: str,
) -> SilverValidationCheck:
    null_count = int(dataframe["crs"].isna().sum())
    unique_count = int(dataframe["crs"].dropna().nunique())

    return SilverValidationCheck(
        name=check_name,
        passed=null_count == 0 and unique_count >= 1,
        details={
            "null_count": null_count,
            "unique_count": unique_count,
        },
    )


def safe_int_mean(series: pd.Series) -> int | None:
    if series.empty:
        return None
    return int(series.mean())


def safe_int_max(series: pd.Series) -> int | None:
    if series.empty:
        return None
    return int(series.max())


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
