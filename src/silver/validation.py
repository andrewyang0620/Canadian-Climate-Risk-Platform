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


CLIMATE_MEASUREMENT_COLUMNS = [
    "mean_temp_c",
    "min_temp_c",
    "max_temp_c",
    "total_precip_mm",
    "total_rain_mm",
    "total_snow",
    "snow_on_ground",
    "speed_max_gust",
    "direction_max_gust",
    "cooling_degree_days",
    "heating_degree_days",
    "min_relative_humidity",
    "max_relative_humidity",
]


def validate_eccc_climate_daily_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    expected_years: list[int] | None = None,
    min_measurement_presence_rate: float = 0.95,
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver ECCC daily climate outputs."""
    silver_root = Path(silver_root)
    expected_years = expected_years or list(range(2016, 2026))

    files = latest_climate_daily_partition_files(silver_root=silver_root)

    metrics = collect_climate_daily_metrics(files)

    checks = [
        SilverValidationCheck(
            name="climate_partition_count_matches_expected_years",
            passed=metrics["partition_count"] == len(expected_years),
            details={
                "partition_count": metrics["partition_count"],
                "expected_partition_count": len(expected_years),
            },
        ),
        SilverValidationCheck(
            name="climate_years_match_expected_range",
            passed=metrics["years"] == expected_years,
            details={
                "actual": metrics["years"],
                "expected": expected_years,
            },
        ),
        SilverValidationCheck(
            name="climate_provinces_are_ab_bc",
            passed=metrics["provinces"] == ["AB", "BC"],
            details={
                "actual": metrics["provinces"],
                "expected": ["AB", "BC"],
            },
        ),
        SilverValidationCheck(
            name="climate_row_count_gt_zero",
            passed=metrics["total_rows"] > 0,
            details={"total_rows": metrics["total_rows"]},
        ),
        SilverValidationCheck(
            name="climate_station_id_not_null",
            passed=metrics["station_id_nulls"] == 0,
            details={"null_count": metrics["station_id_nulls"]},
        ),
        SilverValidationCheck(
            name="climate_observation_date_not_null",
            passed=metrics["observation_date_nulls"] == 0,
            details={"null_count": metrics["observation_date_nulls"]},
        ),
        SilverValidationCheck(
            name="climate_daily_key_not_null_and_unique",
            passed=metrics["climate_daily_key_nulls"] == 0
            and metrics["climate_daily_key_duplicates"] == 0,
            details={
                "null_count": metrics["climate_daily_key_nulls"],
                "duplicate_count": metrics["climate_daily_key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="climate_coordinates_not_null",
            passed=metrics["latitude_nulls"] == 0 and metrics["longitude_nulls"] == 0,
            details={
                "latitude_nulls": metrics["latitude_nulls"],
                "longitude_nulls": metrics["longitude_nulls"],
            },
        ),
        SilverValidationCheck(
            name="climate_coordinates_in_bc_ab_range",
            passed=metrics["latitude_out_of_range"] == 0 and metrics["longitude_out_of_range"] == 0,
            details={
                "latitude_out_of_range": metrics["latitude_out_of_range"],
                "longitude_out_of_range": metrics["longitude_out_of_range"],
                "latitude_range": [48.0, 61.0],
                "longitude_range": [-140.0, -109.0],
            },
        ),
        SilverValidationCheck(
            name="climate_measurement_presence_rate_above_threshold",
            passed=metrics["measurement_presence_rate"] >= min_measurement_presence_rate,
            details={
                "measurement_presence_rate": metrics["measurement_presence_rate"],
                "min_required": min_measurement_presence_rate,
                "rows_with_any_measurement": metrics["rows_with_any_measurement"],
                "total_rows": metrics["total_rows"],
                "measurement_columns": CLIMATE_MEASUREMENT_COLUMNS,
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="eccc_climate_daily_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={
            "silver_climate_daily_root": (silver_root / "silver_climate_daily").as_posix()
        },
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def latest_climate_daily_partition_files(*, silver_root: Path) -> list[Path]:
    files = sorted(silver_root.glob("silver_climate_daily/**/silver_climate_daily.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No Silver parquet output found for table=silver_climate_daily under {silver_root}"
        )

    latest_extract_date = max(
        file.parts[-4].replace("extract_date=", "")
        for file in files
        if "extract_date=" in file.parts[-4]
    )

    latest_files = [file for file in files if f"extract_date={latest_extract_date}" in file.parts]

    latest_run_id = max(
        part.replace("run_id=", "")
        for file in latest_files
        for part in file.parts
        if part.startswith("run_id=")
    )

    return [file for file in latest_files if f"run_id={latest_run_id}" in file.parts]


def collect_climate_daily_metrics(files: list[Path]) -> dict[str, Any]:
    total_rows = 0
    years: set[int] = set()
    provinces: set[str] = set()

    station_id_nulls = 0
    observation_date_nulls = 0
    climate_daily_key_nulls = 0
    climate_daily_key_duplicates = 0
    latitude_nulls = 0
    longitude_nulls = 0
    latitude_out_of_range = 0
    longitude_out_of_range = 0
    rows_with_any_measurement = 0

    seen_keys: set[str] = set()
    date_min = None
    date_max = None
    station_ids: set[str] = set()

    for file in files:
        dataframe = pd.read_parquet(file)
        total_rows += len(dataframe)

        years.update(int(value) for value in dataframe["observation_year"].dropna().unique())
        provinces.update(str(value) for value in dataframe["province"].dropna().unique())

        station_id_nulls += int(dataframe["station_id"].isna().sum())
        observation_date_nulls += int(dataframe["observation_date"].isna().sum())
        climate_daily_key_nulls += int(dataframe["climate_daily_key"].isna().sum())
        latitude_nulls += int(dataframe["latitude"].isna().sum())
        longitude_nulls += int(dataframe["longitude"].isna().sum())

        latitude_out_of_range += int(
            ((dataframe["latitude"] < 48.0) | (dataframe["latitude"] > 61.0)).sum()
        )
        longitude_out_of_range += int(
            ((dataframe["longitude"] < -140.0) | (dataframe["longitude"] > -109.0)).sum()
        )

        keys = dataframe["climate_daily_key"].dropna().astype(str)
        for key in keys:
            if key in seen_keys:
                climate_daily_key_duplicates += 1
            else:
                seen_keys.add(key)

        measurement_columns = [
            column for column in CLIMATE_MEASUREMENT_COLUMNS if column in dataframe.columns
        ]

        if measurement_columns:
            rows_with_any_measurement += int(
                dataframe[measurement_columns].notna().any(axis=1).sum()
            )

        station_ids.update(str(value) for value in dataframe["station_id"].dropna().unique())

        current_date_min = dataframe["observation_date"].min()
        current_date_max = dataframe["observation_date"].max()

        date_min = current_date_min if date_min is None else min(date_min, current_date_min)
        date_max = current_date_max if date_max is None else max(date_max, current_date_max)

    measurement_presence_rate = rows_with_any_measurement / total_rows if total_rows else 0.0

    return {
        "partition_count": len(files),
        "total_rows": total_rows,
        "years": sorted(years),
        "provinces": sorted(provinces),
        "station_id_nulls": station_id_nulls,
        "observation_date_nulls": observation_date_nulls,
        "climate_daily_key_nulls": climate_daily_key_nulls,
        "climate_daily_key_duplicates": climate_daily_key_duplicates,
        "latitude_nulls": latitude_nulls,
        "longitude_nulls": longitude_nulls,
        "latitude_out_of_range": latitude_out_of_range,
        "longitude_out_of_range": longitude_out_of_range,
        "rows_with_any_measurement": rows_with_any_measurement,
        "measurement_presence_rate": round(measurement_presence_rate, 6),
        "date_min": date_min,
        "date_max": date_max,
        "station_count": len(station_ids),
    }
