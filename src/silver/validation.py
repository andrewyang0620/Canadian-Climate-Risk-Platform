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


def validate_wildfire_history_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver wildfire event outputs."""
    silver_root = Path(silver_root)

    wildfire_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_wildfire_event",
    )

    dataframe = pd.read_parquet(wildfire_path)
    metrics = collect_wildfire_event_metrics(dataframe)

    checks = [
        SilverValidationCheck(
            name="wildfire_row_count_gt_zero",
            passed=metrics["row_count"] > 0,
            details={"row_count": metrics["row_count"]},
        ),
        SilverValidationCheck(
            name="wildfire_provinces_are_ab_bc",
            passed=metrics["provinces"] == ["AB", "BC"],
            details={"actual": metrics["provinces"], "expected": ["AB", "BC"]},
        ),
        SilverValidationCheck(
            name="wildfire_event_key_not_null_and_unique",
            passed=metrics["event_key_nulls"] == 0 and metrics["event_key_duplicates"] == 0,
            details={
                "null_count": metrics["event_key_nulls"],
                "duplicate_count": metrics["event_key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="wildfire_coordinates_not_null",
            passed=metrics["latitude_nulls"] == 0 and metrics["longitude_nulls"] == 0,
            details={
                "latitude_nulls": metrics["latitude_nulls"],
                "longitude_nulls": metrics["longitude_nulls"],
            },
        ),
        SilverValidationCheck(
            name="wildfire_coordinates_in_bc_ab_range",
            passed=metrics["latitude_out_of_range"] == 0 and metrics["longitude_out_of_range"] == 0,
            details={
                "latitude_out_of_range": metrics["latitude_out_of_range"],
                "longitude_out_of_range": metrics["longitude_out_of_range"],
                "latitude_range": [48.0, 61.0],
                "longitude_range": [-140.0, -109.0],
            },
        ),
        SilverValidationCheck(
            name="wildfire_non_null_fire_years_in_expected_range",
            passed=metrics["fire_year_out_of_range"] == 0,
            details={
                "fire_year_min": metrics["fire_year_min"],
                "fire_year_max": metrics["fire_year_max"],
                "fire_year_nulls": metrics["fire_year_nulls"],
                "fire_year_out_of_range": metrics["fire_year_out_of_range"],
                "expected_range": [1900, 2100],
            },
        ),
        SilverValidationCheck(
            name="wildfire_fire_size_non_negative",
            passed=metrics["negative_fire_size_count"] == 0,
            details={
                "negative_fire_size_count": metrics["negative_fire_size_count"],
                "fire_size_nulls": metrics["fire_size_nulls"],
                "fire_size_max": metrics["fire_size_max"],
            },
        ),
        SilverValidationCheck(
            name="wildfire_province_inference_method_present",
            passed=metrics["province_method_nulls"] == 0,
            details={
                "province_method_nulls": metrics["province_method_nulls"],
                "province_method_counts": metrics["province_method_counts"],
            },
        ),
        SilverValidationCheck(
            name="wildfire_source_record_count_valid",
            passed=metrics["source_record_count_invalid"] == 0,
            details={
                "source_record_count_invalid": metrics["source_record_count_invalid"],
                "source_record_count_max": metrics["source_record_count_max"],
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="wildfire_history_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={"silver_wildfire_event": wildfire_path.as_posix()},
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def collect_wildfire_event_metrics(dataframe: pd.DataFrame) -> dict[str, Any]:
    row_count = int(len(dataframe))

    non_null_years = dataframe["fire_year"].dropna()
    non_null_sizes = dataframe["fire_size_ha"].dropna()

    province_method_counts = (
        dataframe["province_inference_method"].value_counts(dropna=False).to_dict()
    )

    return {
        "row_count": row_count,
        "provinces": sorted(dataframe["province"].dropna().unique().tolist()),
        "event_key_nulls": int(dataframe["wildfire_event_key"].isna().sum()),
        "event_key_duplicates": int(dataframe["wildfire_event_key"].duplicated().sum()),
        "latitude_nulls": int(dataframe["latitude"].isna().sum()),
        "longitude_nulls": int(dataframe["longitude"].isna().sum()),
        "latitude_out_of_range": int(
            ((dataframe["latitude"] < 48.0) | (dataframe["latitude"] > 61.0)).sum()
        ),
        "longitude_out_of_range": int(
            ((dataframe["longitude"] < -140.0) | (dataframe["longitude"] > -109.0)).sum()
        ),
        "fire_year_nulls": int(dataframe["fire_year"].isna().sum()),
        "fire_year_min": safe_series_min(non_null_years),
        "fire_year_max": safe_series_max(non_null_years),
        "fire_year_out_of_range": int(((non_null_years < 1900) | (non_null_years > 2100)).sum()),
        "fire_size_nulls": int(dataframe["fire_size_ha"].isna().sum()),
        "fire_size_max": safe_series_max(non_null_sizes),
        "negative_fire_size_count": int((non_null_sizes < 0).sum()),
        "province_method_nulls": int(dataframe["province_inference_method"].isna().sum()),
        "province_method_counts": {
            str(key): int(value) for key, value in province_method_counts.items()
        },
        "source_record_count_invalid": int((dataframe["source_record_count"] < 1).sum()),
        "source_record_count_max": safe_series_max(dataframe["source_record_count"]),
    }


def safe_series_min(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.min())


def safe_series_max(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.max())


def validate_hydat_archive_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    expected_start_year: int = 1901,
    expected_end_year: int = 2026,
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver HYDAT station and daily hydrometric outputs."""
    silver_root = Path(silver_root)

    station_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_hydro_station",
    )
    daily_files = latest_hydro_daily_partition_files(silver_root=silver_root)

    station_df = pd.read_parquet(station_path)
    station_metrics = collect_hydro_station_metrics(station_df)

    station_ids = set(station_df["station_id"].dropna().astype(str).tolist())
    daily_metrics = collect_hydro_daily_metrics(daily_files, station_ids)

    expected_years = list(range(expected_start_year, expected_end_year + 1))

    checks = [
        SilverValidationCheck(
            name="hydro_station_row_count_gt_zero",
            passed=station_metrics["row_count"] > 0,
            details={"row_count": station_metrics["row_count"]},
        ),
        SilverValidationCheck(
            name="hydro_station_provinces_are_ab_bc",
            passed=station_metrics["provinces"] == ["AB", "BC"],
            details={
                "actual": station_metrics["provinces"],
                "expected": ["AB", "BC"],
            },
        ),
        SilverValidationCheck(
            name="hydro_station_id_not_null_and_unique",
            passed=station_metrics["station_id_nulls"] == 0
            and station_metrics["station_id_duplicates"] == 0,
            details={
                "null_count": station_metrics["station_id_nulls"],
                "duplicate_count": station_metrics["station_id_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="hydro_station_coordinates_not_null",
            passed=station_metrics["latitude_nulls"] == 0
            and station_metrics["longitude_nulls"] == 0,
            details={
                "latitude_nulls": station_metrics["latitude_nulls"],
                "longitude_nulls": station_metrics["longitude_nulls"],
            },
        ),
        SilverValidationCheck(
            name="hydro_station_coordinates_in_bc_ab_range",
            passed=station_metrics["latitude_out_of_range"] == 0
            and station_metrics["longitude_out_of_range"] == 0,
            details={
                "latitude_out_of_range": station_metrics["latitude_out_of_range"],
                "longitude_out_of_range": station_metrics["longitude_out_of_range"],
                "latitude_range": [48.0, 61.0],
                "longitude_range": [-140.0, -109.0],
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_partition_count_matches_years",
            passed=daily_metrics["partition_count"] == len(expected_years),
            details={
                "partition_count": daily_metrics["partition_count"],
                "expected_partition_count": len(expected_years),
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_years_match_expected_range",
            passed=daily_metrics["years"] == expected_years,
            details={
                "actual": daily_metrics["years"],
                "expected": expected_years,
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_measurement_types_are_flow_level",
            passed=daily_metrics["measurement_types"] == ["flow", "level"],
            details={
                "actual": daily_metrics["measurement_types"],
                "expected": ["flow", "level"],
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_key_not_null_and_unique",
            passed=daily_metrics["hydro_daily_key_nulls"] == 0
            and daily_metrics["hydro_daily_key_duplicates"] == 0,
            details={
                "null_count": daily_metrics["hydro_daily_key_nulls"],
                "duplicate_count": daily_metrics["hydro_daily_key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_required_fields_not_null",
            passed=daily_metrics["station_id_nulls"] == 0
            and daily_metrics["observation_date_nulls"] == 0
            and daily_metrics["measurement_value_nulls"] == 0,
            details={
                "station_id_nulls": daily_metrics["station_id_nulls"],
                "observation_date_nulls": daily_metrics["observation_date_nulls"],
                "measurement_value_nulls": daily_metrics["measurement_value_nulls"],
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_flow_values_non_negative",
            passed=daily_metrics["negative_flow_value_count"] == 0,
            details={
                "negative_flow_value_count": daily_metrics["negative_flow_value_count"],
                "flow_row_count": daily_metrics["flow_row_count"],
            },
        ),
        SilverValidationCheck(
            name="hydro_daily_station_ids_exist_in_station_table",
            passed=daily_metrics["unknown_station_id_count"] == 0,
            details={
                "unknown_station_id_count": daily_metrics["unknown_station_id_count"],
                "daily_station_count": daily_metrics["daily_station_count"],
                "station_table_count": station_metrics["row_count"],
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="hydat_archive_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={
            "silver_hydro_station": station_path.as_posix(),
            "silver_hydro_daily_root": (silver_root / "silver_hydro_daily").as_posix(),
        },
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def latest_hydro_daily_partition_files(*, silver_root: Path) -> list[Path]:
    files = sorted(silver_root.glob("silver_hydro_daily/**/silver_hydro_daily.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No Silver parquet output found for table=silver_hydro_daily under {silver_root}"
        )

    run_pairs = []

    for file in files:
        extract_date = None
        run_id = None

        for part in file.parts:
            if part.startswith("extract_date="):
                extract_date = part.replace("extract_date=", "")
            if part.startswith("run_id="):
                run_id = part.replace("run_id=", "")

        if extract_date and run_id:
            run_pairs.append((extract_date, run_id))

    if not run_pairs:
        raise FileNotFoundError(
            f"No partitioned Silver HYDAT daily files with extract_date/run_id under {silver_root}"
        )

    latest_extract_date, latest_run_id = max(run_pairs)

    return [
        file
        for file in files
        if f"extract_date={latest_extract_date}" in file.parts
        and f"run_id={latest_run_id}" in file.parts
    ]


def collect_hydro_station_metrics(dataframe: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": int(len(dataframe)),
        "provinces": sorted(dataframe["province"].dropna().unique().tolist()),
        "station_id_nulls": int(dataframe["station_id"].isna().sum()),
        "station_id_duplicates": int(dataframe["station_id"].duplicated().sum()),
        "latitude_nulls": int(dataframe["latitude"].isna().sum()),
        "longitude_nulls": int(dataframe["longitude"].isna().sum()),
        "latitude_out_of_range": int(
            ((dataframe["latitude"] < 48.0) | (dataframe["latitude"] > 61.0)).sum()
        ),
        "longitude_out_of_range": int(
            ((dataframe["longitude"] < -140.0) | (dataframe["longitude"] > -109.0)).sum()
        ),
    }


def collect_hydro_daily_metrics(
    files: list[Path],
    station_ids: set[str],
) -> dict[str, Any]:
    total_rows = 0
    years: set[int] = set()
    measurement_types: set[str] = set()
    daily_station_ids: set[str] = set()

    hydro_daily_key_nulls = 0
    hydro_daily_key_duplicates = 0
    station_id_nulls = 0
    observation_date_nulls = 0
    measurement_value_nulls = 0
    negative_flow_value_count = 0
    flow_row_count = 0
    unknown_station_id_count = 0

    for file in files:
        dataframe = pd.read_parquet(file)

        total_rows += int(len(dataframe))
        years.update(
            int(value) for value in dataframe["observation_year"].dropna().unique().tolist()
        )
        measurement_types.update(
            str(value) for value in dataframe["measurement_type"].dropna().unique()
        )

        hydro_daily_key_nulls += int(dataframe["hydro_daily_key"].isna().sum())
        hydro_daily_key_duplicates += int(dataframe["hydro_daily_key"].duplicated().sum())
        station_id_nulls += int(dataframe["station_id"].isna().sum())
        observation_date_nulls += int(dataframe["observation_date"].isna().sum())
        measurement_value_nulls += int(dataframe["measurement_value"].isna().sum())

        station_series = dataframe["station_id"].dropna().astype(str)
        daily_station_ids.update(station_series.unique().tolist())
        unknown_station_id_count += int((~station_series.isin(station_ids)).sum())

        flow_rows = dataframe[dataframe["measurement_type"] == "flow"]
        flow_row_count += int(len(flow_rows))
        negative_flow_value_count += int((flow_rows["measurement_value"] < 0).sum())

    return {
        "partition_count": int(len(files)),
        "total_rows": total_rows,
        "years": sorted(years),
        "measurement_types": sorted(measurement_types),
        "hydro_daily_key_nulls": hydro_daily_key_nulls,
        "hydro_daily_key_duplicates": hydro_daily_key_duplicates,
        "station_id_nulls": station_id_nulls,
        "observation_date_nulls": observation_date_nulls,
        "measurement_value_nulls": measurement_value_nulls,
        "negative_flow_value_count": negative_flow_value_count,
        "flow_row_count": flow_row_count,
        "unknown_station_id_count": unknown_station_id_count,
        "daily_station_count": int(len(daily_station_ids)),
    }


def validate_canadian_disaster_database_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver Canadian Disaster Database event-month outputs."""
    silver_root = Path(silver_root)

    disaster_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_disaster_event_month",
    )

    dataframe = pd.read_parquet(disaster_path)
    metrics = collect_disaster_event_month_metrics(dataframe)

    checks = [
        SilverValidationCheck(
            name="disaster_row_count_gt_zero",
            passed=metrics["row_count"] > 0,
            details={"row_count": metrics["row_count"]},
        ),
        SilverValidationCheck(
            name="disaster_provinces_are_ab_bc",
            passed=metrics["provinces"] == ["AB", "BC"],
            details={"actual": metrics["provinces"], "expected": ["AB", "BC"]},
        ),
        SilverValidationCheck(
            name="disaster_event_month_key_not_null_and_unique",
            passed=metrics["key_nulls"] == 0 and metrics["key_duplicates"] == 0,
            details={
                "null_count": metrics["key_nulls"],
                "duplicate_count": metrics["key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="disaster_required_dates_not_null",
            passed=metrics["event_month_nulls"] == 0
            and metrics["event_start_date_nulls"] == 0
            and metrics["event_end_date_nulls"] == 0,
            details={
                "event_month_nulls": metrics["event_month_nulls"],
                "event_start_date_nulls": metrics["event_start_date_nulls"],
                "event_end_date_nulls": metrics["event_end_date_nulls"],
            },
        ),
        SilverValidationCheck(
            name="disaster_event_month_between_start_and_end",
            passed=metrics["event_month_outside_date_range"] == 0,
            details={"event_month_outside_date_range": metrics["event_month_outside_date_range"]},
        ),
        SilverValidationCheck(
            name="disaster_type_not_null",
            passed=metrics["disaster_type_nulls"] == 0,
            details={"null_count": metrics["disaster_type_nulls"]},
        ),
        SilverValidationCheck(
            name="disaster_impact_counts_non_negative",
            passed=metrics["negative_impact_count"] == 0,
            details={
                "negative_impact_count": metrics["negative_impact_count"],
                "impact_columns": ["fatalities", "injured", "evacuated"],
            },
        ),
        SilverValidationCheck(
            name="disaster_cost_fields_non_negative",
            passed=metrics["negative_cost_count"] == 0,
            details={
                "negative_cost_count": metrics["negative_cost_count"],
                "cost_columns": [
                    "estimated_total_cost_cad",
                    "normalized_total_cost_cad",
                ],
            },
        ),
        SilverValidationCheck(
            name="disaster_source_record_count_valid",
            passed=metrics["source_record_count_invalid"] == 0,
            details={
                "source_record_count_invalid": metrics["source_record_count_invalid"],
                "source_record_count_max": metrics["source_record_count_max"],
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="canadian_disaster_database_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={"silver_disaster_event_month": disaster_path.as_posix()},
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def collect_disaster_event_month_metrics(dataframe: pd.DataFrame) -> dict[str, Any]:
    impact_columns = [
        column for column in ["fatalities", "injured", "evacuated"] if column in dataframe.columns
    ]

    cost_columns = [
        column
        for column in ["estimated_total_cost_cad", "normalized_total_cost_cad"]
        if column in dataframe.columns
    ]

    negative_impact_count = 0
    for column in impact_columns:
        negative_impact_count += int((dataframe[column].dropna() < 0).sum())

    negative_cost_count = 0
    for column in cost_columns:
        negative_cost_count += int((dataframe[column].dropna() < 0).sum())

    event_month_outside_date_range = count_event_month_outside_date_range(dataframe)

    return {
        "row_count": int(len(dataframe)),
        "provinces": sorted(dataframe["province"].dropna().unique().tolist()),
        "key_nulls": int(dataframe["disaster_event_month_key"].isna().sum()),
        "key_duplicates": int(dataframe["disaster_event_month_key"].duplicated().sum()),
        "event_month_nulls": int(dataframe["event_month"].isna().sum()),
        "event_start_date_nulls": int(dataframe["event_start_date"].isna().sum()),
        "event_end_date_nulls": int(dataframe["event_end_date"].isna().sum()),
        "event_month_outside_date_range": event_month_outside_date_range,
        "disaster_type_nulls": int(dataframe["disaster_type"].isna().sum()),
        "negative_impact_count": negative_impact_count,
        "negative_cost_count": negative_cost_count,
        "source_record_count_invalid": int((dataframe["source_record_count"] < 1).sum()),
        "source_record_count_max": safe_series_max(dataframe["source_record_count"]),
    }


def count_event_month_outside_date_range(dataframe: pd.DataFrame) -> int:
    bad_count = 0

    for row in dataframe[["event_month", "event_start_date", "event_end_date"]].itertuples(
        index=False
    ):
        event_month, start_date, end_date = row

        if pd.isna(event_month) or pd.isna(start_date) or pd.isna(end_date):
            bad_count += 1
            continue

        event_period = pd.Period(str(event_month)[:7], freq="M")
        start_period = pd.Period(str(start_date)[:7], freq="M")
        end_period = pd.Period(str(end_date)[:7], freq="M")

        if end_period < start_period:
            end_period = start_period

        if event_period < start_period or event_period > end_period:
            bad_count += 1

    return bad_count


def validate_municipal_flood_hazard_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver municipal flood hazard zone outputs."""
    silver_root = Path(silver_root)

    flood_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_flood_hazard_zone",
    )

    dataframe = pd.read_parquet(flood_path)
    metrics = collect_municipal_flood_hazard_metrics(dataframe)

    checks = [
        SilverValidationCheck(
            name="flood_hazard_row_count_gt_zero",
            passed=metrics["row_count"] > 0,
            details={"row_count": metrics["row_count"]},
        ),
        SilverValidationCheck(
            name="flood_hazard_cities_are_calgary_vancouver",
            passed=metrics["cities"] == ["calgary", "vancouver"],
            details={
                "actual": metrics["cities"],
                "expected": ["calgary", "vancouver"],
            },
        ),
        SilverValidationCheck(
            name="flood_hazard_sources_are_expected",
            passed=metrics["source_names"] == ["calgary_flood_hazard", "vancouver_floodplain"],
            details={
                "actual": metrics["source_names"],
                "expected": ["calgary_flood_hazard", "vancouver_floodplain"],
            },
        ),
        SilverValidationCheck(
            name="flood_hazard_each_city_has_rows",
            passed=all(count > 0 for count in metrics["city_row_counts"].values()),
            details={"city_row_counts": metrics["city_row_counts"]},
        ),
        SilverValidationCheck(
            name="flood_hazard_key_not_null_and_unique",
            passed=metrics["key_nulls"] == 0 and metrics["key_duplicates"] == 0,
            details={
                "null_count": metrics["key_nulls"],
                "duplicate_count": metrics["key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="flood_hazard_geometry_not_null",
            passed=metrics["geometry_nulls"] == 0,
            details={"geometry_nulls": metrics["geometry_nulls"]},
        ),
        SilverValidationCheck(
            name="flood_hazard_geometry_types_are_polygonal",
            passed=metrics["unexpected_geometry_type_count"] == 0,
            details={
                "geometry_type_counts": metrics["geometry_type_counts"],
                "unexpected_geometry_type_count": metrics["unexpected_geometry_type_count"],
                "expected_geometry_types": ["Polygon", "MultiPolygon"],
            },
        ),
        SilverValidationCheck(
            name="flood_hazard_class_not_null",
            passed=metrics["hazard_class_nulls"] == 0,
            details={"hazard_class_nulls": metrics["hazard_class_nulls"]},
        ),
        SilverValidationCheck(
            name="flood_hazard_source_record_count_valid",
            passed=metrics["source_record_count_invalid"] == 0,
            details={
                "source_record_count_invalid": metrics["source_record_count_invalid"],
                "source_record_count_max": metrics["source_record_count_max"],
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="municipal_flood_hazard_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={"silver_flood_hazard_zone": flood_path.as_posix()},
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def collect_municipal_flood_hazard_metrics(
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    expected_geometry_types = {"Polygon", "MultiPolygon"}
    geometry_type_counts = dataframe["geometry_type"].value_counts(dropna=False).to_dict()

    unexpected_geometry_type_count = int(
        (~dataframe["geometry_type"].isin(expected_geometry_types)).sum()
    )

    city_row_counts = {
        str(key): int(value)
        for key, value in dataframe["city"].value_counts(dropna=False).to_dict().items()
    }

    return {
        "row_count": int(len(dataframe)),
        "cities": sorted(dataframe["city"].dropna().unique().tolist()),
        "source_names": sorted(dataframe["source_name"].dropna().unique().tolist()),
        "city_row_counts": city_row_counts,
        "key_nulls": int(dataframe["flood_hazard_zone_key"].isna().sum()),
        "key_duplicates": int(dataframe["flood_hazard_zone_key"].duplicated().sum()),
        "geometry_nulls": int(dataframe["geometry_wkt"].isna().sum()),
        "geometry_type_counts": {
            str(key): int(value) for key, value in geometry_type_counts.items()
        },
        "unexpected_geometry_type_count": unexpected_geometry_type_count,
        "hazard_class_nulls": int(dataframe["hazard_class"].isna().sum()),
        "source_record_count_invalid": int((dataframe["source_record_count"] < 1).sum()),
        "source_record_count_max": safe_series_max(dataframe["source_record_count"]),
    }


def validate_municipal_property_assessment_silver_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    expected_assessment_year: int = 2026,
    output_json_path: str | Path | None = None,
) -> SilverValidationReport:
    """Validate Silver municipal property assessment outputs."""
    silver_root = Path(silver_root)

    assessment_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_property_assessment",
    )

    dataframe = pd.read_parquet(assessment_path)
    metrics = collect_municipal_property_assessment_metrics(dataframe)

    checks = [
        SilverValidationCheck(
            name="property_assessment_row_count_gt_zero",
            passed=metrics["row_count"] > 0,
            details={"row_count": metrics["row_count"]},
        ),
        SilverValidationCheck(
            name="property_assessment_city_is_calgary",
            passed=metrics["cities"] == ["calgary"],
            details={"actual": metrics["cities"], "expected": ["calgary"]},
        ),
        SilverValidationCheck(
            name="property_assessment_year_matches_expected",
            passed=metrics["assessment_years"] == [expected_assessment_year],
            details={
                "actual": metrics["assessment_years"],
                "expected": [expected_assessment_year],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_key_not_null_and_unique",
            passed=metrics["key_nulls"] == 0 and metrics["key_duplicates"] == 0,
            details={
                "null_count": metrics["key_nulls"],
                "duplicate_count": metrics["key_duplicates"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_source_ids_not_null",
            passed=metrics["source_property_id_nulls"] == 0
            and metrics["source_unique_key_nulls"] == 0,
            details={
                "source_property_id_nulls": metrics["source_property_id_nulls"],
                "source_unique_key_nulls": metrics["source_unique_key_nulls"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_value_present_and_non_negative",
            passed=metrics["assessed_value_nulls"] == 0
            and metrics["negative_assessed_value_count"] == 0,
            details={
                "assessed_value_nulls": metrics["assessed_value_nulls"],
                "negative_assessed_value_count": metrics["negative_assessed_value_count"],
                "assessed_value_min": metrics["assessed_value_min"],
                "assessed_value_max": metrics["assessed_value_max"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_address_not_null",
            passed=metrics["address_nulls"] == 0,
            details={"address_nulls": metrics["address_nulls"]},
        ),
        SilverValidationCheck(
            name="property_assessment_geometry_not_null",
            passed=metrics["geometry_nulls"] == 0,
            details={"geometry_nulls": metrics["geometry_nulls"]},
        ),
        SilverValidationCheck(
            name="property_assessment_coordinates_not_null",
            passed=metrics["latitude_nulls"] == 0 and metrics["longitude_nulls"] == 0,
            details={
                "latitude_nulls": metrics["latitude_nulls"],
                "longitude_nulls": metrics["longitude_nulls"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_coordinates_in_calgary_range",
            passed=metrics["latitude_out_of_range"] == 0 and metrics["longitude_out_of_range"] == 0,
            details={
                "latitude_out_of_range": metrics["latitude_out_of_range"],
                "longitude_out_of_range": metrics["longitude_out_of_range"],
                "latitude_range": [50.8, 51.3],
                "longitude_range": [-114.4, -113.7],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_class_not_null",
            passed=metrics["assessment_class_description_nulls"] == 0,
            details={
                "assessment_class_description_nulls": metrics["assessment_class_description_nulls"],
                "assessment_class_counts": metrics["assessment_class_counts"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_land_size_non_negative",
            passed=metrics["negative_land_size_count"] == 0,
            details={
                "negative_land_size_count": metrics["negative_land_size_count"],
                "land_size_sm_nulls": metrics["land_size_sm_nulls"],
            },
        ),
        SilverValidationCheck(
            name="property_assessment_source_record_count_valid",
            passed=metrics["source_record_count_invalid"] == 0,
            details={
                "source_record_count_invalid": metrics["source_record_count_invalid"],
                "source_record_count_max": metrics["source_record_count_max"],
            },
        ),
    ]

    report = SilverValidationReport(
        validation_name="municipal_property_assessment_silver_validation",
        passed=all(check.passed for check in checks),
        checks=checks,
        output_paths={"silver_property_assessment": assessment_path.as_posix()},
    )

    if output_json_path is not None:
        write_json(output_json_path, report.to_dict())

    return report


def collect_municipal_property_assessment_metrics(
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    assessment_class_counts = (
        dataframe["assessment_class_description"].value_counts(dropna=False).head(20).to_dict()
    )

    land_size_columns = [
        column
        for column in ["land_size_sm", "land_size_sf", "land_size_ac"]
        if column in dataframe.columns
    ]

    negative_land_size_count = 0
    for column in land_size_columns:
        negative_land_size_count += int((dataframe[column].dropna() < 0).sum())

    return {
        "row_count": int(len(dataframe)),
        "cities": sorted(dataframe["city"].dropna().unique().tolist()),
        "assessment_years": sorted(
            int(value) for value in dataframe["assessment_year"].dropna().unique().tolist()
        ),
        "key_nulls": int(dataframe["property_assessment_key"].isna().sum()),
        "key_duplicates": int(dataframe["property_assessment_key"].duplicated().sum()),
        "source_property_id_nulls": int(dataframe["source_property_id"].isna().sum()),
        "source_unique_key_nulls": int(dataframe["source_unique_key"].isna().sum()),
        "assessed_value_nulls": int(dataframe["assessed_value_total"].isna().sum()),
        "negative_assessed_value_count": int(
            (dataframe["assessed_value_total"].dropna() < 0).sum()
        ),
        "assessed_value_min": safe_series_min(dataframe["assessed_value_total"].dropna()),
        "assessed_value_max": safe_series_max(dataframe["assessed_value_total"].dropna()),
        "address_nulls": int(dataframe["address_text"].isna().sum()),
        "geometry_nulls": int(dataframe["geometry_wkt"].isna().sum()),
        "latitude_nulls": int(dataframe["latitude"].isna().sum()),
        "longitude_nulls": int(dataframe["longitude"].isna().sum()),
        "latitude_out_of_range": int(
            ((dataframe["latitude"] < 50.8) | (dataframe["latitude"] > 51.3)).sum()
        ),
        "longitude_out_of_range": int(
            ((dataframe["longitude"] < -114.4) | (dataframe["longitude"] > -113.7)).sum()
        ),
        "assessment_class_description_nulls": int(
            dataframe["assessment_class_description"].isna().sum()
        ),
        "assessment_class_counts": {
            str(key): int(value) for key, value in assessment_class_counts.items()
        },
        "negative_land_size_count": negative_land_size_count,
        "land_size_sm_nulls": int(dataframe["land_size_sm"].isna().sum()),
        "source_record_count_invalid": int((dataframe["source_record_count"] < 1).sum()),
        "source_record_count_max": safe_series_max(dataframe["source_record_count"]),
    }
