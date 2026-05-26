from __future__ import annotations

import gzip
import json
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from src.silver.common import (
    SilverRunResult,
    append_jsonl,
    file_sha256,
    latest_successful_bronze_record,
    utc_now_iso,
    utc_today,
    write_json,
    write_parquet,
)


TARGET_PROVINCES = {"BC", "AB"}


def run_eccc_climate_daily_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "eccc_historical_climate"

    bronze_record = latest_successful_bronze_record(
        source_name=source_name,
        manifest_path=bronze_manifest_path,
    )

    raw_files = climate_raw_files_from_bronze_record(bronze_record)

    if not raw_files:
        raise FileNotFoundError("No ECCC climate yearly raw files found in latest Bronze record.")

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()

    output_root = Path(output_root)

    output_tables: list[dict[str, Any]] = []
    total_rows = 0
    all_years: list[int] = []

    for raw_file in raw_files:
        raw_path = Path(raw_file["raw_file_path"])

        if not raw_path.exists():
            raise FileNotFoundError(f"ECCC climate raw file does not exist: {raw_path}")

        dataframe = standardize_eccc_climate_jsonl_gzip(raw_path)

        if dataframe.empty:
            print(f"[WARN] no standardized climate rows produced from {raw_path}")
            continue

        years = sorted(dataframe["observation_year"].dropna().unique().tolist())

        for year in years:
            year_df = dataframe[dataframe["observation_year"] == year].copy()

            output_path = (
                output_root
                / "silver_climate_daily"
                / f"extract_date={extract_date}"
                / f"run_id={run_id}"
                / f"observation_year={int(year)}"
                / "silver_climate_daily.parquet"
            )

            write_parquet(output_path, year_df)

            output_tables.append(
                table_output_metadata(
                    table_name="silver_climate_daily",
                    path=output_path,
                    dataframe=year_df,
                    partition={"observation_year": int(year)},
                    source_raw_file=raw_path,
                )
            )

            total_rows += len(year_df)
            all_years.append(int(year))

            print(
                "[OK] wrote silver_climate_daily partition | "
                f"year={int(year)} rows={len(year_df)} path={output_path}"
            )

    if total_rows == 0:
        raise RuntimeError("ECCC climate Silver standardization produced zero rows.")

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_run_id": bronze_record.get("run_id"),
        "bronze_extract_timestamp": bronze_record.get("extract_timestamp"),
        "bronze_raw_file_count": len(raw_files),
        "bronze_raw_files": raw_files,
        "silver_layer": "climate_daily_standardization",
        "load_status": "success",
        "target_tables": ["silver_climate_daily"],
        "output_tables": output_tables,
        "row_count": total_rows,
        "observation_years": sorted(set(all_years)),
        "standardization_notes": {
            "province_filter": "Records are filtered to PROVINCE_CODE in BC, AB.",
            "geometry": "Point coordinates are stored as longitude/latitude columns.",
            "temperature_units": "ECCC daily temperature fields are standardized as Celsius.",
            "precipitation_units": "ECCC daily precipitation/rain fields are standardized as millimetres. Snow fields retain ECCC source units.",
        },
    }

    metadata_path = (
        output_root
        / "_metadata"
        / source_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "metadata.json"
    )

    write_json(metadata_path, metadata)

    manifest_record = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
        "manifest_record_created_at": utc_now_iso(),
    }

    append_jsonl(silver_manifest_path, manifest_record)

    print(
        "[OK] wrote ECCC climate Silver outputs | "
        f"rows={total_rows} years={sorted(set(all_years))} run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def climate_raw_files_from_bronze_record(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Return yearly ECCC climate raw files from a Bronze manifest record."""
    extra_metadata = record.get("extra_metadata") or {}
    year_outputs = extra_metadata.get("year_outputs")

    if isinstance(year_outputs, list) and year_outputs:
        return [
            {
                "year": item.get("year"),
                "raw_file_path": item["raw_file_path"],
                "record_count": item.get("record_count"),
                "file_checksum": item.get("file_checksum"),
            }
            for item in year_outputs
            if item.get("raw_file_path")
        ]

    raw_file_path = record.get("raw_file_path")
    if not raw_file_path:
        return []

    return [
        {
            "year": None,
            "raw_file_path": raw_file_path,
            "record_count": record.get("row_count"),
            "file_checksum": record.get("file_checksum"),
        }
    ]


def standardize_eccc_climate_jsonl_gzip(path: str | Path) -> pd.DataFrame:
    rows = []

    with gzip.open(path, "rt", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue

            feature = json.loads(stripped)
            row = normalize_eccc_climate_feature(feature)

            if row is not None:
                rows.append(row)

    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    dataframe = deduplicate_climate_daily_dataframe(dataframe)

    dataframe = dataframe.sort_values(["province", "station_id", "observation_date"]).reset_index(
        drop=True
    )

    return dataframe


def normalize_eccc_climate_feature(feature: dict[str, Any]) -> dict[str, Any] | None:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}

    province = clean_str(properties.get("PROVINCE_CODE"))

    if province not in TARGET_PROVINCES:
        return None

    coordinates = geometry.get("coordinates") or [None, None]
    longitude = safe_float(coordinates[0]) if len(coordinates) >= 1 else None
    latitude = safe_float(coordinates[1]) if len(coordinates) >= 2 else None

    observation_date = parse_local_date(properties.get("LOCAL_DATE"))

    if observation_date is None:
        return None

    station_id = clean_str(properties.get("CLIMATE_IDENTIFIER"))

    if not station_id:
        return None

    local_year = safe_int(properties.get("LOCAL_YEAR"))
    local_month = safe_int(properties.get("LOCAL_MONTH"))
    local_day = safe_int(properties.get("LOCAL_DAY"))

    return {
        "climate_daily_key": f"{station_id}_{observation_date}",
        "station_id": station_id,
        "station_name": clean_str(properties.get("STATION_NAME")),
        "province": province,
        "observation_date": observation_date,
        "observation_year": local_year or int(observation_date[:4]),
        "observation_month": local_month or int(observation_date[5:7]),
        "observation_day": local_day or int(observation_date[8:10]),
        "latitude": latitude,
        "longitude": longitude,
        "geometry_type": clean_str(geometry.get("type")),
        "mean_temp_c": safe_float(properties.get("MEAN_TEMPERATURE")),
        "min_temp_c": safe_float(properties.get("MIN_TEMPERATURE")),
        "max_temp_c": safe_float(properties.get("MAX_TEMPERATURE")),
        "total_precip_mm": safe_float(properties.get("TOTAL_PRECIPITATION")),
        "total_rain_mm": safe_float(properties.get("TOTAL_RAIN")),
        "total_snow": safe_float(properties.get("TOTAL_SNOW")),
        "snow_on_ground": safe_float(properties.get("SNOW_ON_GROUND")),
        "speed_max_gust": safe_float(properties.get("SPEED_MAX_GUST")),
        "direction_max_gust": safe_float(properties.get("DIRECTION_MAX_GUST")),
        "cooling_degree_days": safe_float(properties.get("COOLING_DEGREE_DAYS")),
        "heating_degree_days": safe_float(properties.get("HEATING_DEGREE_DAYS")),
        "min_relative_humidity": safe_float(properties.get("MIN_REL_HUMIDITY")),
        "max_relative_humidity": safe_float(properties.get("MAX_REL_HUMIDITY")),
        "mean_temp_flag": clean_str(properties.get("MEAN_TEMPERATURE_FLAG")),
        "min_temp_flag": clean_str(properties.get("MIN_TEMPERATURE_FLAG")),
        "max_temp_flag": clean_str(properties.get("MAX_TEMPERATURE_FLAG")),
        "total_precip_flag": clean_str(properties.get("TOTAL_PRECIPITATION_FLAG")),
        "total_rain_flag": clean_str(properties.get("TOTAL_RAIN_FLAG")),
        "total_snow_flag": clean_str(properties.get("TOTAL_SNOW_FLAG")),
        "source_feature_id": clean_str(feature.get("id") or properties.get("ID")),
        "source_name": "eccc_historical_climate",
    }


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
    partition: dict[str, Any],
    source_raw_file: Path,
) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "file_path": path.as_posix(),
        "file_name": path.name,
        "file_size_bytes": path.stat().st_size,
        "file_checksum": file_sha256(path),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": list(dataframe.columns),
        "partition": partition,
        "source_raw_file_path": source_raw_file.as_posix(),
        "source_raw_file_checksum": file_sha256(source_raw_file),
    }


def parse_local_date(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    return text[:10]


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def clean_str(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


CLIMATE_DEDUP_MEASUREMENT_COLUMNS = [
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


def deduplicate_climate_daily_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate to one row per station-date climate_daily_key."""
    if dataframe.empty:
        return dataframe

    measurement_columns = [
        column for column in CLIMATE_DEDUP_MEASUREMENT_COLUMNS if column in dataframe.columns
    ]

    working = dataframe.copy()

    if measurement_columns:
        working["_measurement_non_null_count"] = working[measurement_columns].notna().sum(axis=1)
    else:
        working["_measurement_non_null_count"] = 0

    working["_source_record_count"] = working.groupby("climate_daily_key")[
        "climate_daily_key"
    ].transform("size")

    working = working.sort_values(
        ["climate_daily_key", "_measurement_non_null_count", "source_feature_id"],
        ascending=[True, False, True],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["climate_daily_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(
        columns=["_measurement_non_null_count", "_source_record_count"]
    ).reset_index(drop=True)
