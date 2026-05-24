from __future__ import annotations

import calendar
import sqlite3
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from src.silver.common import (
    SilverRunResult,
    append_jsonl,
    file_sha256,
    latest_successful_bronze_raw_path,
    utc_now_iso,
    utc_today,
    write_json,
    write_parquet,
)


TARGET_PROVINCES = {"BC", "AB"}

STATION_COLUMNS = [
    "STATION_NUMBER",
    "STATION_NAME",
    "PROV_TERR_STATE_LOC",
    "LATITUDE",
    "LONGITUDE",
    "DRAINAGE_AREA_GROSS",
    "DRAINAGE_AREA_EFFECT",
    "RHBN",
    "REAL_TIME",
]

FLOW_VALUE_PREFIX = "FLOW"
FLOW_SYMBOL_PREFIX = "FLOW_SYMBOL"
LEVEL_VALUE_PREFIX = "LEVEL"
LEVEL_SYMBOL_PREFIX = "LEVEL_SYMBOL"


def run_hydat_archive_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "hydat_archive"

    raw_path = latest_successful_bronze_raw_path(
        source_name=source_name,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    station_output_path = (
        output_root
        / "silver_hydro_station"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_hydro_station.parquet"
    )

    daily_output_root = (
        output_root / "silver_hydro_daily" / f"extract_date={extract_date}" / f"run_id={run_id}"
    )

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        sqlite_path = extract_hydat_sqlite(raw_path, Path(temp_dir))

        station_df = standardize_hydat_stations(sqlite_path)

        if station_df.empty:
            raise RuntimeError("HYDAT station Silver standardization produced zero rows.")

        write_parquet(station_output_path, station_df)

        output_tables = [
            table_output_metadata(
                table_name="silver_hydro_station",
                path=station_output_path,
                dataframe=station_df,
                source_raw_file=raw_path,
            )
        ]

        daily_output_tables, daily_summary = write_hydat_daily_partitions_streaming(
            sqlite_path=sqlite_path,
            station_df=station_df,
            output_root=daily_output_root,
            source_raw_file=raw_path,
        )

        output_tables.extend(daily_output_tables)

    if daily_summary["daily_row_count"] == 0:
        raise RuntimeError("HYDAT daily Silver standardization produced zero rows.")

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_raw_file_path": raw_path.as_posix(),
        "bronze_raw_file_checksum": file_sha256(raw_path),
        "silver_layer": "hydat_archive_standardization",
        "load_status": "success",
        "target_tables": ["silver_hydro_station", "silver_hydro_daily"],
        "output_tables": output_tables,
        "station_row_count": int(len(station_df)),
        "daily_row_count": daily_summary["daily_row_count"],
        "daily_year_min": daily_summary["daily_year_min"],
        "daily_year_max": daily_summary["daily_year_max"],
        "measurement_types": daily_summary["measurement_types"],
        "standardization_notes": {
            "station_filter": "Stations are filtered to PROV_TERR_STATE_LOC in BC, AB.",
            "daily_tables": "DLY_FLOWS and DLY_LEVELS are unpivoted year-by-year into one row per station-date-measurement type.",
            "grain": "silver_hydro_daily grain is station_id + observation_date + measurement_type.",
            "processing": "Daily HYDAT records are streamed by observation_year to avoid materializing the full archive in memory.",
            "flow_units": "HYDAT flow values are retained in source units, normally cubic metres per second.",
            "level_units": "HYDAT level values are retained in source units, normally metres.",
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
        "[OK] wrote HYDAT Silver outputs | "
        f"stations={len(station_df)} "
        f"daily_rows={daily_summary['daily_row_count']} "
        f"years={daily_summary['daily_year_min']}-{daily_summary['daily_year_max']} "
        f"types={daily_summary['measurement_types']} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def extract_hydat_sqlite(archive_path: str | Path, destination: Path) -> Path:
    archive_path = Path(archive_path)

    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(destination)

    candidates = sorted(destination.rglob("*.sqlite3")) + sorted(destination.rglob("*.sqlite"))

    if not candidates:
        raise FileNotFoundError(f"No SQLite database found inside {archive_path}")

    return candidates[0]


def standardize_hydat_stations(sqlite_path: str | Path) -> pd.DataFrame:
    with sqlite3.connect(sqlite_path) as conn:
        stations = pd.read_sql_query("SELECT * FROM STATIONS", conn)

    stations.columns = [column.upper() for column in stations.columns]

    required = {"STATION_NUMBER", "STATION_NAME", "PROV_TERR_STATE_LOC"}
    missing = required - set(stations.columns)

    if missing:
        raise ValueError(f"HYDAT STATIONS table missing required columns: {missing}")

    stations["province"] = stations["PROV_TERR_STATE_LOC"].map(normalize_province)

    stations = stations[stations["province"].isin(TARGET_PROVINCES)].copy()

    result = pd.DataFrame(
        {
            "station_id": stations["STATION_NUMBER"].astype(str),
            "station_name": stations["STATION_NAME"].map(clean_str),
            "province": stations["province"],
            "latitude": (
                stations.get("LATITUDE").map(safe_float) if "LATITUDE" in stations.columns else None
            ),
            "longitude": (
                stations.get("LONGITUDE").map(safe_float)
                if "LONGITUDE" in stations.columns
                else None
            ),
            "drainage_area_gross": (
                stations.get("DRAINAGE_AREA_GROSS").map(safe_float)
                if "DRAINAGE_AREA_GROSS" in stations.columns
                else None
            ),
            "drainage_area_effect": (
                stations.get("DRAINAGE_AREA_EFFECT").map(safe_float)
                if "DRAINAGE_AREA_EFFECT" in stations.columns
                else None
            ),
            "rhbn": stations.get("RHBN").map(clean_str) if "RHBN" in stations.columns else None,
            "real_time": (
                stations.get("REAL_TIME").map(clean_str)
                if "REAL_TIME" in stations.columns
                else None
            ),
            "source_name": "hydat_archive",
        }
    )

    result["geometry_type"] = "Point"
    result["geometry_wkt"] = result.apply(
        lambda row: point_wkt(row["longitude"], row["latitude"]),
        axis=1,
    )

    result = result.sort_values(["province", "station_id"]).reset_index(drop=True)

    return result


def standardize_hydat_daily_measurements(
    sqlite_path: str | Path,
    station_df: pd.DataFrame,
) -> pd.DataFrame:
    station_ids = set(station_df["station_id"].astype(str).tolist())

    flow_df = read_hydat_daily_table(
        sqlite_path=sqlite_path,
        table_name="DLY_FLOWS",
        measurement_type="flow",
        value_prefix=FLOW_VALUE_PREFIX,
        symbol_prefix=FLOW_SYMBOL_PREFIX,
        station_ids=station_ids,
    )

    level_df = read_hydat_daily_table(
        sqlite_path=sqlite_path,
        table_name="DLY_LEVELS",
        measurement_type="level",
        value_prefix=LEVEL_VALUE_PREFIX,
        symbol_prefix=LEVEL_SYMBOL_PREFIX,
        station_ids=station_ids,
    )

    daily = pd.concat([flow_df, level_df], ignore_index=True)

    if daily.empty:
        return daily

    daily = daily.merge(
        station_df[["station_id", "province", "latitude", "longitude"]],
        on="station_id",
        how="left",
    )

    daily["hydro_daily_key"] = (
        daily["station_id"].astype(str)
        + "_"
        + daily["observation_date"].astype(str)
        + "_"
        + daily["measurement_type"].astype(str)
    )

    daily = deduplicate_hydro_daily(daily)

    daily = daily.sort_values(["measurement_type", "station_id", "observation_date"]).reset_index(
        drop=True
    )

    return daily


def read_hydat_daily_table(
    *,
    sqlite_path: str | Path,
    table_name: str,
    measurement_type: str,
    value_prefix: str,
    symbol_prefix: str,
    station_ids: set[str],
) -> pd.DataFrame:
    with sqlite3.connect(sqlite_path) as conn:
        table_names = {
            row[0].upper()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }

        if table_name.upper() not in table_names:
            return pd.DataFrame()

        dataframe = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    if dataframe.empty:
        return dataframe

    dataframe.columns = [column.upper() for column in dataframe.columns]

    required = {"STATION_NUMBER", "YEAR", "MONTH"}
    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(f"HYDAT {table_name} table missing required columns: {missing}")

    dataframe = dataframe[dataframe["STATION_NUMBER"].astype(str).isin(station_ids)].copy()

    rows = []

    for record in dataframe.itertuples(index=False):
        values = record._asdict()
        station_id = str(values["STATION_NUMBER"])
        year = safe_int(values["YEAR"])
        month = safe_int(values["MONTH"])

        if year is None or month is None:
            continue

        if month < 1 or month > 12:
            continue

        max_day = calendar.monthrange(year, month)[1]

        for day in range(1, 32):
            if day > max_day:
                continue

            value_column = f"{value_prefix}{day}"
            symbol_column = f"{symbol_prefix}{day}"

            if value_column not in values:
                continue

            measurement_value = safe_float(values.get(value_column))

            if measurement_value is None:
                continue

            observation_date = f"{year:04d}-{month:02d}-{day:02d}"

            rows.append(
                {
                    "station_id": station_id,
                    "observation_date": observation_date,
                    "observation_year": year,
                    "observation_month": month,
                    "observation_day": day,
                    "measurement_type": measurement_type,
                    "measurement_value": measurement_value,
                    "measurement_symbol": clean_str(values.get(symbol_column)),
                    "grade_code": clean_str(values.get("GRADE_CODE")),
                    "source_table": table_name,
                    "source_name": "hydat_archive",
                }
            )

    return pd.DataFrame(rows)


def deduplicate_hydro_daily(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_source_record_count"] = working.groupby("hydro_daily_key")[
        "hydro_daily_key"
    ].transform("size")

    working = working.sort_values(
        ["hydro_daily_key", "measurement_value"],
        ascending=[True, False],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["hydro_daily_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_source_record_count"])


def write_daily_partitions(
    *,
    dataframe: pd.DataFrame,
    output_root: Path,
    source_raw_file: Path,
) -> list[dict[str, Any]]:
    output_tables = []

    for year in sorted(dataframe["observation_year"].dropna().unique().tolist()):
        year_df = dataframe[dataframe["observation_year"] == year].copy()

        output_path = output_root / f"observation_year={int(year)}" / "silver_hydro_daily.parquet"

        write_parquet(output_path, year_df)

        output_tables.append(
            table_output_metadata(
                table_name="silver_hydro_daily",
                path=output_path,
                dataframe=year_df,
                source_raw_file=source_raw_file,
                partition={"observation_year": int(year)},
            )
        )

        print(
            "[OK] wrote silver_hydro_daily partition | "
            f"year={int(year)} rows={len(year_df)} path={output_path}"
        )

    return output_tables


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
    source_raw_file: Path,
    partition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "table_name": table_name,
        "file_path": path.as_posix(),
        "file_name": path.name,
        "file_size_bytes": path.stat().st_size,
        "file_checksum": file_sha256(path),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": list(dataframe.columns),
        "source_raw_file_path": source_raw_file.as_posix(),
        "source_raw_file_checksum": file_sha256(source_raw_file),
    }

    if partition is not None:
        payload["partition"] = partition

    return payload


def normalize_province(value: Any) -> str | None:
    text = clean_str(value)

    if text is None:
        return None

    upper = text.upper()

    if upper in {"BC", "B.C.", "BRITISH COLUMBIA"}:
        return "BC"

    if upper in {"AB", "ALBERTA"}:
        return "AB"

    return upper


def point_wkt(longitude: Any, latitude: Any) -> str | None:
    lon = safe_float(longitude)
    lat = safe_float(latitude)

    if lon is None or lat is None:
        return None

    return f"POINT ({lon} {lat})"


def clean_str(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


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


def write_hydat_daily_partitions_streaming(
    *,
    sqlite_path: str | Path,
    station_df: pd.DataFrame,
    output_root: Path,
    source_raw_file: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    station_ids = set(station_df["station_id"].astype(str).tolist())

    flow_years = hydat_table_years(sqlite_path, "DLY_FLOWS")
    level_years = hydat_table_years(sqlite_path, "DLY_LEVELS")
    years = sorted(set(flow_years) | set(level_years))

    output_tables: list[dict[str, Any]] = []
    total_rows = 0
    written_years: list[int] = []
    measurement_types: set[str] = set()

    station_lookup = station_df[["station_id", "province", "latitude", "longitude"]]

    for year in years:
        year_frames = []

        flow_df = read_hydat_daily_table_for_year(
            sqlite_path=sqlite_path,
            table_name="DLY_FLOWS",
            year=year,
            measurement_type="flow",
            value_prefix=FLOW_VALUE_PREFIX,
            symbol_prefix=FLOW_SYMBOL_PREFIX,
            station_ids=station_ids,
        )

        if not flow_df.empty:
            year_frames.append(flow_df)

        level_df = read_hydat_daily_table_for_year(
            sqlite_path=sqlite_path,
            table_name="DLY_LEVELS",
            year=year,
            measurement_type="level",
            value_prefix=LEVEL_VALUE_PREFIX,
            symbol_prefix=LEVEL_SYMBOL_PREFIX,
            station_ids=station_ids,
        )

        if not level_df.empty:
            year_frames.append(level_df)

        if not year_frames:
            continue

        year_df = pd.concat(year_frames, ignore_index=True)

        year_df = year_df.merge(
            station_lookup,
            on="station_id",
            how="left",
        )

        year_df["hydro_daily_key"] = (
            year_df["station_id"].astype(str)
            + "_"
            + year_df["observation_date"].astype(str)
            + "_"
            + year_df["measurement_type"].astype(str)
        )

        year_df = deduplicate_hydro_daily(year_df)

        year_df = year_df.sort_values(
            ["measurement_type", "station_id", "observation_date"]
        ).reset_index(drop=True)

        output_path = output_root / f"observation_year={int(year)}" / "silver_hydro_daily.parquet"

        write_parquet(output_path, year_df)

        output_tables.append(
            table_output_metadata(
                table_name="silver_hydro_daily",
                path=output_path,
                dataframe=year_df,
                source_raw_file=source_raw_file,
                partition={"observation_year": int(year)},
            )
        )

        total_rows += int(len(year_df))
        written_years.append(int(year))
        measurement_types.update(year_df["measurement_type"].dropna().unique().tolist())

        print(
            "[OK] wrote silver_hydro_daily partition | "
            f"year={int(year)} rows={len(year_df)} path={output_path}"
        )

        del year_df
        del year_frames

    return output_tables, {
        "daily_row_count": total_rows,
        "daily_year_min": min(written_years) if written_years else None,
        "daily_year_max": max(written_years) if written_years else None,
        "measurement_types": sorted(measurement_types),
    }


def hydat_table_years(sqlite_path: str | Path, table_name: str) -> list[int]:
    with sqlite3.connect(sqlite_path) as conn:
        table_names = {
            row[0].upper()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }

        if table_name.upper() not in table_names:
            return []

        rows = conn.execute(
            f"SELECT DISTINCT YEAR FROM {table_name} WHERE YEAR IS NOT NULL"
        ).fetchall()

    years = []

    for row in rows:
        year = safe_int(row[0])
        if year is None:
            continue
        if year <= 0 or year > 2100:
            continue
        years.append(year)

    return sorted(set(years))


def read_hydat_daily_table_for_year(
    *,
    sqlite_path: str | Path,
    table_name: str,
    year: int,
    measurement_type: str,
    value_prefix: str,
    symbol_prefix: str,
    station_ids: set[str],
) -> pd.DataFrame:
    with sqlite3.connect(sqlite_path) as conn:
        table_names = {
            row[0].upper()
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }

        if table_name.upper() not in table_names:
            return pd.DataFrame()

        dataframe = pd.read_sql_query(
            f"SELECT * FROM {table_name} WHERE YEAR = ?",
            conn,
            params=[year],
        )

    if dataframe.empty:
        return dataframe

    dataframe.columns = [column.upper() for column in dataframe.columns]

    return unpivot_hydat_daily_dataframe(
        dataframe=dataframe,
        measurement_type=measurement_type,
        value_prefix=value_prefix,
        symbol_prefix=symbol_prefix,
        station_ids=station_ids,
    )


def unpivot_hydat_daily_dataframe(
    *,
    dataframe: pd.DataFrame,
    measurement_type: str,
    value_prefix: str,
    symbol_prefix: str,
    station_ids: set[str],
) -> pd.DataFrame:
    required = {"STATION_NUMBER", "YEAR", "MONTH"}
    missing = required - set(dataframe.columns)

    if missing:
        raise ValueError(f"HYDAT daily table missing required columns: {missing}")

    dataframe = dataframe[dataframe["STATION_NUMBER"].astype(str).isin(station_ids)].copy()

    rows = []

    for record in dataframe.itertuples(index=False):
        values = record._asdict()
        station_id = str(values["STATION_NUMBER"])
        year = safe_int(values["YEAR"])
        month = safe_int(values["MONTH"])

        if year is None or month is None:
            continue

        if year <= 0 or month < 1 or month > 12:
            continue

        max_day = calendar.monthrange(year, month)[1]

        for day in range(1, 32):
            if day > max_day:
                continue

            value_column = f"{value_prefix}{day}"
            symbol_column = f"{symbol_prefix}{day}"

            if value_column not in values:
                continue

            measurement_value = safe_float(values.get(value_column))

            if measurement_value is None:
                continue

            observation_date = f"{year:04d}-{month:02d}-{day:02d}"

            rows.append(
                {
                    "station_id": station_id,
                    "observation_date": observation_date,
                    "observation_year": year,
                    "observation_month": month,
                    "observation_day": day,
                    "measurement_type": measurement_type,
                    "measurement_value": measurement_value,
                    "measurement_symbol": clean_str(values.get(symbol_column)),
                    "grade_code": clean_str(values.get("GRADE_CODE")),
                    "source_table": measurement_type,
                    "source_name": "hydat_archive",
                }
            )

    return pd.DataFrame(rows)
