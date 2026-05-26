from __future__ import annotations

import math
import re
import uuid
import unicodedata
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


TARGET_PROVINCES = {"AB", "BC"}

COLUMN_CANDIDATES = {
    "event_id": [
        "EVENT_ID",
        "event id",
        "id",
        "disaster_id",
        "disaster id",
        "event number",
        "event_number",
    ],
    "event_category": [
        "EVENT_CATEGORY_NAME",
        "event category name",
        "event category",
        "event_category",
        "category",
        "disaster category",
    ],
    "event_group": [
        "EVENT_GROUP_NAME",
        "event group name",
        "event group",
        "event_group",
        "group",
        "disaster group",
    ],
    "event_subgroup": [
        "EVENT_SUBGROUP_NAME",
        "event subgroup name",
        "event subgroup",
        "event_subgroup",
        "subgroup",
        "disaster subgroup",
    ],
    "event_type_code": [
        "EVENT_TYPE",
        "event type",
        "event_type",
        "event code",
        "event_code",
    ],
    "event_type_description": [
        "EVENT_TYPE_DESCRIPTION",
        "event type description",
        "event_type_description",
        "type description",
        "hazard type",
        "disaster type",
    ],
    "province": [
        "PROVINCES_AFFECTED / PROVINCES AFFECTÉES",
        "PROVINCES_AFFECTED / PROVINCES AFFECTEES",
        "provinces affected provinces affectees",
        "provinces affected",
        "province affected",
        "province",
        "prov",
        "province/territory",
        "province territory",
        "province_territory",
    ],
    "location": [
        "PLACE",
        "place",
        "location",
        "location description",
        "location_description",
        "place name",
        "affected area",
    ],
    "start_date": [
        "EVENT_START_DATE",
        "event start date",
        "event_start_date",
        "start date",
        "start_date",
        "start",
        "begin date",
        "begin_date",
        "date start",
    ],
    "end_date": [
        "EVENT_END_DATE",
        "event end date",
        "event_end_date",
        "end date",
        "end_date",
        "end",
        "finish date",
        "finish_date",
        "date end",
    ],
    "fatalities": [
        "DEAD",
        "dead",
        "fatalities",
        "deaths",
        "killed",
    ],
    "injured": [
        "INJURED",
        "injured",
        "injured/infected",
        "injured infected",
        "injured_infected",
        "injuries",
    ],
    "evacuated": [
        "EVACUATED",
        "evacuated",
        "evacuees",
        "evacuated people",
        "number evacuated",
    ],
    "estimated_total_cost": [
        "TOTAL_COST",
        "total cost",
        "total_cost",
        "estimated total cost",
        "estimated_total_cost",
        "est total cost",
        "estimated cost",
        "cost",
    ],
    "description": [
        "COMMENT",
        "comments",
        "comment",
        "description",
        "remarks",
        "notes",
    ],
    "geometry": [
        "GEOG_OBJ",
        "geog obj",
        "geog_obj",
        "geometry",
        "wkt",
    ],
}


def run_canadian_disaster_database_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "canadian_disaster_database"

    raw_path = latest_successful_bronze_raw_path(
        source_name=source_name,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    dataframe = standardize_canadian_disaster_database(raw_path)

    if dataframe.empty:
        raise RuntimeError("Canadian Disaster Database Silver produced zero rows.")

    output_path = (
        output_root
        / "silver_disaster_event_month"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_disaster_event_month.parquet"
    )

    write_parquet(output_path, dataframe)

    output_tables = [
        table_output_metadata(
            table_name="silver_disaster_event_month",
            path=output_path,
            dataframe=dataframe,
            source_raw_file=raw_path,
        )
    ]

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_raw_file_path": raw_path.as_posix(),
        "bronze_raw_file_checksum": file_sha256(raw_path),
        "silver_layer": "disaster_event_month_standardization",
        "load_status": "success",
        "target_tables": ["silver_disaster_event_month"],
        "output_tables": output_tables,
        "row_count": int(len(dataframe)),
        "event_month_min": dataframe["event_month"].min(),
        "event_month_max": dataframe["event_month"].max(),
        "provinces": sorted(dataframe["province"].dropna().unique().tolist()),
        "standardization_notes": {
            "source": "Canadian Disaster Database Excel extract.",
            "province_filter": "Records are expanded/filtered to target provinces AB and BC.",
            "grain": "One row per source disaster event, target province, and active event month.",
            "date_logic": "Events spanning multiple months are expanded into monthly rows from start_date through end_date.",
            "cost_fields": "Cost fields are retained as numeric CAD values when present.",
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
        "[OK] wrote CDD Silver outputs | "
        f"rows={len(dataframe)} "
        f"provinces={metadata['provinces']} "
        f"months={metadata['event_month_min']}..{metadata['event_month_max']} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_canadian_disaster_database(path: str | Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=0, dtype=object)
    raw = raw.dropna(how="all").reset_index(drop=True)

    column_map = build_column_map(raw)

    rows = []

    for idx, record in raw.iterrows():
        row = record.to_dict()
        source_row_number = int(idx) + 1

        provinces = target_provinces_from_value(get_mapped_value(row, column_map, "province"))

        if not provinces:
            continue

        start_date = parse_date(get_mapped_value(row, column_map, "start_date"))
        end_date = parse_date(get_mapped_value(row, column_map, "end_date"))

        if start_date is None and end_date is None:
            continue

        if start_date is None:
            start_date = end_date

        if end_date is None:
            end_date = start_date

        event_months = event_month_range(start_date, end_date)

        if not event_months:
            continue

        source_event_id = clean_str(get_mapped_value(row, column_map, "event_id"))

        if not source_event_id:
            source_event_id = f"row_{source_row_number}"

        base_payload = {
            "source_event_id": source_event_id,
            "disaster_category": clean_str(get_mapped_value(row, column_map, "event_category")),
            "disaster_group": clean_str(get_mapped_value(row, column_map, "event_group")),
            "disaster_subgroup": clean_str(get_mapped_value(row, column_map, "event_subgroup")),
            "event_type_code": clean_str(get_mapped_value(row, column_map, "event_type_code")),
            "disaster_type": clean_str(get_mapped_value(row, column_map, "event_type_description"))
            or clean_str(get_mapped_value(row, column_map, "event_type_code")),
            "location_text": clean_str(get_mapped_value(row, column_map, "location")),
            "source_geometry": clean_str(get_mapped_value(row, column_map, "geometry")),
            "event_start_date": start_date,
            "event_end_date": end_date,
            "fatalities": safe_int(get_mapped_value(row, column_map, "fatalities")),
            "injured": safe_int(get_mapped_value(row, column_map, "injured")),
            "evacuated": safe_int(get_mapped_value(row, column_map, "evacuated")),
            "estimated_total_cost_cad": safe_float(
                get_mapped_value(row, column_map, "estimated_total_cost")
            ),
            "normalized_total_cost_cad": safe_float(
                get_mapped_value(row, column_map, "normalized_total_cost")
            ),
            "description": clean_str(get_mapped_value(row, column_map, "description")),
            "source_row_number": source_row_number,
            "source_name": "canadian_disaster_database",
        }

        for province in provinces:
            for event_month in event_months:
                disaster_event_month_key = f"{source_event_id}_{province}_{event_month[:7]}"

                rows.append(
                    {
                        "disaster_event_month_key": disaster_event_month_key,
                        "province": province,
                        "event_month": event_month,
                        "event_year": int(event_month[:4]),
                        "event_month_number": int(event_month[5:7]),
                        **base_payload,
                    }
                )

    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    dataframe = deduplicate_disaster_event_months(dataframe)

    return dataframe.sort_values(["province", "event_month", "source_event_id"]).reset_index(
        drop=True
    )


def build_column_map(dataframe: pd.DataFrame) -> dict[str, str | None]:
    normalized_to_original = {normalize_name(column): column for column in dataframe.columns}

    result: dict[str, str | None] = {}

    for logical_name, candidates in COLUMN_CANDIDATES.items():
        result[logical_name] = None

        for candidate in candidates:
            normalized_candidate = normalize_name(candidate)

            if normalized_candidate in normalized_to_original:
                result[logical_name] = normalized_to_original[normalized_candidate]
                break

    required = ["province", "start_date"]

    missing_required = [name for name in required if result.get(name) is None]

    if missing_required:
        raise ValueError(
            "CDD missing required columns "
            f"{missing_required}. Available columns: {list(dataframe.columns)}"
        )

    return result


def get_mapped_value(
    row: dict[str, Any],
    column_map: dict[str, str | None],
    logical_name: str,
) -> Any:
    column = column_map.get(logical_name)

    if column is None:
        return None

    return row.get(column)


def target_provinces_from_value(value: Any) -> list[str]:
    text = clean_str(value)

    if text is None:
        return []

    upper = text.upper().replace(".", "")

    provinces = []

    if "BRITISH COLUMBIA" in upper:
        provinces.append("BC")

    if "ALBERTA" in upper:
        provinces.append("AB")

    tokens = {token.strip() for token in re.split(r"[^A-Z]+", upper) if token.strip()}

    if "BC" in tokens:
        provinces.append("BC")

    if "AB" in tokens:
        provinces.append("AB")

    return sorted(set(provinces))


def event_month_range(start_date: str, end_date: str) -> list[str]:
    start_period = pd.Period(start_date[:7], freq="M")
    end_period = pd.Period(end_date[:7], freq="M")

    if end_period < start_period:
        end_period = start_period

    return [
        f"{period.year:04d}-{period.month:02d}-01"
        for period in pd.period_range(start_period, end_period, freq="M")
    ]


def deduplicate_disaster_event_months(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_source_record_count"] = working.groupby("disaster_event_month_key")[
        "disaster_event_month_key"
    ].transform("size")

    working["_quality_score"] = (
        working["disaster_type"].notna().astype(int)
        + working["event_start_date"].notna().astype(int)
        + working["event_end_date"].notna().astype(int)
        + working["location_text"].notna().astype(int)
    )

    working = working.sort_values(
        ["disaster_event_month_key", "_quality_score", "source_row_number"],
        ascending=[True, False, True],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["disaster_event_month_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_source_record_count", "_quality_score"])


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def parse_date(value: Any) -> str | None:
    if value is None or is_missing(value):
        return None

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        number = float(value)

        if 20000 <= number <= 60000:
            parsed = pd.to_datetime(number, unit="D", origin="1899-12-30", errors="coerce")
            if not pd.isna(parsed):
                return parsed.date().isoformat()

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date().isoformat()


def clean_str(value: Any) -> str | None:
    if value is None or is_missing(value):
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


def safe_float(value: Any) -> float | None:
    if value is None or is_missing(value):
        return None

    text = str(value).strip().replace("$", "").replace(",", "")

    if text == "":
        return None

    try:
        number = float(text)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(number):
        return None

    return number


def safe_int(value: Any) -> int | None:
    number = safe_float(value)

    if number is None:
        return None

    return int(number)


def is_missing(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
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
        "source_raw_file_path": source_raw_file.as_posix(),
        "source_raw_file_checksum": file_sha256(source_raw_file),
    }
