from __future__ import annotations

import re
import uuid
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


CALGARY_SOURCE_NAME = "calgary_property_assessment"


def run_municipal_property_assessment_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "municipal_property_assessment"

    raw_path = latest_successful_bronze_raw_path(
        source_name=CALGARY_SOURCE_NAME,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    dataframe = standardize_calgary_property_assessment(raw_path)

    if dataframe.empty:
        raise RuntimeError("Municipal property assessment Silver produced zero rows.")

    output_path = (
        output_root
        / "silver_property_assessment"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_property_assessment.parquet"
    )

    write_parquet(output_path, dataframe)

    output_tables = [
        table_output_metadata(
            table_name="silver_property_assessment",
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
        "silver_layer": "municipal_property_assessment_standardization",
        "load_status": "success",
        "target_tables": ["silver_property_assessment"],
        "output_tables": output_tables,
        "source_inputs": [
            {
                "source_name": CALGARY_SOURCE_NAME,
                "city": "calgary",
                "raw_file_path": raw_path.as_posix(),
                "raw_file_checksum": file_sha256(raw_path),
            }
        ],
        "row_count": int(len(dataframe)),
        "cities": sorted(dataframe["city"].dropna().unique().tolist()),
        "assessment_years": sorted(
            int(value) for value in dataframe["assessment_year"].dropna().unique()
        ),
        "standardization_notes": {
            "grain": "One row per Calgary property assessment record.",
            "identity": "property_assessment_key is based on city + source unique_key.",
            "geometry": "Source multipolygon WKT is retained as geometry_wkt; approximate centroid is derived from WKT coordinate bounds.",
            "values": "Assessment values are retained as numeric CAD values.",
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
        "[OK] wrote municipal property assessment Silver outputs | "
        f"rows={len(dataframe)} cities={metadata['cities']} "
        f"years={metadata['assessment_years']} run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_calgary_property_assessment(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype=object)
    raw.columns = [normalize_name(column) for column in raw.columns]

    required = {
        "roll_year",
        "roll_number",
        "assessed_value",
        "address",
        "multipolygon",
        "unique_key",
    }
    missing = required - set(raw.columns)

    if missing:
        raise ValueError(f"Calgary property assessment missing required columns: {sorted(missing)}")

    result = pd.DataFrame(
        {
            "property_assessment_key": "calgary_" + raw["unique_key"].astype(str),
            "city": "calgary",
            "source_property_id": raw["roll_number"].map(clean_str),
            "source_parcel_id": raw.get("cpid").map(clean_str) if "cpid" in raw.columns else None,
            "source_unique_key": raw["unique_key"].map(clean_str),
            "assessment_year": raw["roll_year"].map(safe_int),
            "address_text": raw["address"].map(clean_str),
            "assessed_value_total": raw["assessed_value"].map(safe_float),
            "assessed_value_residential": (
                raw.get("re_assessed_value").map(safe_float)
                if "re_assessed_value" in raw.columns
                else None
            ),
            "assessed_value_non_residential": (
                raw.get("nr_assessed_value").map(safe_float)
                if "nr_assessed_value" in raw.columns
                else None
            ),
            "assessed_value_farmland": (
                raw.get("fl_assessed_value").map(safe_float)
                if "fl_assessed_value" in raw.columns
                else None
            ),
            "assessment_class": (
                raw.get("assessment_class").map(clean_str)
                if "assessment_class" in raw.columns
                else None
            ),
            "assessment_class_description": (
                raw.get("assessment_class_description").map(clean_str)
                if "assessment_class_description" in raw.columns
                else None
            ),
            "community_code": (
                raw.get("comm_code").map(clean_str) if "comm_code" in raw.columns else None
            ),
            "community_name": (
                raw.get("comm_name").map(clean_str) if "comm_name" in raw.columns else None
            ),
            "year_of_construction": (
                raw.get("year_of_construction").map(safe_int)
                if "year_of_construction" in raw.columns
                else None
            ),
            "land_use_designation": (
                raw.get("land_use_designation").map(clean_str)
                if "land_use_designation" in raw.columns
                else None
            ),
            "property_type": (
                raw.get("property_type").map(clean_str) if "property_type" in raw.columns else None
            ),
            "sub_property_use": (
                raw.get("sub_property_use").map(clean_str)
                if "sub_property_use" in raw.columns
                else None
            ),
            "land_size_sm": (
                raw.get("land_size_sm").map(safe_float) if "land_size_sm" in raw.columns else None
            ),
            "land_size_sf": (
                raw.get("land_size_sf").map(safe_float) if "land_size_sf" in raw.columns else None
            ),
            "land_size_ac": (
                raw.get("land_size_ac").map(safe_float) if "land_size_ac" in raw.columns else None
            ),
            "source_modified_at": (
                raw.get("mod_date").map(clean_str) if "mod_date" in raw.columns else None
            ),
            "geometry_type": "MultiPolygon",
            "geometry_wkt": raw["multipolygon"].map(clean_str),
            "source_name": CALGARY_SOURCE_NAME,
        }
    )

    bounds = result["geometry_wkt"].map(extract_wkt_bounds)
    result["longitude"] = bounds.map(lambda value: value["centroid_lon"] if value else None)
    result["latitude"] = bounds.map(lambda value: value["centroid_lat"] if value else None)
    result["bbox_min_lon"] = bounds.map(lambda value: value["min_lon"] if value else None)
    result["bbox_min_lat"] = bounds.map(lambda value: value["min_lat"] if value else None)
    result["bbox_max_lon"] = bounds.map(lambda value: value["max_lon"] if value else None)
    result["bbox_max_lat"] = bounds.map(lambda value: value["max_lat"] if value else None)

    result = result.dropna(subset=["property_assessment_key"]).copy()

    result = deduplicate_property_assessments(result)

    result = result.sort_values(["city", "assessment_year", "property_assessment_key"]).reset_index(
        drop=True
    )

    return result


def deduplicate_property_assessments(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_source_record_count"] = working.groupby("property_assessment_key")[
        "property_assessment_key"
    ].transform("size")

    working["_quality_score"] = (
        working["assessed_value_total"].notna().astype(int)
        + working["geometry_wkt"].notna().astype(int)
        + working["address_text"].notna().astype(int)
        + working["assessment_year"].notna().astype(int)
    )

    working = working.sort_values(
        ["property_assessment_key", "_quality_score"],
        ascending=[True, False],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["property_assessment_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_source_record_count", "_quality_score"])


def extract_wkt_bounds(wkt: Any) -> dict[str, float] | None:
    text = clean_str(wkt)

    if text is None:
        return None

    numbers = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", text)]

    if len(numbers) < 4:
        return None

    if len(numbers) % 2 != 0:
        numbers = numbers[:-1]

    longitudes = numbers[0::2]
    latitudes = numbers[1::2]

    if not longitudes or not latitudes:
        return None

    min_lon = min(longitudes)
    max_lon = max(longitudes)
    min_lat = min(latitudes)
    max_lat = max(latitudes)

    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
        "centroid_lon": (min_lon + max_lon) / 2,
        "centroid_lat": (min_lat + max_lat) / 2,
    }


def normalize_name(value: str) -> str:
    return str(value).strip().lower()


def clean_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


def safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None

    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None

    if pd.isna(number):
        return None

    return number


def safe_int(value: Any) -> int | None:
    number = safe_float(value)

    if number is None:
        return None

    return int(number)


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
