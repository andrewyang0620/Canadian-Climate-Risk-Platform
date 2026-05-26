from __future__ import annotations

import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
import shapefile

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

EVENT_ID_CANDIDATES = [
    "NFDBFIREID",
    "FIRE_ID",
    "CFS_REF_ID",
]

AGENCY_CANDIDATES = [
    "SRC_AGENCY",
    "AGENCY",
    "PROV",
    "PROVINCE",
    "PR",
]

YEAR_CANDIDATES = [
    "YEAR",
    "FIRE_YEAR",
]

REPORT_DATE_CANDIDATES = [
    "REP_DATE",
    "REPORT_DATE",
]

START_DATE_CANDIDATES = [
    "ATTK_DATE",
    "START_DATE",
]

OUT_DATE_CANDIDATES = [
    "OUT_DATE",
    "EXT_DATE",
]

SIZE_CANDIDATES = [
    "SIZE_HA",
    "AREA_HA",
]

CAUSE_CANDIDATES = [
    "CAUSE",
    "CAUSE_TYPE",
]


def run_wildfire_history_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "wildfire_history"

    raw_path = latest_successful_bronze_raw_path(
        source_name=source_name,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    with tempfile.TemporaryDirectory() as temp_dir:
        extract_root = Path(temp_dir) / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        extract_zip_recursive(raw_path, extract_root)

        shp_path = find_wildfire_shapefile(extract_root)
        dataframe = standardize_wildfire_shapefile(shp_path)

    if dataframe.empty:
        raise RuntimeError("Wildfire Silver standardization produced zero rows.")

    output_path = (
        output_root
        / "silver_wildfire_event"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_wildfire_event.parquet"
    )

    write_parquet(output_path, dataframe)

    output_tables = [
        table_output_metadata(
            table_name="silver_wildfire_event",
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
        "silver_layer": "wildfire_event_standardization",
        "load_status": "success",
        "target_tables": ["silver_wildfire_event"],
        "output_tables": output_tables,
        "row_count": int(len(dataframe)),
        "province_values": sorted(dataframe["province"].dropna().unique().tolist()),
        "fire_year_min": safe_int(dataframe["fire_year"].min()),
        "fire_year_max": safe_int(dataframe["fire_year"].max()),
        "standardization_notes": {
            "source": "Canadian National Fire Database point shapefile.",
            "province_filter": "Records are filtered to BC and Alberta using source agency/province fields first, then coordinate fallback.",
            "geometry": "Point geometry stored as longitude, latitude, and WKT.",
            "deduplication": "One row per wildfire_event_key; richest record retained if duplicate keys are found.",
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
        "[OK] wrote wildfire Silver outputs | "
        f"rows={len(dataframe)} "
        f"provinces={sorted(dataframe['province'].dropna().unique().tolist())} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def extract_zip_recursive(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(destination)

    nested_zips = sorted(destination.rglob("*.zip"))

    for nested_zip in nested_zips:
        nested_destination = nested_zip.parent / f"__extracted_{nested_zip.stem}"
        nested_destination.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(nested_zip, "r") as nested_archive:
            nested_archive.extractall(nested_destination)


def find_wildfire_shapefile(root: Path) -> Path:
    candidates = []

    for shp_path in sorted(root.rglob("*.shp")):
        reader = shapefile.Reader(str(shp_path))

        try:
            field_names = normalized_field_names(reader)

            has_event = any(normalize_name(field) in field_names for field in EVENT_ID_CANDIDATES)
            has_year = any(normalize_name(field) in field_names for field in YEAR_CANDIDATES)
            has_size = any(normalize_name(field) in field_names for field in SIZE_CANDIDATES)

            if has_event and has_year and has_size:
                candidates.append(shp_path)
        finally:
            close_shapefile_reader(reader)

    if not candidates:
        raise FileNotFoundError(
            f"No wildfire shapefile with event/year/size fields found under {root}"
        )

    return candidates[0]


def standardize_wildfire_shapefile(shp_path: str | Path) -> pd.DataFrame:
    reader = shapefile.Reader(str(shp_path))
    rows = []

    try:
        field_names = [field[0] for field in reader.fields[1:]]

        for shape_record in reader.iterShapeRecords():
            properties = dict(zip(field_names, list(shape_record.record)))
            geom = shape_record.shape

            longitude, latitude = point_coordinates(geom, properties)

            if longitude is None or latitude is None:
                continue

            province, province_method = infer_province(properties, latitude, longitude)

            if province not in TARGET_PROVINCES:
                continue

            event_id = first_non_empty(properties, EVENT_ID_CANDIDATES)
            fire_year = parse_fire_year(first_non_empty(properties, YEAR_CANDIDATES))
            size_ha = safe_float(first_non_empty(properties, SIZE_CANDIDATES))

            if not event_id:
                event_id = build_fallback_event_id(properties, latitude, longitude)

            wildfire_event_key = f"{province}_{event_id}"

            report_date = parse_date(first_non_empty(properties, REPORT_DATE_CANDIDATES))
            start_date = parse_date(first_non_empty(properties, START_DATE_CANDIDATES))
            out_date = parse_date(first_non_empty(properties, OUT_DATE_CANDIDATES))

            rows.append(
                {
                    "wildfire_event_key": wildfire_event_key,
                    "source_event_id": str(event_id),
                    "source_fire_id": clean_str(first_non_empty(properties, ["FIRE_ID"])),
                    "nfdb_fire_id": clean_str(first_non_empty(properties, ["NFDBFIREID"])),
                    "agency": clean_str(first_non_empty(properties, AGENCY_CANDIDATES)),
                    "province": province,
                    "province_inference_method": province_method,
                    "fire_year": fire_year,
                    "report_date": report_date,
                    "start_date": start_date,
                    "out_date": out_date,
                    "fire_size_ha": size_ha,
                    "fire_cause": clean_str(first_non_empty(properties, CAUSE_CANDIDATES)),
                    "latitude": latitude,
                    "longitude": longitude,
                    "geometry_type": geometry_type_name(geom),
                    "geometry_wkt": f"POINT ({longitude} {latitude})",
                    "source_name": "wildfire_history",
                    "source_layer": Path(shp_path).stem,
                }
            )
    finally:
        close_shapefile_reader(reader)

    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    dataframe = deduplicate_wildfire_events(dataframe)

    dataframe = dataframe.sort_values(
        ["province", "fire_year", "wildfire_event_key"],
        na_position="last",
    ).reset_index(drop=True)

    return dataframe


def deduplicate_wildfire_events(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_quality_score"] = (
        working["fire_year"].notna().astype(int)
        + working["report_date"].notna().astype(int)
        + working["fire_size_ha"].notna().astype(int)
        + working["latitude"].notna().astype(int)
        + working["longitude"].notna().astype(int)
    )

    working["_source_record_count"] = working.groupby("wildfire_event_key")[
        "wildfire_event_key"
    ].transform("size")

    working = working.sort_values(
        ["wildfire_event_key", "_quality_score", "fire_size_ha"],
        ascending=[True, False, False],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["wildfire_event_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_quality_score", "_source_record_count"])


def point_coordinates(
    geom: Any,
    properties: dict[str, Any],
) -> tuple[float | None, float | None]:
    longitude = safe_float(first_non_empty(properties, ["LONGITUDE", "LON", "X"]))
    latitude = safe_float(first_non_empty(properties, ["LATITUDE", "LAT", "Y"]))

    if longitude is not None and latitude is not None:
        return longitude, latitude

    if geom.points:
        x, y = geom.points[0]
        return safe_float(x), safe_float(y)

    return None, None


def infer_province(
    properties: dict[str, Any],
    latitude: float,
    longitude: float,
) -> tuple[str | None, str]:
    agency = clean_str(first_non_empty(properties, AGENCY_CANDIDATES))

    if agency:
        upper = agency.upper()

        if upper in {"BC", "B.C.", "BRITISH COLUMBIA"} or "BRITISH" in upper:
            return "BC", "source_agency"

        if upper in {"AB", "ALBERTA"} or "ALBERTA" in upper:
            return "AB", "source_agency"

    # Coordinate fallback. This is intentionally broad and should later be
    # replaced or confirmed by province boundary spatial join.
    #
    # Check Alberta before British Columbia because broad western province
    # bounding boxes overlap around the Rockies. Source agency remains the
    # preferred province signal when available.
    if 48.5 <= latitude <= 60.5 and -120.5 <= longitude <= -109.0:
        return "AB", "coordinate_bbox"

    if 48.0 <= latitude <= 60.5 and -139.5 <= longitude <= -114.0:
        return "BC", "coordinate_bbox"

    return None, "not_target_province"


def first_non_empty(
    properties: dict[str, Any],
    candidates: list[str],
) -> Any:
    normalized = {normalize_name(key): value for key, value in properties.items()}

    for candidate in candidates:
        value = normalized.get(normalize_name(candidate))

        if value is None:
            continue

        if isinstance(value, str) and value.strip() == "":
            continue

        return value

    return None


def normalized_field_names(reader: shapefile.Reader) -> set[str]:
    return {normalize_name(field[0]) for field in reader.fields[1:]}


def normalize_name(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


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


def parse_fire_year(value: Any) -> int | None:
    year = safe_int(value)

    if year is None:
        return None

    # NFDB uses sentinel values such as -999 for unknown dates/years.
    if year <= 0:
        return None

    # Keep the guard broad because the archive is historical, but reject impossible future years.
    if year > 2100:
        return None

    return year


def safe_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_date(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    if text in {"0000-00-00", "00000000", "0"}:
        return None

    # pyshp can return date objects, strings, or compact date integers.
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()[:10]
        except ValueError:
            return None

    digits = "".join(character for character in text if character.isdigit())

    if len(digits) >= 8:
        year = digits[:4]
        month = digits[4:6]
        day = digits[6:8]

        if year == "0000" or month == "00" or day == "00":
            return None

        return f"{year}-{month}-{day}"

    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]

    return None


def geometry_type_name(geom: Any) -> str:
    shape_type = getattr(geom, "shapeTypeName", None)

    if shape_type:
        return str(shape_type)

    return "Point"


def build_fallback_event_id(
    properties: dict[str, Any],
    latitude: float,
    longitude: float,
) -> str:
    year = safe_int(first_non_empty(properties, YEAR_CANDIDATES))
    size = safe_float(first_non_empty(properties, SIZE_CANDIDATES))
    return f"fallback_{year}_{round(latitude, 5)}_{round(longitude, 5)}_{size}"


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


def close_shapefile_reader(reader: shapefile.Reader) -> None:
    """Close pyshp file handles so Windows can delete temporary files."""
    close = getattr(reader, "close", None)

    if callable(close):
        close()
