from __future__ import annotations

import tempfile
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import shapefile
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union
from shapely.validation import make_valid

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

SOURCE_NAME = "wildfire_perimeter_polygons"
TARGET_TABLE = "silver_wildfire_perimeter_polygon"
TARGET_PROVINCES = {"BC", "AB"}
SOURCE_CRS_NAME = "NAD_1983_Lambert_Conformal_Conic"

REQUIRED_SOURCE_FIELDS = {
    "SRC_AGENCY",
    "FIRE_ID",
    "FIRENAME",
    "YEAR",
    "MONTH",
    "DAY",
    "REP_DATE",
    "OUT_DATE",
    "SIZE_HA",
    "CALC_HA",
    "CAUSE",
    "MAP_SOURCE",
    "SOURCE_KEY",
    "MAP_METHOD",
    "POLY_DATE",
    "CFS_REF_ID",
    "ACQ_DATE",
}


def run_wildfire_perimeter_polygon_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    raw_path = latest_successful_bronze_raw_path(
        source_name=SOURCE_NAME,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    dataframe = standardize_wildfire_perimeter_package(raw_path)

    if dataframe.empty:
        raise RuntimeError("Wildfire perimeter Silver standardization produced zero rows.")

    output_path = (
        output_root
        / TARGET_TABLE
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / f"{TARGET_TABLE}.parquet"
    )

    write_parquet(output_path, dataframe)

    output_tables = [
        table_output_metadata(
            table_name=TARGET_TABLE,
            path=output_path,
            dataframe=dataframe,
            source_raw_file=raw_path,
        )
    ]

    metadata = {
        "run_id": run_id,
        "source_name": SOURCE_NAME,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_raw_file_path": raw_path.as_posix(),
        "bronze_raw_file_checksum": file_sha256(raw_path),
        "silver_layer": "wildfire_perimeter_polygon_standardization",
        "load_status": "success",
        "target_tables": [TARGET_TABLE],
        "output_tables": output_tables,
        "row_count": int(len(dataframe)),
        "province_values": sorted(dataframe["province"].dropna().unique().tolist()),
        "fire_year_min": safe_int(dataframe["fire_year"].min()),
        "fire_year_max": safe_int(dataframe["fire_year"].max()),
        "geometry_repaired_count": int(dataframe["geometry_was_repaired"].sum()),
        "standardization_notes": {
            "source": "Canadian National Fire Database polygon shapefile package.",
            "source_files": sorted(dataframe["source_file"].dropna().unique().tolist()),
            "scope_filter": "Records are filtered to BC/AB only. Historical years are preserved in Silver.",
            "key_policy": "wildfire_perimeter_key is derived from CFS_REF_ID plus source file lineage. CFS_REF_ID is preserved as a natural source key but is not assumed equivalent to silver_wildfire_event keys.",
            "geometry": "Polygon/MultiPolygon WKT stored in source CRS after repair when needed.",
            "source_crs": SOURCE_CRS_NAME,
        },
    }

    metadata_path = (
        output_root
        / "_metadata"
        / SOURCE_NAME
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
        "[OK] wrote wildfire perimeter Silver outputs | "
        f"rows={len(dataframe)} "
        f"provinces={sorted(dataframe['province'].dropna().unique().tolist())} "
        f"years={safe_int(dataframe['fire_year'].min())}-{safe_int(dataframe['fire_year'].max())} "
        f"geometry_repairs={int(dataframe['geometry_was_repaired'].sum())} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=SOURCE_NAME,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_wildfire_perimeter_package(archive_path: str | Path) -> pd.DataFrame:
    archive_path = Path(archive_path)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        extract_root = Path(temp_dir) / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_root)

        shapefiles = find_wildfire_perimeter_shapefiles(extract_root)
        frames = [standardize_wildfire_perimeter_shapefile(path) for path in shapefiles]

    if not frames:
        raise FileNotFoundError(f"No wildfire perimeter shapefiles found in {archive_path}")

    dataframe = pd.concat(frames, ignore_index=True)

    dataframe = dataframe.sort_values(
        ["fire_year", "province", "cfs_ref_id", "source_file", "source_record_number"]
    ).reset_index(drop=True)

    validate_wildfire_perimeter_dataframe(dataframe)

    return dataframe


def find_wildfire_perimeter_shapefiles(root: Path) -> list[Path]:
    candidates: list[Path] = []

    for shp_path in sorted(root.rglob("*.shp")):
        reader = shapefile.Reader(str(shp_path))

        try:
            field_names = {field[0] for field in reader.fields[1:]}

            if REQUIRED_SOURCE_FIELDS.issubset(field_names):
                candidates.append(shp_path)
        finally:
            reader.close()

    if not candidates:
        raise FileNotFoundError(
            f"No NFDB polygon shapefile with required fields found under {root}"
        )

    return candidates


def standardize_wildfire_perimeter_shapefile(shp_path: Path) -> pd.DataFrame:
    reader = shapefile.Reader(str(shp_path))
    rows: list[dict[str, Any]] = []

    try:
        field_names = [field[0] for field in reader.fields[1:]]
        source_crs = read_source_crs(shp_path)

        for source_record_number, shape_record in enumerate(
            reader.iterShapeRecords(),
            start=1,
        ):
            properties = dict(zip(field_names, list(shape_record.record)))

            province = clean_str(properties.get("SRC_AGENCY")).upper()
            fire_year = safe_int(properties.get("YEAR"))

            if province not in TARGET_PROVINCES:
                continue

            if fire_year is None:
                continue

            cfs_ref_id = clean_str(properties.get("CFS_REF_ID"))

            if not cfs_ref_id:
                continue

            source_geometry = shapely_shape(shape_record.shape.__geo_interface__)
            geometry, geometry_was_repaired = repair_geometry(source_geometry)

            rows.append(
                {
                    "wildfire_perimeter_key": (
                        f"nfdb_poly__{cfs_ref_id}__{shp_path.stem}__record_{source_record_number}"
                    ),
                    "cfs_ref_id": cfs_ref_id,
                    "source_fire_id": clean_str(properties.get("FIRE_ID")),
                    "source_key": clean_str(properties.get("SOURCE_KEY")),
                    "source_agency": province,
                    "province": province,
                    "fire_name": clean_str(properties.get("FIRENAME")),
                    "fire_year": fire_year,
                    "fire_month": safe_int(properties.get("MONTH")),
                    "fire_day": safe_int(properties.get("DAY")),
                    "report_date": parse_date(properties.get("REP_DATE")),
                    "out_date": parse_date(properties.get("OUT_DATE")),
                    "polygon_date": parse_date(properties.get("POLY_DATE")),
                    "acquired_date": parse_date(properties.get("ACQ_DATE")),
                    "date_type": clean_str(properties.get("DATE_TYPE")),
                    "decade": clean_str(properties.get("DECADE")),
                    "source_size_ha": safe_float(properties.get("SIZE_HA")),
                    "calculated_size_ha": safe_float(properties.get("CALC_HA")),
                    "fire_cause": clean_str(properties.get("CAUSE")),
                    "prescribed": clean_str(properties.get("PRESCRIBED")),
                    "map_source": clean_str(properties.get("MAP_SOURCE")),
                    "map_method": clean_str(properties.get("MAP_METHOD")),
                    "water_removed": clean_str(properties.get("WATER_REM")),
                    "unburned_removed": clean_str(properties.get("UNBURN_REM")),
                    "more_info": clean_str(properties.get("MORE_INFO")),
                    "cfs_note1": clean_str(properties.get("CFS_NOTE1")),
                    "cfs_note2": clean_str(properties.get("CFS_NOTE2")),
                    "agency_source_file": clean_str(properties.get("AG_SRCFILE")),
                    "geometry_type": geometry.geom_type,
                    "geometry_wkt": geometry.wkt,
                    "geometry_original_is_valid": bool(source_geometry.is_valid),
                    "geometry_was_repaired": bool(geometry_was_repaired),
                    "geometry_is_valid": bool(geometry.is_valid),
                    "source_crs": source_crs,
                    "source_name": SOURCE_NAME,
                    "source_layer": "NFDB_poly",
                    "source_file": shp_path.name,
                    "source_record_number": source_record_number,
                }
            )
    finally:
        reader.close()

    return pd.DataFrame(rows)


def repair_geometry(geometry):
    if geometry.is_valid:
        return geometry, False

    repaired = make_valid(geometry)
    polygonal = polygonal_component(repaired)

    if polygonal is None or polygonal.is_empty:
        fallback = geometry.buffer(0)
        polygonal = polygonal_component(fallback)

    if polygonal is not None and not polygonal.is_empty:
        repaired = polygonal

    if not repaired.is_valid:
        fallback = geometry.buffer(0)
        fallback_polygonal = polygonal_component(fallback)

        if fallback_polygonal is not None and not fallback_polygonal.is_empty:
            repaired = fallback_polygonal

    return repaired, True


def polygonal_component(geometry):
    if isinstance(geometry, (Polygon, MultiPolygon)):
        return geometry

    if isinstance(geometry, GeometryCollection):
        polygons = []

        for part in geometry.geoms:
            component = polygonal_component(part)

            if component is None:
                continue

            if isinstance(component, Polygon):
                polygons.append(component)
            elif isinstance(component, MultiPolygon):
                polygons.extend(component.geoms)

        if not polygons:
            return None

        return unary_union(polygons)

    return None


def validate_wildfire_perimeter_dataframe(dataframe: pd.DataFrame) -> None:
    required_columns = {
        "wildfire_perimeter_key",
        "cfs_ref_id",
        "province",
        "fire_year",
        "geometry_type",
        "geometry_wkt",
        "geometry_is_valid",
        "source_crs",
    }

    missing = required_columns - set(dataframe.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if dataframe.empty:
        raise ValueError("Wildfire perimeter dataframe is empty.")

    if dataframe["wildfire_perimeter_key"].isna().any():
        raise ValueError("wildfire_perimeter_key contains null values.")

    duplicate_key_count = int(dataframe["wildfire_perimeter_key"].duplicated().sum())

    if duplicate_key_count:
        raise ValueError(f"Duplicate wildfire_perimeter_key rows: {duplicate_key_count}")

    invalid_province_count = int((~dataframe["province"].isin(TARGET_PROVINCES)).sum())

    if invalid_province_count:
        raise ValueError(f"Invalid province rows: {invalid_province_count}")

    null_year_count = int(dataframe["fire_year"].isna().sum())

    if null_year_count:
        raise ValueError(f"fire_year contains null values: {null_year_count}")

    invalid_geometry_count = int((~dataframe["geometry_is_valid"]).sum())

    if invalid_geometry_count:
        raise ValueError(f"Invalid geometries after repair: {invalid_geometry_count}")


def read_source_crs(shp_path: Path) -> str:
    prj_path = shp_path.with_suffix(".prj")

    if not prj_path.exists():
        return "unknown"

    prj_text = prj_path.read_text(encoding="utf-8", errors="ignore")

    if SOURCE_CRS_NAME in prj_text:
        return SOURCE_CRS_NAME

    if prj_text.startswith("PROJCS["):
        return prj_text.split(",", maxsplit=1)[0].replace("PROJCS[", "").strip('"')

    return "unknown"


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
    source_raw_file: Path,
) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "path": path.as_posix(),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "source_raw_file": source_raw_file.as_posix(),
        "key_column": "wildfire_perimeter_key",
        "key_unique_count": int(dataframe["wildfire_perimeter_key"].nunique()),
        "province_values": sorted(dataframe["province"].dropna().unique().tolist()),
        "fire_year_min": safe_int(dataframe["fire_year"].min()),
        "fire_year_max": safe_int(dataframe["fire_year"].max()),
    }


def clean_str(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_date(value: Any) -> str | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()

    if not text:
        return None

    if text.lower() in {"none", "null", "nan"}:
        return None

    for date_format in ["%Y-%m-%d", "%Y%m%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue

    return text
