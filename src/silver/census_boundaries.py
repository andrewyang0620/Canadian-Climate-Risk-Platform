from __future__ import annotations

import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import fiona
import pandas as pd
from shapely.geometry import shape

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

# Target Province
TARGET_PRUID_TO_ABBR = {
    "48": "AB",
    "59": "BC",
}

TARGET_PRUID_TO_NAME = {
    "48": "Alberta",
    "59": "British Columbia",
}


def run_census_boundary_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "census_boundaries"

    # latest successful path from bronze
    raw_path = latest_successful_bronze_raw_path(
        source_name=source_name,
        manifest_path=bronze_manifest_path,
    )

    # generate run identifier
    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()

    output_root = Path(output_root)

    # Extract to temp folder
    with tempfile.TemporaryDirectory() as temp_dir:
        extract_root = Path(temp_dir) / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        extract_zip_recursive(raw_path, extract_root)

        # Find the shapefile of prov/city
        province_layer = find_shapefile_by_required_columns(
            extract_root,
            required_columns=["PRUID", "PRNAME"],
        )
        csd_layer = find_shapefile_by_required_columns(
            extract_root,
            required_columns=["CSDUID", "CSDNAME"],
        )

        # standardlize tables
        province_df = standardize_province_boundaries(province_layer)
        municipality_df = standardize_municipality_boundaries(csd_layer)

    province_output = (
        output_root
        / "silver_boundary_province"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_boundary_province.parquet"
    )
    municipality_output = (
        output_root
        / "silver_boundary_municipality"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_boundary_municipality.parquet"
    )

    # write parquet files
    write_parquet(province_output, province_df)
    write_parquet(municipality_output, municipality_df)

    # generate metadata df
    output_tables = [
        table_output_metadata(
            table_name="silver_boundary_province",
            path=province_output,
            dataframe=province_df,
        ),
        table_output_metadata(
            table_name="silver_boundary_municipality",
            path=municipality_output,
            dataframe=municipality_df,
        ),
    ]

    # write metadata JSON
    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_raw_file_path": raw_path.as_posix(),
        "bronze_raw_file_checksum": file_sha256(raw_path),
        "silver_layer": "boundary_standardization",
        "load_status": "success",
        "target_tables": [
            "silver_boundary_province",
            "silver_boundary_municipality",
        ],
        "output_tables": output_tables,
        "standardization_notes": {
            "province_filter": "Filtered to PRUID 48 Alberta and 59 British Columbia.",
            "geometry_format": "Geometry is stored as WKT for portable local processing.",
            "source_boundary_year": 2021,
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
    # Add silver manifest
    append_jsonl(silver_manifest_path, manifest_record)

    print(
        "[OK] wrote census boundary Silver outputs | "
        f"province_rows={len(province_df)} | "
        f"municipality_rows={len(municipality_df)} | "
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
    # extract zio from zip file
    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(destination)

    nested_zips = sorted(destination.rglob("*.zip"))

    for nested_zip in nested_zips:
        nested_destination = nested_zip.parent / f"__extracted_{nested_zip.stem}"
        nested_destination.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(nested_zip, "r") as nested_archive:
            nested_archive.extractall(nested_destination)


def find_shapefile_by_required_columns(
    root: Path,
    *,
    required_columns: list[str],
) -> Path:
    # Find shapefile prov/city by COLNAME
    required = {normalize_name(column) for column in required_columns}

    for shp_path in sorted(root.rglob("*.shp")):
        with fiona.open(shp_path) as collection:
            schema = collection.schema or {}
            properties = schema.get("properties", {})
            columns = {normalize_name(column) for column in properties.keys()}

        if required <= columns:
            return shp_path

    raise FileNotFoundError(
        f"No shapefile under {root} contains required columns {required_columns}"
    )


def standardize_province_boundaries(shp_path: Path) -> pd.DataFrame:
    # return the prov bound
    rows = []

    with fiona.open(shp_path) as collection:
        crs = str(collection.crs_wkt or collection.crs)

        for feature in collection:
            props = dict(feature.get("properties") or {})
            pruid = str(props.get("PRUID", "")).zfill(2)

            if pruid not in TARGET_PRUID_TO_ABBR:
                continue

            geom = shape(feature["geometry"])

            rows.append(
                {
                    "province_key": TARGET_PRUID_TO_ABBR[pruid],
                    "province_code": pruid,
                    "province_name": TARGET_PRUID_TO_NAME[pruid],
                    "source_province_name": props.get("PRNAME"),
                    "province_abbr": TARGET_PRUID_TO_ABBR[pruid],
                    "land_area_sq_km": to_float(props.get("LANDAREA")),
                    "boundary_year": 2021,
                    "source_name": "census_boundaries",
                    "source_layer": shp_path.stem,
                    "geometry_type": geom.geom_type,
                    "geometry_wkt": geom.wkt,
                    "crs": crs,
                }
            )

    dataframe = pd.DataFrame(rows)
    return dataframe.sort_values(["province_key"]).reset_index(drop=True)


def standardize_municipality_boundaries(shp_path: Path) -> pd.DataFrame:
    # return city bounds
    rows = []

    with fiona.open(shp_path) as collection:
        crs = str(collection.crs_wkt or collection.crs)

        for feature in collection:
            props = dict(feature.get("properties") or {})
            pruid = str(props.get("PRUID", "")).zfill(2)

            if pruid not in TARGET_PRUID_TO_ABBR:
                continue

            geom = shape(feature["geometry"])
            csduid = str(props.get("CSDUID"))

            rows.append(
                {
                    "municipality_key": csduid,
                    "municipality_id": csduid,
                    "municipality_name": props.get("CSDNAME"),
                    "municipality_type": props.get("CSDTYPE"),
                    "province": TARGET_PRUID_TO_ABBR[pruid],
                    "province_key": TARGET_PRUID_TO_ABBR[pruid],
                    "province_code": pruid,
                    "province_name": TARGET_PRUID_TO_NAME[pruid],
                    "dguid": props.get("DGUID"),
                    "land_area_sq_km": to_float(props.get("LANDAREA")),
                    "boundary_year": 2021,
                    "source_name": "census_boundaries",
                    "source_layer": shp_path.stem,
                    "geometry_type": geom.geom_type,
                    "geometry_wkt": geom.wkt,
                    "crs": crs,
                }
            )

    dataframe = pd.DataFrame(rows)
    return dataframe.sort_values(["province", "municipality_key"]).reset_index(drop=True)


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
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
    }


def normalize_name(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def to_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None
