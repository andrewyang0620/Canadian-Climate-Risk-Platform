from __future__ import annotations

import re
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import fiona
import pandas as pd
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, shape
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


SOURCE_NAME = "national_hydrometric_basin_polygons"

SILVER_TABLES = {
    "drainage_basin": "silver_hydro_basin_polygon",
    "pour_point": "silver_hydro_basin_pour_point",
    "station_point": "silver_hydro_basin_station_point",
}


def run_hydro_basin_polygon_silver(
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

    tables = standardize_hydro_basin_package(raw_path)
    validate_hydro_basin_tables(tables)

    output_tables = []

    for layer_name, table_name in SILVER_TABLES.items():
        dataframe = tables[table_name]
        output_path = (
            output_root
            / table_name
            / f"extract_date={extract_date}"
            / f"run_id={run_id}"
            / f"{table_name}.parquet"
        )

        write_parquet(output_path, dataframe)

        output_tables.append(
            table_output_metadata(
                table_name=table_name,
                path=output_path,
                dataframe=dataframe,
                source_raw_file=raw_path,
            )
        )

    metadata = {
        "run_id": run_id,
        "source_name": SOURCE_NAME,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "bronze_raw_file_path": raw_path.as_posix(),
        "bronze_raw_file_checksum": file_sha256(raw_path),
        "silver_layer": "hydro_basin_polygon_standardization",
        "load_status": "success",
        "target_tables": list(SILVER_TABLES.values()),
        "output_tables": output_tables,
        "row_counts": {
            table_name: int(len(tables[table_name])) for table_name in SILVER_TABLES.values()
        },
        "station_id_counts": {
            table_name: int(tables[table_name]["station_id"].nunique())
            for table_name in SILVER_TABLES.values()
        },
        "standardization_notes": {
            "source_package": (
                "Each MDA_ADP chunk contains DrainageBasin, PourPoint, and Station "
                "GeoJSON layers."
            ),
            "join_key": "Source StationNum is standardized to station_id.",
            "crs": "Source CRS is retained as source_crs; downstream spatial processing should reproject to EPSG:3347.",
            "geometry": "Geometry is stored as WKT to match existing Silver spatial conventions.",
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
        "[OK] wrote Hydro basin polygon Silver outputs | "
        f"polygon_rows={len(tables['silver_hydro_basin_polygon'])} "
        f"pour_point_rows={len(tables['silver_hydro_basin_pour_point'])} "
        f"station_point_rows={len(tables['silver_hydro_basin_station_point'])} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=SOURCE_NAME,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_hydro_basin_package(archive_path: str | Path) -> dict[str, pd.DataFrame]:
    archive_path = Path(archive_path)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        geojson_files = extract_geojson_files_from_archive(
            archive_path=archive_path,
            destination=Path(temp_dir),
        )

        tables = {
            "silver_hydro_basin_polygon": read_hydro_basin_layer(
                geojson_files=geojson_files,
                layer_name="drainage_basin",
            ),
            "silver_hydro_basin_pour_point": read_hydro_basin_layer(
                geojson_files=geojson_files,
                layer_name="pour_point",
            ),
            "silver_hydro_basin_station_point": read_hydro_basin_layer(
                geojson_files=geojson_files,
                layer_name="station_point",
            ),
        }

    validate_hydro_basin_tables(tables)

    return tables


def extract_geojson_files_from_archive(
    *,
    archive_path: str | Path,
    destination: Path,
) -> list[Path]:
    archive_path = Path(archive_path)
    destination.mkdir(parents=True, exist_ok=True)

    outer_dir = destination / "outer"
    outer_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(outer_dir)

    nested_zip_paths = sorted(outer_dir.rglob("*.zip"))

    for nested_zip_path in nested_zip_paths:
        nested_output_dir = destination / nested_zip_path.stem
        nested_output_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(nested_zip_path, "r") as nested_archive:
            nested_archive.extractall(nested_output_dir)

    geojson_files = sorted(destination.rglob("*.geojson"))

    if not geojson_files:
        raise FileNotFoundError(f"No GeoJSON files found inside archive: {archive_path}")

    return geojson_files


def read_hydro_basin_layer(
    *,
    geojson_files: list[Path],
    layer_name: str,
) -> pd.DataFrame:
    matching_files = [
        path for path in geojson_files if classify_layer_from_filename(path.name) == layer_name
    ]

    if not matching_files:
        raise FileNotFoundError(f"No GeoJSON files found for layer_name={layer_name}")

    rows: list[dict[str, Any]] = []

    for path in matching_files:
        rows.extend(read_hydro_basin_geojson(path=path, layer_name=layer_name))

    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        raise ValueError(f"Hydro basin layer produced zero rows: {layer_name}")

    dataframe = dataframe.sort_values(["mda_adp_region", "station_id"]).reset_index(drop=True)

    return dataframe


def read_hydro_basin_geojson(
    *,
    path: str | Path,
    layer_name: str,
) -> list[dict[str, Any]]:
    path = Path(path)
    rows: list[dict[str, Any]] = []

    mda_adp_region = extract_mda_adp_region(path.name)

    with fiona.open(path) as source:
        source_crs = normalize_crs(source.crs)

        for feature in source:
            properties = dict(feature.get("properties") or {})
            station_id = clean_str(properties.get("StationNum"))

            if not station_id:
                raise ValueError(f"Missing StationNum in {path}")

            geometry = feature.get("geometry")

            if geometry is None:
                raise ValueError(f"Missing geometry for station_id={station_id} in {path}")

            source_geometry = shape(geometry)
            shapely_geometry, geometry_was_repaired = repair_geometry(
                source_geometry,
                layer_name=layer_name,
            )

            base_row = {
                "station_id": station_id,
                "station_name": clean_str(properties.get("NameNom")),
                "status": clean_str(properties.get("Status")),
                "status_fr": clean_str(properties.get("Etat")),
                "geometry_type": shapely_geometry.geom_type,
                "geometry_wkt": shapely_geometry.wkt,
                "geometry_original_is_valid": bool(source_geometry.is_valid),
                "geometry_was_repaired": bool(geometry_was_repaired),
                "geometry_is_valid": bool(shapely_geometry.is_valid),
                "source_crs": source_crs,
                "source_name": SOURCE_NAME,
                "source_layer": layer_name,
                "source_file": path.name,
                "mda_adp_region": mda_adp_region,
            }

            if layer_name == "drainage_basin":
                row = {
                    "hydro_basin_polygon_key": station_id,
                    **base_row,
                    "basin_area_sq_km": safe_float(properties.get("Area_km2")),
                    "basin_area_sq_km_fr": safe_float(properties.get("Aire_km2")),
                    "remark": clean_str(properties.get("Remark")),
                    "remark_fr": clean_str(properties.get("Remarque")),
                    "source_version": clean_str(properties.get("Version")),
                    "source_revision_date": clean_str(properties.get("Date_rev")),
                    "shape_length_m": safe_float(properties.get("Shape_Leng")),
                    "shape_area_sq_m": safe_float(properties.get("Shape_Area")),
                }
            elif layer_name == "pour_point":
                row = {
                    "hydro_basin_pour_point_key": station_id,
                    **base_row,
                    "province_or_territory": clean_str(properties.get("ProvTerr")),
                }
            elif layer_name == "station_point":
                row = {
                    "hydro_basin_station_point_key": station_id,
                    **base_row,
                    "province_or_territory": clean_str(properties.get("ProvTerr")),
                    "hydat_version": clean_str(properties.get("HYDAT_ver")),
                }
            else:
                raise ValueError(f"Unsupported hydro basin layer: {layer_name}")

            rows.append(row)

    return rows


def repair_geometry(geometry, *, layer_name: str):
    """Repair invalid source geometries while preserving repair audit fields."""
    if geometry.is_valid:
        return geometry, False

    repaired = make_valid(geometry)

    if layer_name == "drainage_basin":
        polygonal = polygonal_component(repaired)

        if polygonal is None or polygonal.is_empty:
            fallback = geometry.buffer(0)
            polygonal = polygonal_component(fallback)

        if polygonal is not None and not polygonal.is_empty:
            repaired = polygonal

    if not repaired.is_valid:
        fallback = geometry.buffer(0)

        if layer_name == "drainage_basin":
            polygonal = polygonal_component(fallback)
            if polygonal is not None and not polygonal.is_empty:
                fallback = polygonal

        if fallback.is_valid and not fallback.is_empty:
            repaired = fallback

    return repaired, True


def polygonal_component(geometry):
    """Extract polygonal parts from a repaired geometry collection."""
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


def validate_hydro_basin_tables(tables: dict[str, pd.DataFrame]) -> None:
    expected_tables = set(SILVER_TABLES.values())
    actual_tables = set(tables)

    if actual_tables != expected_tables:
        raise ValueError(f"Expected tables {expected_tables}, got {actual_tables}")

    station_sets = {}

    for table_name, dataframe in tables.items():
        if dataframe.empty:
            raise ValueError(f"{table_name} is empty")

        required_columns = {"station_id", "geometry_type", "geometry_wkt", "source_crs"}
        missing_columns = required_columns - set(dataframe.columns)

        if missing_columns:
            raise ValueError(f"{table_name} missing columns: {sorted(missing_columns)}")

        if dataframe["station_id"].isna().any():
            raise ValueError(f"{table_name} has null station_id")

        duplicate_station_count = int(dataframe["station_id"].duplicated().sum())
        if duplicate_station_count:
            raise ValueError(
                f"{table_name} has duplicate station_id rows: {duplicate_station_count}"
            )

        invalid_geometry_count = int((~dataframe["geometry_is_valid"]).sum())
        if invalid_geometry_count:
            raise ValueError(f"{table_name} has invalid geometries: {invalid_geometry_count}")

        station_sets[table_name] = set(dataframe["station_id"].astype(str))

    baseline_table = "silver_hydro_basin_polygon"
    baseline_station_ids = station_sets[baseline_table]

    for table_name, station_ids in station_sets.items():
        if station_ids != baseline_station_ids:
            raise ValueError(
                f"Station ID mismatch between {baseline_table} and {table_name}: "
                f"baseline_only={len(baseline_station_ids - station_ids)} "
                f"other_only={len(station_ids - baseline_station_ids)}"
            )


def classify_layer_from_filename(filename: str) -> str | None:
    if "_DrainageBasin_" in filename:
        return "drainage_basin"

    if "_PourPoint_" in filename:
        return "pour_point"

    if "_Station." in filename:
        return "station_point"

    return None


def extract_mda_adp_region(filename: str) -> str:
    match = re.search(r"MDA_ADP_(\d{2})", filename)

    if not match:
        raise ValueError(f"Could not extract MDA_ADP region from filename: {filename}")

    return match.group(1)


def normalize_crs(crs: Any) -> str | None:
    if not crs:
        return None

    text = str(crs)

    if "4326" in text:
        return "EPSG:4326"

    return text


def clean_str(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    return text or None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    if pd.isna(number):
        return None

    return number


def table_output_metadata(
    *,
    table_name: str,
    path: str | Path,
    dataframe: pd.DataFrame,
    source_raw_file: str | Path,
) -> dict[str, Any]:
    path = Path(path)

    return {
        "table_name": table_name,
        "path": path.as_posix(),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "file_checksum": file_sha256(path),
        "source_raw_file": Path(source_raw_file).as_posix(),
    }
