from __future__ import annotations

import json
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


FLOOD_SOURCES = {
    "vancouver_floodplain": {
        "city": "vancouver",
        "source_name": "vancouver_floodplain",
    },
    "calgary_flood_hazard": {
        "city": "calgary",
        "source_name": "calgary_flood_hazard",
    },
}

ZONE_CLASS_CANDIDATES = [
    "description",
    "flood_cd",
    "flood_code",
    "floodplain",
    "flood_plain",
    "flood_zone",
    "hazard",
    "hazard_class",
    "zone",
    "name",
    "type",
]

ZONE_ID_CANDIDATES = [
    "id",
    "objectid",
    "object_id",
    "globalid",
    "flood_cd",
    "flood_code",
    "zone_id",
]


def run_municipal_flood_hazard_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "municipal_flood_hazard"

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    frames = []
    source_inputs = []

    for source_config in FLOOD_SOURCES.values():
        raw_path = latest_successful_bronze_raw_path(
            source_name=source_config["source_name"],
            manifest_path=bronze_manifest_path,
        )

        dataframe = standardize_flood_geojson(
            raw_path,
            city=source_config["city"],
            source_name=source_config["source_name"],
        )

        if dataframe.empty:
            print(f"[WARN] no flood hazard rows produced for {source_config['source_name']}")
            continue

        frames.append(dataframe)
        source_inputs.append(
            {
                "source_name": source_config["source_name"],
                "city": source_config["city"],
                "raw_file_path": raw_path.as_posix(),
                "raw_file_checksum": file_sha256(raw_path),
                "row_count": int(len(dataframe)),
            }
        )

    if not frames:
        raise RuntimeError("Municipal flood hazard Silver produced zero rows.")

    result_df = pd.concat(frames, ignore_index=True)
    result_df = result_df.sort_values(["city", "flood_hazard_zone_key"]).reset_index(drop=True)

    output_path = (
        output_root
        / "silver_flood_hazard_zone"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_flood_hazard_zone.parquet"
    )

    write_parquet(output_path, result_df)

    output_tables = [
        table_output_metadata(
            table_name="silver_flood_hazard_zone",
            path=output_path,
            dataframe=result_df,
            source_inputs=source_inputs,
        )
    ]

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "silver_layer": "municipal_flood_hazard_standardization",
        "load_status": "success",
        "target_tables": ["silver_flood_hazard_zone"],
        "output_tables": output_tables,
        "source_inputs": source_inputs,
        "row_count": int(len(result_df)),
        "cities": sorted(result_df["city"].dropna().unique().tolist()),
        "geometry_types": result_df["geometry_type"].value_counts().to_dict(),
        "standardization_notes": {
            "grain": "One row per source flood hazard polygon feature.",
            "geometry": "Source GeoJSON geometries are stored as WKT.",
            "source_properties": "Original feature properties are retained as source_properties_json for auditability.",
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
        "[OK] wrote municipal flood hazard Silver outputs | "
        f"rows={len(result_df)} cities={metadata['cities']} run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_flood_geojson(
    path: str | Path,
    *,
    city: str,
    source_name: str,
) -> pd.DataFrame:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))

    features = payload.get("features", [])

    rows = []

    for index, feature in enumerate(features, start=1):
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}

        geometry_type = clean_str(geometry.get("type"))
        geometry_wkt = geometry_to_wkt(geometry)

        if not geometry_wkt:
            continue

        source_zone_id = first_non_empty(properties, ZONE_ID_CANDIDATES)
        hazard_class = first_non_empty(properties, ZONE_CLASS_CANDIDATES)

        if not source_zone_id:
            source_zone_id = f"feature_{index}"

        flood_hazard_zone_key = f"{city}_{source_name}_{source_zone_id}_{index}"

        rows.append(
            {
                "flood_hazard_zone_key": flood_hazard_zone_key,
                "city": city,
                "source_zone_id": clean_str(source_zone_id),
                "hazard_class": clean_str(hazard_class),
                "geometry_type": geometry_type,
                "geometry_wkt": geometry_wkt,
                "source_feature_index": index,
                "source_name": source_name,
                "source_properties_json": json.dumps(
                    properties,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    dataframe = pd.DataFrame(rows)

    if dataframe.empty:
        return dataframe

    dataframe = deduplicate_flood_zones(dataframe)

    return dataframe


def deduplicate_flood_zones(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_source_record_count"] = working.groupby("flood_hazard_zone_key")[
        "flood_hazard_zone_key"
    ].transform("size")

    working = working.sort_values(["flood_hazard_zone_key", "source_feature_index"])

    deduped = working.drop_duplicates(
        subset=["flood_hazard_zone_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_source_record_count"]).reset_index(drop=True)


def geometry_to_wkt(geometry: dict[str, Any]) -> str | None:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if not geometry_type or coordinates is None:
        return None

    if geometry_type == "Polygon":
        return polygon_to_wkt(coordinates)

    if geometry_type == "MultiPolygon":
        return multipolygon_to_wkt(coordinates)

    if geometry_type == "Point":
        if not coordinates or len(coordinates) < 2:
            return None
        return f"POINT ({coordinates[0]} {coordinates[1]})"

    if geometry_type == "LineString":
        return linestring_to_wkt(coordinates)

    return None


def polygon_to_wkt(coordinates: list[Any]) -> str:
    rings = [coordinate_ring_to_text(ring) for ring in coordinates]
    return f"POLYGON ({', '.join(rings)})"


def multipolygon_to_wkt(coordinates: list[Any]) -> str:
    polygons = []

    for polygon in coordinates:
        rings = [coordinate_ring_to_text(ring) for ring in polygon]
        polygons.append(f"({', '.join(rings)})")

    return f"MULTIPOLYGON ({', '.join(polygons)})"


def linestring_to_wkt(coordinates: list[Any]) -> str:
    points = [f"{point[0]} {point[1]}" for point in coordinates if len(point) >= 2]
    return f"LINESTRING ({', '.join(points)})"


def coordinate_ring_to_text(ring: list[Any]) -> str:
    points = [f"{point[0]} {point[1]}" for point in ring if len(point) >= 2]
    return f"({', '.join(points)})"


def first_non_empty(properties: dict[str, Any], candidates: list[str]) -> Any:
    normalized = {normalize_name(key): value for key, value in properties.items()}

    for candidate in candidates:
        value = normalized.get(normalize_name(candidate))

        if value is None:
            continue

        if isinstance(value, str) and value.strip() == "":
            continue

        return value

    return None


def normalize_name(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def clean_str(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
    source_inputs: list[dict[str, Any]],
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
        "source_inputs": source_inputs,
    }
