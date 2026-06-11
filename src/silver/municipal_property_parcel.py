from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_vancouver_property_parcel_silver(raw_path: str | Path) -> pd.DataFrame:
    """Build Silver property parcel records from Vancouver parcel GeoJSON."""
    raw_path = Path(raw_path)

    with raw_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    features = payload.get("features") or []
    records: list[dict[str, Any]] = []

    for feature_index, feature in enumerate(features):
        props = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}

        civic_number = clean_text(props.get("civic_number"))
        street_name = clean_text(props.get("streetname"))
        source_tax_coord = clean_text(props.get("tax_coord"))
        source_parcel_id = clean_text(props.get("site_id"))

        geo_point = props.get("geo_point_2d") or {}
        longitude = safe_float(geo_point.get("lon"))
        latitude = safe_float(geo_point.get("lat"))

        geometry_type = clean_text(geometry.get("type"))
        geometry_wkt = geojson_geometry_to_wkt(geometry)

        records.append(
            {
                "property_parcel_key": build_property_parcel_key(
                    source_parcel_id=source_parcel_id,
                    source_tax_coord=source_tax_coord,
                    civic_number=civic_number,
                    street_name=street_name,
                    geometry=geometry,
                    feature_index=feature_index,
                ),
                "city": "vancouver",
                "province": "BC",
                "source_name": "vancouver_property_parcels",
                "source_parcel_id": source_parcel_id,
                "source_tax_coord": source_tax_coord,
                "civic_number": civic_number,
                "street_name": street_name,
                "address_text": build_address_text(civic_number, street_name),
                "latitude": latitude,
                "longitude": longitude,
                "geometry_type": geometry_type,
                "geometry_wkt": geometry_wkt,
                "source_record_count": 1,
            }
        )

    dataframe = pd.DataFrame.from_records(records)

    if dataframe.empty:
        raise RuntimeError("Vancouver property parcel Silver produced zero rows.")

    dataframe = clean_property_parcel_coordinates(dataframe)
    dataframe = dataframe.sort_values("property_parcel_key").reset_index(drop=True)

    return dataframe


def build_property_parcel_key(
    *,
    source_parcel_id: str | None,
    source_tax_coord: str | None,
    civic_number: str | None,
    street_name: str | None,
    geometry: dict[str, Any],
    feature_index: int,
) -> str:
    """Build a deterministic compact parcel key.

    site_id and tax_coord are not perfectly complete or unique, so we include a
    stable hash over identifying properties and geometry.
    """
    identity = {
        "source_parcel_id": source_parcel_id,
        "source_tax_coord": source_tax_coord,
        "civic_number": civic_number,
        "street_name": street_name,
        "geometry": geometry,
        "feature_index": feature_index,
    }

    digest = hashlib.md5(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"


def build_address_text(civic_number: str | None, street_name: str | None) -> str | None:
    parts = [part for part in [civic_number, street_name] if part]

    if not parts:
        return None

    return " ".join(parts)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    return text


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number


def clean_property_parcel_coordinates(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Null coordinates outside Vancouver's expected city envelope."""
    working = dataframe.copy()

    has_coordinates = working["latitude"].notna() & working["longitude"].notna()

    valid_coordinates = (
        (working["latitude"] >= 49.0)
        & (working["latitude"] <= 49.4)
        & (working["longitude"] >= -123.4)
        & (working["longitude"] <= -122.8)
    )

    invalid_mask = has_coordinates & ~valid_coordinates

    working.loc[invalid_mask, "latitude"] = None
    working.loc[invalid_mask, "longitude"] = None

    return working


def geojson_geometry_to_wkt(geometry: dict[str, Any]) -> str | None:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if geometry_type == "Polygon":
        return polygon_to_wkt(coordinates)

    if geometry_type == "MultiPolygon":
        return multipolygon_to_wkt(coordinates)

    return None


def polygon_to_wkt(coordinates: Any) -> str | None:
    if not coordinates:
        return None

    rings = []

    for ring in coordinates:
        ring_text = ", ".join(format_position(position) for position in ring)
        rings.append(f"({ring_text})")

    return f"POLYGON ({', '.join(rings)})"


def multipolygon_to_wkt(coordinates: Any) -> str | None:
    if not coordinates:
        return None

    polygons = []

    for polygon in coordinates:
        rings = []

        for ring in polygon:
            ring_text = ", ".join(format_position(position) for position in ring)
            rings.append(f"({ring_text})")

        polygons.append(f"({', '.join(rings)})")

    return f"MULTIPOLYGON ({', '.join(polygons)})"


def format_position(position: Any) -> str:
    longitude = format_number(position[0])
    latitude = format_number(position[1])

    return f"{longitude} {latitude}"


def format_number(value: Any) -> str:
    number = float(value)
    text = f"{number:.8f}"

    return text.rstrip("0").rstrip(".")
