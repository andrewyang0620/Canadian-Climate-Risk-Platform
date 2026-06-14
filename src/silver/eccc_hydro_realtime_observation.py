from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_eccc_hydro_realtime_observation_silver(
    raw_path: str | Path,
) -> pd.DataFrame:
    """Build Silver realtime hydrometric observation table from ECCC GeoJSON JSONL gzip."""
    raw_path = Path(raw_path)

    rows: list[dict[str, Any]] = []

    with gzip.open(raw_path, "rt", encoding="utf-8") as file:
        for source_record_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            feature = json.loads(line)
            rows.append(
                standardize_eccc_hydro_realtime_feature(
                    feature,
                    source_record_number=source_record_number,
                )
            )

    if not rows:
        raise RuntimeError("ECCC hydrometric realtime Silver produced zero rows.")

    dataframe = pd.DataFrame(rows)

    dataframe["observed_at_utc"] = pd.to_datetime(
        dataframe["observed_at_utc"],
        utc=True,
        errors="coerce",
    )

    dataframe["source_record_count"] = dataframe.groupby("hydro_realtime_observation_key")[
        "hydro_realtime_observation_key"
    ].transform("size")

    dataframe = dataframe.sort_values(
        ["station_id", "observed_at_utc", "source_record_number"],
        na_position="last",
    ).reset_index(drop=True)

    return dataframe


def standardize_eccc_hydro_realtime_feature(
    feature: dict[str, Any],
    *,
    source_record_number: int,
) -> dict[str, Any]:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}

    coordinates = geometry.get("coordinates") or []
    longitude = safe_float(coordinates[0]) if len(coordinates) >= 2 else None
    latitude = safe_float(coordinates[1]) if len(coordinates) >= 2 else None

    station_id = clean_text(properties.get("STATION_NUMBER"))
    observed_at_utc = clean_text(properties.get("DATETIME"))

    raw_water_level_m = safe_float(properties.get("LEVEL"))
    raw_discharge_cms = safe_float(properties.get("DISCHARGE"))

    negative_discharge_flag = raw_discharge_cms is not None and raw_discharge_cms < 0
    negative_water_level_flag = raw_water_level_m is not None and raw_water_level_m < 0

    discharge_cms = None if negative_discharge_flag else raw_discharge_cms
    water_level_m = raw_water_level_m

    return {
        "hydro_realtime_observation_key": build_observation_key(
            station_id=station_id,
            observed_at_utc=observed_at_utc,
        ),
        "source_name": "eccc_hydrometric_realtime",
        "source_feature_id": clean_text(feature.get("id")),
        "source_identifier": clean_text(properties.get("IDENTIFIER")),
        "station_id": station_id,
        "station_name": clean_text(properties.get("STATION_NAME")),
        "province_code": clean_text(properties.get("PROV_TERR_STATE_LOC")),
        "observed_at_utc": observed_at_utc,
        "observed_at_local": clean_text(properties.get("DATETIME_LST")),
        "latitude": latitude,
        "longitude": longitude,
        "geometry_wkt": build_point_wkt(longitude=longitude, latitude=latitude),
        "water_level_m": water_level_m,
        "discharge_cms": discharge_cms,
        "raw_water_level_m": raw_water_level_m,
        "raw_discharge_cms": raw_discharge_cms,
        "negative_water_level_flag": negative_water_level_flag,
        "negative_discharge_flag": negative_discharge_flag,
        "has_water_level": water_level_m is not None,
        "has_discharge": discharge_cms is not None,
        "has_any_measurement": water_level_m is not None or discharge_cms is not None,
        "level_symbol_en": clean_text(properties.get("LEVEL_SYMBOL_EN")),
        "level_symbol_fr": clean_text(properties.get("LEVEL_SYMBOL_FR")),
        "discharge_symbol_en": clean_text(properties.get("DISCHARGE_SYMBOL_EN")),
        "discharge_symbol_fr": clean_text(properties.get("DISCHARGE_SYMBOL_FR")),
        "source_record_number": source_record_number,
    }


def latest_eccc_hydrometric_realtime_raw_path(
    *,
    bronze_root: str | Path = "lakehouse/bronze",
) -> Path:
    candidates = sorted(
        Path(bronze_root).glob(
            "eccc_hydrometric_realtime/extract_date=*/run_id=*/raw/eccc_hydrometric_realtime_bc_ab.jsonl.gz"
        )
    )

    if not candidates:
        raise FileNotFoundError("No real ECCC hydrometric realtime Bronze JSONL gzip found.")

    return candidates[-1]


def build_observation_key(
    *,
    station_id: str | None,
    observed_at_utc: str | None,
) -> str:
    if not station_id or not observed_at_utc:
        raise ValueError("Hydro realtime observation key requires station_id and observed_at_utc.")

    digest = hashlib.md5(f"{station_id}|{observed_at_utc}".encode("utf-8")).hexdigest()[:16]

    return f"eccc_hydro_rt_{digest}"


def build_point_wkt(
    *,
    longitude: float | None,
    latitude: float | None,
) -> str | None:
    if longitude is None or latitude is None:
        return None

    return f"POINT ({longitude} {latitude})"


def clean_text(value: Any) -> str | None:
    if value is None:
        return None

    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    return text


def safe_float(value: Any) -> float | None:
    text = clean_text(value)

    if text is None:
        return None

    try:
        return float(text)
    except ValueError:
        return None
