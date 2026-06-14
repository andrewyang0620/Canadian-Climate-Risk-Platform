from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

CALGARY_COORDINATE_BOUNDS = {
    "latitude_min": 50.8,
    "latitude_max": 51.3,
    "longitude_min": -114.4,
    "longitude_max": -113.7,
}


def build_calgary_development_permit_silver(raw_path: str | Path) -> pd.DataFrame:
    """Build Silver development permit records from Calgary development permits."""
    raw_path = Path(raw_path)

    raw = pd.read_csv(raw_path, dtype=object, low_memory=False)

    applied_date = clean_date_series(raw["applieddate"], min_year=1979, max_year=2030)
    decision_date = clean_date_series(raw["decisiondate"], min_year=1979, max_year=2026)
    release_date = clean_date_series(raw["releasedate"], min_year=1979, max_year=2026)
    must_commence_date = clean_date_series(
        raw["mustcommencedate"],
        min_year=1979,
        max_year=2035,
    )
    canceled_refused_date = clean_date_series(
        raw["canceledrefuseddate"],
        min_year=1979,
        max_year=2026,
    )
    sdab_hearing_date = clean_date_series(
        raw["sdabhearingdate"],
        min_year=1979,
        max_year=2026,
    )
    sdab_decision_date = clean_date_series(
        raw["sdabdecisiondate"],
        min_year=1979,
        max_year=2026,
    )

    source_permit_id = raw["permitnum"].map(clean_text)

    dataframe = pd.DataFrame(
        {
            "development_permit_key": source_permit_id.map(
                lambda value: f"calgary_{value}" if value else None
            ),
            "city": "calgary",
            "province": "AB",
            "source_name": "calgary_development_permits",
            "source_permit_id": source_permit_id,
            "permit_number": source_permit_id,
            "address_text": raw["address"].map(clean_text),
            "applicant_name": raw["applicant"].map(clean_text),
            "category": raw["category"].map(clean_text),
            "description": raw["description"].map(clean_text),
            "proposed_use_code": raw["proposedusecode"].map(clean_text),
            "proposed_use_description": raw["proposedusedescription"].map(clean_text),
            "permitted_discretionary": raw["permitteddiscretionary"].map(clean_text),
            "land_use_district": raw["landusedistrict"].map(clean_text),
            "land_use_district_description": raw["landusedistrictdescription"].map(clean_text),
            "concurrent_location": raw["concurrent_loc"].map(clean_text),
            "status_current": raw["statuscurrent"].map(clean_text),
            "applied_date": applied_date,
            "decision_date": decision_date,
            "release_date": release_date,
            "must_commence_date": must_commence_date,
            "canceled_refused_date": canceled_refused_date,
            "decision": raw["decision"].map(clean_text),
            "decision_by": raw["decisionby"].map(clean_text),
            "sdab_number": raw["sdabnumber"].map(clean_text),
            "sdab_hearing_date": sdab_hearing_date,
            "sdab_decision": raw["sdabdecision"].map(clean_text),
            "sdab_decision_date": sdab_decision_date,
            "community_code": raw["communitycode"].map(clean_text),
            "community_name": raw["communityname"].map(clean_text),
            "ward": raw["ward"].map(safe_int),
            "quadrant": raw["quadrant"].map(clean_text),
            "latitude": raw["latitude"].map(safe_float),
            "longitude": raw["longitude"].map(safe_float),
            "geometry_wkt": raw["point"].map(clean_text),
            "location_count": raw["locationcount"].map(safe_int),
            "location_types": raw["locationtypes"].map(clean_text),
            "location_addresses": raw["locationaddresses"].map(clean_text),
            "locations_geojson": raw["locationsgeojson"].map(clean_text),
            "locations_wkt": raw["locationswkt"].map(clean_text),
            "source_record_count": 1,
        }
    )

    dataframe["applied_year"] = dataframe["applied_date"].dt.year
    dataframe["decision_year"] = dataframe["decision_date"].dt.year

    dataframe = clean_development_permit_coordinates(dataframe)

    if dataframe.empty:
        raise RuntimeError("Calgary development permit Silver produced zero rows.")

    dataframe = dataframe.sort_values(
        ["applied_date", "development_permit_key"],
        na_position="last",
    ).reset_index(drop=True)

    return dataframe


def clean_development_permit_coordinates(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Null coordinates outside Calgary's expected city envelope."""
    working = dataframe.copy()

    has_coordinates = working["latitude"].notna() & working["longitude"].notna()

    valid_coordinates = (
        (working["latitude"] >= CALGARY_COORDINATE_BOUNDS["latitude_min"])
        & (working["latitude"] <= CALGARY_COORDINATE_BOUNDS["latitude_max"])
        & (working["longitude"] >= CALGARY_COORDINATE_BOUNDS["longitude_min"])
        & (working["longitude"] <= CALGARY_COORDINATE_BOUNDS["longitude_max"])
    )

    invalid_mask = has_coordinates & ~valid_coordinates

    working.loc[invalid_mask, "latitude"] = None
    working.loc[invalid_mask, "longitude"] = None
    working.loc[invalid_mask, "geometry_wkt"] = None

    return working


def clean_date_series(
    series: pd.Series,
    *,
    min_year: int,
    max_year: int,
) -> pd.Series:
    dates = pd.to_datetime(series, errors="coerce").dt.normalize()

    valid_dates = dates.dt.year.between(min_year, max_year, inclusive="both")

    return dates.where(valid_dates)


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
        return float(text.replace(",", ""))
    except ValueError:
        return None


def safe_int(value: Any) -> int | None:
    number = safe_float(value)

    if number is None:
        return None

    return int(number)
