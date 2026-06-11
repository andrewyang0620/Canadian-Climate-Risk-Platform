from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


STATCAN_BUILDING_PERMITS_TABLE_ID = "34-10-0292-01"
STATCAN_BUILDING_PERMITS_PRODUCT_ID = "3410029201"

DEFAULT_TARGET_GEOS = {
    "Canada",
    "Alberta",
    "British Columbia",
    "Calgary, Alberta",
    "Vancouver, British Columbia",
}

SCALAR_MULTIPLIERS = {
    "units": 1.0,
    "thousands": 1000.0,
    "millions": 1_000_000.0,
}


def build_statcan_building_permit_month_silver(
    raw_path: str | Path,
    *,
    target_geos: set[str] | None = None,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    """Build project-scoped monthly StatCan building permit aggregate Silver table.

    Raw StatCan table is very large, so Silver keeps only project-relevant
    Canada / Alberta / British Columbia / Calgary / Vancouver geographies.
    """
    raw_path = Path(raw_path)
    target_geos = target_geos or DEFAULT_TARGET_GEOS

    frames: list[pd.DataFrame] = []
    source_row_start = 1

    for chunk in pd.read_csv(
        raw_path,
        dtype=object,
        chunksize=chunksize,
        low_memory=False,
    ):
        filtered = chunk[chunk["GEO"].isin(target_geos)].copy()

        if not filtered.empty:
            standardized = standardize_statcan_building_permit_month_chunk(
                filtered,
                source_row_start=source_row_start,
            )
            frames.append(standardized)

        source_row_start += len(chunk)

    if not frames:
        raise RuntimeError("StatCan building permit monthly Silver produced zero rows.")

    dataframe = pd.concat(frames, ignore_index=True)

    dataframe["source_record_count"] = dataframe.groupby("statcan_building_permit_month_key")[
        "statcan_building_permit_month_key"
    ].transform("size")

    dataframe = dataframe.sort_values(
        [
            "reference_month",
            "geo_name",
            "type_of_building",
            "type_of_work",
            "measure_name",
            "seasonal_adjustment_value_type",
            "source_record_number",
        ],
        na_position="last",
    ).reset_index(drop=True)

    return dataframe


def standardize_statcan_building_permit_month_chunk(
    chunk: pd.DataFrame,
    *,
    source_row_start: int,
) -> pd.DataFrame:
    working = chunk.copy()
    source_record_numbers = list(range(source_row_start, source_row_start + len(working)))

    ref_date_text = working["REF_DATE"].map(clean_text)
    reference_month = pd.to_datetime(
        ref_date_text.map(to_month_start_text),
        errors="coerce",
    )

    geo_name = working["GEO"].map(clean_text)
    scalar_factor = working["SCALAR_FACTOR"].map(clean_text)
    value = working["VALUE"].map(safe_float)

    dataframe = pd.DataFrame(
        {
            "statcan_building_permit_month_key": [
                build_statcan_key(ref_date=ref_date, vector=vector)
                for ref_date, vector in zip(
                    ref_date_text,
                    working["VECTOR"].map(clean_text),
                    strict=True,
                )
            ],
            "source_name": "statcan_building_permits",
            "statcan_table_id": STATCAN_BUILDING_PERMITS_TABLE_ID,
            "statcan_product_id": STATCAN_BUILDING_PERMITS_PRODUCT_ID,
            "reference_month": reference_month,
            "reference_year": reference_month.dt.year,
            "reference_month_number": reference_month.dt.month,
            "geo_name": geo_name,
            "dguid": working["DGUID"].map(clean_text),
            "geo_level": geo_name.map(classify_geo_level),
            "province_code": geo_name.map(infer_province_code),
            "type_of_building": working["Type of building"].map(clean_text),
            "type_of_work": working["Type of work"].map(clean_text),
            "measure_name": working["Variables"].map(clean_text),
            "seasonal_adjustment_value_type": working["Seasonal adjustment, value type"].map(
                clean_text
            ),
            "value": value,
            "value_scaled": [
                scale_value(raw_value, raw_scalar)
                for raw_value, raw_scalar in zip(value, scalar_factor, strict=True)
            ],
            "unit_of_measure": working["UOM"].map(clean_text),
            "unit_of_measure_id": working["UOM_ID"].map(safe_int),
            "scalar_factor": scalar_factor,
            "scalar_factor_id": working["SCALAR_ID"].map(safe_int),
            "vector": working["VECTOR"].map(clean_text),
            "coordinate": working["COORDINATE"].map(clean_text),
            "status": working["STATUS"].map(clean_text),
            "symbol": working["SYMBOL"].map(clean_text),
            "terminated": working["TERMINATED"].map(clean_text),
            "decimals": working["DECIMALS"].map(safe_int),
            "source_record_number": source_record_numbers,
        }
    )

    return dataframe


def latest_statcan_building_permits_raw_path(
    *,
    bronze_root: str | Path = "lakehouse/bronze",
) -> Path:
    candidates = sorted(
        Path(bronze_root).glob(
            "statcan_building_permits/extract_date=*/run_id=*/raw/statcan_building_permits_raw.csv"
        )
    )

    if not candidates:
        raise FileNotFoundError("No real StatCan building permits Bronze raw CSV found.")

    return candidates[-1]


def build_statcan_key(
    *,
    ref_date: str | None,
    vector: str | None,
) -> str:
    if not ref_date or not vector:
        raise ValueError("StatCan key requires non-null REF_DATE and VECTOR.")

    safe_ref_date = ref_date.replace("-", "")
    safe_vector = vector.replace(" ", "_")

    return f"statcan_{STATCAN_BUILDING_PERMITS_PRODUCT_ID}_{safe_ref_date}_{safe_vector}"


def classify_geo_level(geo_name: str | None) -> str | None:
    if geo_name is None:
        return None

    if geo_name == "Canada":
        return "country"

    if geo_name in {"Alberta", "British Columbia"}:
        return "province"

    if geo_name in {"Calgary, Alberta", "Vancouver, British Columbia"}:
        return "cma"

    return "other"


def infer_province_code(geo_name: str | None) -> str | None:
    if geo_name is None:
        return None

    if geo_name in {"Alberta", "Calgary, Alberta"}:
        return "AB"

    if geo_name in {"British Columbia", "Vancouver, British Columbia"}:
        return "BC"

    return None


def scale_value(value: float | None, scalar_factor: str | None) -> float | None:
    if value is None:
        return None

    multiplier = SCALAR_MULTIPLIERS.get(str(scalar_factor).lower(), 1.0)

    return value * multiplier


def to_month_start_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = value.strip()

    if len(text) == 7:
        return f"{text}-01"

    return text


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
