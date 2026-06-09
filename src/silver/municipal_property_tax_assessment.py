from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


VANCOUVER_PROPERTY_TAX_COLUMNS = [
    "pid",
    "legal_type",
    "folio",
    "land_coordinate",
    "zoning_district",
    "zoning_classification",
    "lot",
    "plan",
    "block",
    "district_lot",
    "from_civic_number",
    "to_civic_number",
    "street_name",
    "property_postal_code",
    "narrative_legal_line1",
    "narrative_legal_line2",
    "narrative_legal_line3",
    "narrative_legal_line4",
    "narrative_legal_line5",
    "current_land_value",
    "current_improvement_value",
    "tax_assessment_year",
    "previous_land_value",
    "previous_improvement_value",
    "year_built",
    "big_improvement_year",
    "tax_levy",
    "neighbourhood_code",
    "report_year",
    "note",
]


def build_vancouver_property_tax_assessment_silver(
    raw_path: str | Path,
    *,
    chunksize: int = 100_000,
) -> pd.DataFrame:
    """Build Silver property tax assessment records from Vancouver tax CSV.

    The raw source does not have a perfectly unique natural key. The Silver
    grain is therefore one standardized source record per raw CSV row.
    """
    raw_path = Path(raw_path)

    frames: list[pd.DataFrame] = []
    source_row_start = 1

    chunk_iter = pd.read_csv(
        raw_path,
        sep=";",
        encoding="utf-8-sig",
        dtype=object,
        chunksize=chunksize,
        low_memory=False,
    )

    for chunk in chunk_iter:
        standardized = standardize_vancouver_property_tax_chunk(
            chunk,
            source_row_start=source_row_start,
        )
        frames.append(standardized)
        source_row_start += len(chunk)

    if not frames:
        raise RuntimeError("Vancouver property tax assessment Silver produced zero rows.")

    dataframe = pd.concat(frames, ignore_index=True)

    if dataframe.empty:
        raise RuntimeError("Vancouver property tax assessment Silver produced zero rows.")

    dataframe = dataframe.sort_values("source_row_number").reset_index(drop=True)

    return dataframe


def standardize_vancouver_property_tax_chunk(
    chunk: pd.DataFrame,
    *,
    source_row_start: int,
) -> pd.DataFrame:
    working = chunk.copy()

    for column in VANCOUVER_PROPERTY_TAX_COLUMNS:
        if column not in working.columns:
            working[column] = None

    source_row_numbers = list(range(source_row_start, source_row_start + len(working)))

    source_pid = working["pid"].map(clean_text)
    source_folio = working["folio"].map(clean_text)
    source_land_coordinate = working["land_coordinate"].map(clean_text)
    report_year_text = working["report_year"].map(clean_text)

    legal_type = working["legal_type"].map(clean_text)
    zoning_district = working["zoning_district"].map(clean_text)
    zoning_classification = working["zoning_classification"].map(clean_text)

    lot = working["lot"].map(clean_text)
    plan = working["plan"].map(clean_text)
    block = working["block"].map(clean_text)
    district_lot = working["district_lot"].map(clean_text)

    civic_number_from = working["from_civic_number"].map(clean_text)
    civic_number_to = working["to_civic_number"].map(clean_text)
    street_name = working["street_name"].map(clean_text)

    current_land_value = working["current_land_value"].map(safe_non_negative_float)
    current_improvement_value = working["current_improvement_value"].map(safe_non_negative_float)
    previous_land_value = working["previous_land_value"].map(safe_non_negative_float)
    previous_improvement_value = working["previous_improvement_value"].map(safe_non_negative_float)

    dataframe = pd.DataFrame(
        {
            "property_tax_assessment_key": [
                build_property_tax_assessment_key(
                    source_pid=pid,
                    source_folio=folio,
                    source_land_coordinate=land_coordinate,
                    report_year=report_year,
                    source_row_number=source_row_number,
                )
                for pid, folio, land_coordinate, report_year, source_row_number in zip(
                    source_pid,
                    source_folio,
                    source_land_coordinate,
                    report_year_text,
                    source_row_numbers,
                    strict=True,
                )
            ],
            "city": "vancouver",
            "province": "BC",
            "source_name": "vancouver_property_tax",
            "source_pid": source_pid,
            "source_folio": source_folio,
            "source_land_coordinate": source_land_coordinate,
            "legal_type": legal_type,
            "zoning_district": zoning_district,
            "zoning_classification": zoning_classification,
            "lot": lot,
            "plan": plan,
            "block": block,
            "district_lot": district_lot,
            "civic_number_from": civic_number_from,
            "civic_number_to": civic_number_to,
            "street_name": street_name,
            "address_text": [
                build_address_text(civic_from, civic_to, street)
                for civic_from, civic_to, street in zip(
                    civic_number_from,
                    civic_number_to,
                    street_name,
                    strict=True,
                )
            ],
            "postal_code": working["property_postal_code"].map(clean_text),
            "legal_description_text": [
                combine_text_parts(line1, line2, line3, line4, line5)
                for line1, line2, line3, line4, line5 in zip(
                    working["narrative_legal_line1"].map(clean_text),
                    working["narrative_legal_line2"].map(clean_text),
                    working["narrative_legal_line3"].map(clean_text),
                    working["narrative_legal_line4"].map(clean_text),
                    working["narrative_legal_line5"].map(clean_text),
                    strict=True,
                )
            ],
            "current_land_value": current_land_value,
            "current_improvement_value": current_improvement_value,
            "current_total_assessed_value": sum_available_values(
                current_land_value,
                current_improvement_value,
            ),
            "previous_land_value": previous_land_value,
            "previous_improvement_value": previous_improvement_value,
            "previous_total_assessed_value": sum_available_values(
                previous_land_value,
                previous_improvement_value,
            ),
            "tax_levy": working["tax_levy"].map(safe_non_negative_float),
            "tax_assessment_year": working["tax_assessment_year"].map(safe_int),
            "report_year": working["report_year"].map(safe_int),
            "year_built": working["year_built"].map(safe_int),
            "big_improvement_year": working["big_improvement_year"].map(safe_int),
            "neighbourhood_code": working["neighbourhood_code"].map(clean_text),
            "source_note": working["note"].map(clean_text),
            "source_row_number": source_row_numbers,
            "source_record_count": 1,
        }
    )

    return dataframe


def build_property_tax_assessment_key(
    *,
    source_pid: str | None,
    source_folio: str | None,
    source_land_coordinate: str | None,
    report_year: str | None,
    source_row_number: int,
) -> str:
    identity = {
        "source_pid": source_pid,
        "source_folio": source_folio,
        "source_land_coordinate": source_land_coordinate,
        "report_year": report_year,
        "source_row_number": source_row_number,
    }

    digest = hashlib.md5(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]

    return f"vancouver_{digest}"


def build_address_text(
    civic_number_from: str | None,
    civic_number_to: str | None,
    street_name: str | None,
) -> str | None:
    civic_number = build_civic_number_text(civic_number_from, civic_number_to)
    parts = [part for part in [civic_number, street_name] if part]

    if not parts:
        return None

    return " ".join(parts)


def build_civic_number_text(
    civic_number_from: str | None,
    civic_number_to: str | None,
) -> str | None:
    if civic_number_from and civic_number_to:
        if civic_number_from == civic_number_to:
            return civic_number_from

        from_number = safe_int(civic_number_from)
        to_number = safe_int(civic_number_to)

        if from_number is not None and to_number is not None:
            if from_number < to_number:
                return f"{civic_number_from}-{civic_number_to}"

            return f"{civic_number_from} / {civic_number_to}"

        return f"{civic_number_from} / {civic_number_to}"

    if civic_number_from:
        return civic_number_from

    if civic_number_to:
        return civic_number_to

    return None


def combine_text_parts(*parts: str | None) -> str | None:
    cleaned_parts = [part for part in parts if part]

    if not cleaned_parts:
        return None

    return " ".join(cleaned_parts)


def sum_available_values(
    first: pd.Series,
    second: pd.Series,
) -> pd.Series:
    has_any_value = first.notna() | second.notna()

    return (first.fillna(0) + second.fillna(0)).where(has_any_value)


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


def safe_non_negative_float(value: Any) -> float | None:
    number = safe_float(value)

    if number is None:
        return None

    if number < 0:
        return None

    return number


def safe_int(value: Any) -> int | None:
    number = safe_float(value)

    if number is None:
        return None

    return int(number)
