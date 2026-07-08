from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


MART_START_YEAR = 2016
MART_END_YEAR = 2025
AB_BC_PROVINCES = {"AB", "BC"}

DEFAULT_MAPPING = {
    "version": "v1",
    "description": "Empty mapping config.",
    "locations": {},
}

SOURCE_KEY_CANDIDATES = [
    "disaster_event_month_key",
    "silver_disaster_event_month_key",
    "disaster_event_key",
    "event_key",
    "event_id",
    "id",
]

SOURCE_NAME_CANDIDATES = [
    "source_name",
    "source",
]

REFERENCE_MONTH_CANDIDATES = [
    "reference_month",
    "event_month",
    "month",
]

EVENT_YEAR_CANDIDATES = [
    "event_year",
    "reference_year",
    "year",
]

EVENT_MONTH_NUMBER_CANDIDATES = [
    "event_month_number",
    "reference_month_number",
    "month_number",
]

PROVINCE_CANDIDATES = [
    "province_key",
    "province_code",
    "province",
    "province_name",
]

LOCATION_CANDIDATES = [
    "event_location",
    "location_text",
    "location",
    "event_place",
    "place_name",
    "geo_name",
    "municipality_name",
    "community_name",
]

EVENT_TYPE_CANDIDATES = [
    "normalized_event_type",
    "event_type",
    "disaster_type",
    "hazard_type",
    "event_category",
]

EVENT_SUBTYPE_CANDIDATES = [
    "normalized_event_subtype",
    "event_subtype",
    "disaster_subtype",
    "hazard_subtype",
    "disaster_subgroup",
    "disaster_group",
]

VALUE_FIELD_CANDIDATES = {
    "estimated_total_cost_cad": [
        "estimated_total_cost_cad",
        "total_cost_cad",
        "estimated_cost_cad",
        "damage_cost_cad",
    ],
    "fatalities_total": [
        "fatalities_total",
        "fatalities",
        "death_total",
        "deaths",
    ],
    "injured_total": [
        "injured_total",
        "injured",
        "injuries_total",
    ],
    "evacuated_total": [
        "evacuated_total",
        "evacuated",
        "evacuees_total",
    ],
    "affected_total": [
        "affected_total",
        "affected",
        "people_affected",
    ],
    "normalized_total_cost_cad": [
        "normalized_total_cost_cad",
    ],
}


class GoldDisasterReferenceError(Exception):
    """Raised when Gold disaster event reference build fails."""


def load_location_mapping(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return DEFAULT_MAPPING

    mapping_path = Path(path)

    if not mapping_path.exists():
        return DEFAULT_MAPPING

    with mapping_path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)

    if "locations" not in data or not isinstance(data["locations"], dict):
        raise GoldDisasterReferenceError(
            f"Invalid disaster location mapping config: {mapping_path}"
        )

    return data


def build_gold_disaster_event_reference(
    *,
    disaster_event_month: pd.DataFrame,
    location_mapping: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if disaster_event_month.empty:
        raise GoldDisasterReferenceError("silver_disaster_event_month is empty.")

    mapping = location_mapping or DEFAULT_MAPPING
    source = disaster_event_month.copy().reset_index(drop=True)

    columns = list(source.columns)

    source_key_col = _first_present(columns, SOURCE_KEY_CANDIDATES)
    source_name_col = _first_present(columns, SOURCE_NAME_CANDIDATES)
    reference_month_col = _first_present(columns, REFERENCE_MONTH_CANDIDATES)
    event_year_col = _first_present(columns, EVENT_YEAR_CANDIDATES)
    event_month_number_col = _first_present(columns, EVENT_MONTH_NUMBER_CANDIDATES)
    province_col = _first_present(columns, PROVINCE_CANDIDATES)
    location_col = _first_present(columns, LOCATION_CANDIDATES)
    event_type_col = _first_present(columns, EVENT_TYPE_CANDIDATES)
    event_subtype_col = _first_present(columns, EVENT_SUBTYPE_CANDIDATES)

    if reference_month_col is None and event_year_col is None:
        raise GoldDisasterReferenceError(
            "Missing both reference_month and event_year-like columns."
        )

    if province_col is None:
        raise GoldDisasterReferenceError("Missing province-like column.")

    if location_col is None:
        raise GoldDisasterReferenceError("Missing event location-like column.")

    reference_month = _build_reference_month(source, reference_month_col)
    event_year = _build_event_year(
        source,
        event_year_col=event_year_col,
        reference_month=reference_month,
    )
    event_month_number = _build_event_month_number(
        source,
        event_month_number_col=event_month_number_col,
        reference_month=reference_month,
    )

    source_key = _build_source_key(source, source_key_col)
    province_key = source[province_col].map(_normalize_province_key)

    location_text = source[location_col].astype("string")
    location_text_normalized = location_text.map(_normalize_location_text)

    event_type = _optional_string_series(source, event_type_col)
    event_subtype = _optional_string_series(source, event_subtype_col)

    disaster_domain = [
        _classify_disaster_domain(event_type_value, event_subtype_value)
        for event_type_value, event_subtype_value in zip(event_type, event_subtype)
    ]

    mapping_rows = [
        _lookup_location_mapping(
            mapping=mapping,
            location_text=raw_location,
            location_text_normalized=normalized_location,
            province_key=province,
        )
        for raw_location, normalized_location, province in zip(
            location_text,
            location_text_normalized,
            province_key,
        )
    ]

    result = pd.DataFrame(
        {
            "disaster_event_reference_key": [f"disaster_event_ref__{key}" for key in source_key],
            "source_disaster_event_key": source_key,
            "source_row_number": source.index.astype("int64"),
            "source_name": _optional_string_series(source, source_name_col),
            "source_geometry": _optional_string_series(
                source,
                _first_present(columns, ["source_geometry", "geometry", "wkt"]),
            ),
            "description": _optional_string_series(
                source,
                _first_present(columns, ["description", "event_description", "summary"]),
            ),
            "province_key": province_key,
            "source_province_value": source[province_col].astype("string"),
            "reference_month": reference_month,
            "event_year": event_year.astype("Int64"),
            "event_month_number": event_month_number.astype("Int64"),
            "normalized_event_type": event_type,
            "normalized_event_subtype": event_subtype,
            "disaster_domain": disaster_domain,
            "is_wildfire_domain_relevant": [domain == "wildfire" for domain in disaster_domain],
            "is_flood_domain_relevant": [domain == "flood" for domain in disaster_domain],
            "is_climate_domain_relevant": [
                domain in {"severe_storm_or_climate", "climate_extreme"}
                for domain in disaster_domain
            ],
            "is_domain_relevant": [
                domain
                in {
                    "wildfire",
                    "flood",
                    "severe_storm_or_climate",
                    "climate_extreme",
                }
                for domain in disaster_domain
            ],
            "location_text": location_text,
            "location_text_normalized": location_text_normalized,
            "location_tier": [row["location_tier"] for row in mapping_rows],
            "mapped_geo_level": [row["mapped_geo_level"] for row in mapping_rows],
            "mapped_geo_codes_json": [
                json.dumps(row["mapped_geo_codes"], ensure_ascii=False) for row in mapping_rows
            ],
            "mapping_method": [row["mapping_method"] for row in mapping_rows],
            "mapping_confidence": [row["mapping_confidence"] for row in mapping_rows],
            "is_grid_backtest_eligible": [
                bool(row["is_grid_backtest_eligible"]) for row in mapping_rows
            ],
            "is_province_month_backtest_eligible": [
                bool(row["is_province_month_backtest_eligible"]) for row in mapping_rows
            ],
        }
    )

    for output_column, candidates in VALUE_FIELD_CANDIDATES.items():
        source_col = _first_present(columns, candidates)

        if source_col is None:
            result[output_column] = pd.NA
        else:
            result[output_column] = pd.to_numeric(source[source_col], errors="coerce")

    result["is_backtest_window"] = result["event_year"].between(
        MART_START_YEAR,
        MART_END_YEAR,
        inclusive="both",
    )
    result["is_ab_bc_scope"] = result["province_key"].isin(AB_BC_PROVINCES)
    result["is_backtest_eligible"] = (
        result["is_backtest_window"]
        & result["is_ab_bc_scope"]
        & result["is_domain_relevant"]
        & result["is_province_month_backtest_eligible"]
    )

    result = _order_columns(result)

    _require_unique_reference_key(result)

    summary = _build_summary(
        result,
        source_row_count=len(source),
        mapping_version=str(mapping.get("version", "unknown")),
        detected_columns={
            "source_key": source_key_col,
            "source_name": source_name_col,
            "reference_month": reference_month_col,
            "event_year": event_year_col,
            "event_month_number": event_month_number_col,
            "province": province_col,
            "location": location_col,
            "event_type": event_type_col,
            "event_subtype": event_subtype_col,
        },
    )

    return result, summary


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    column_set = set(columns)

    for candidate in candidates:
        if candidate in column_set:
            return candidate

    lower_map = {column.lower(): column for column in columns}

    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]

    return None


def _optional_string_series(dataframe: pd.DataFrame, column: str | None) -> pd.Series:
    if column is None:
        return pd.Series(pd.NA, index=dataframe.index, dtype="string")

    return dataframe[column].astype("string")


def _build_source_key(dataframe: pd.DataFrame, source_key_col: str | None) -> pd.Series:
    if source_key_col is not None:
        keys = dataframe[source_key_col].astype("string")
    else:
        keys = pd.Series(
            [f"source_row_{index:06d}" for index in dataframe.index],
            index=dataframe.index,
            dtype="string",
        )

    if keys.isna().any():
        keys = keys.fillna(
            pd.Series(
                [f"source_row_{index:06d}" for index in dataframe.index],
                index=dataframe.index,
                dtype="string",
            )
        )

    if keys.duplicated().any():
        keys = keys + "__row_" + dataframe.index.astype(str)

    return keys


def _build_reference_month(
    dataframe: pd.DataFrame,
    reference_month_col: str | None,
) -> pd.Series:
    if reference_month_col is None:
        return pd.Series(pd.NA, index=dataframe.index, dtype="string")

    parsed = pd.to_datetime(dataframe[reference_month_col], errors="coerce")
    return parsed.dt.to_period("M").astype("string")


def _build_event_year(
    dataframe: pd.DataFrame,
    *,
    event_year_col: str | None,
    reference_month: pd.Series,
) -> pd.Series:
    if event_year_col is not None:
        parsed_year = pd.to_numeric(dataframe[event_year_col], errors="coerce")
        return parsed_year

    parsed_month = pd.PeriodIndex(reference_month.dropna(), freq="M")
    result = pd.Series(pd.NA, index=dataframe.index, dtype="Float64")
    result.loc[reference_month.notna()] = parsed_month.year
    return result


def _build_event_month_number(
    dataframe: pd.DataFrame,
    *,
    event_month_number_col: str | None,
    reference_month: pd.Series,
) -> pd.Series:
    if event_month_number_col is not None:
        parsed_month_number = pd.to_numeric(
            dataframe[event_month_number_col],
            errors="coerce",
        )
        return parsed_month_number

    parsed_month = pd.PeriodIndex(reference_month.dropna(), freq="M")
    result = pd.Series(pd.NA, index=dataframe.index, dtype="Float64")
    result.loc[reference_month.notna()] = parsed_month.month
    return result


def _normalize_province_key(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).strip().lower()

    mapping = {
        "ab": "AB",
        "48": "AB",
        "alberta": "AB",
        "bc": "BC",
        "59": "BC",
        "british columbia": "BC",
        "b.c.": "BC",
    }

    return mapping.get(text, str(value).strip().upper())


def _normalize_location_text(value: Any) -> str | None:
    if pd.isna(value):
        return None

    text = str(value).lower().strip()
    text = re.sub(r"[,.;:()\-_/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def _classify_disaster_domain(event_type: Any, event_subtype: Any) -> str:
    text = f"{_safe_lower(event_type)} {_safe_lower(event_subtype)}"

    if any(token in text for token in ["wildfire", "wild fire", "forest fire"]):
        return "wildfire"

    if "flood" in text:
        return "flood"

    if any(
        token in text
        for token in [
            "storm",
            "hail",
            "wind",
            "tornado",
            "rain",
            "precipitation",
            "blizzard",
            "winter",
            "ice storm",
        ]
    ):
        return "severe_storm_or_climate"

    if any(token in text for token in ["heat", "drought", "cold", "freeze"]):
        return "climate_extreme"

    return "other_or_unmapped"


def _safe_lower(value: Any) -> str:
    if pd.isna(value):
        return ""

    return str(value).lower().strip()


def _lookup_location_mapping(
    *,
    mapping: dict[str, Any],
    location_text: Any,
    location_text_normalized: str | None,
    province_key: str | None,
) -> dict[str, Any]:
    locations = mapping.get("locations", {})

    raw_key = None if pd.isna(location_text) else str(location_text)
    normalized_key = location_text_normalized

    normalized_locations = {
        _normalize_location_text(key): value
        for key, value in locations.items()
        if _normalize_location_text(key) is not None
    }

    config = None

    if raw_key is not None and raw_key in locations:
        config = locations[raw_key]
    elif normalized_key is not None and normalized_key in locations:
        config = locations[normalized_key]
    elif normalized_key is not None and normalized_key in normalized_locations:
        config = normalized_locations[normalized_key]

    if config is None:
        return _default_unmapped_location(location_text_normalized, province_key)

    mapped_geo_codes = config.get("mapped_geo_codes", [])

    if mapped_geo_codes is None:
        mapped_geo_codes = []

    if not isinstance(mapped_geo_codes, list):
        raise GoldDisasterReferenceError(f"mapped_geo_codes must be a list for location: {raw_key}")

    return {
        "location_tier": config.get("location_tier", "unknown"),
        "mapped_geo_level": config.get("mapped_geo_level", "unmapped"),
        "mapped_geo_codes": [str(value) for value in mapped_geo_codes],
        "mapping_method": config.get("mapping_method", "manual"),
        "mapping_confidence": config.get("mapping_confidence", "unknown"),
        "is_grid_backtest_eligible": bool(config.get("is_grid_backtest_eligible", False)),
        "is_province_month_backtest_eligible": bool(
            config.get("is_province_month_backtest_eligible", True)
        ),
    }


def _default_unmapped_location(
    location_text_normalized: str | None,
    province_key: str | None,
) -> dict[str, Any]:
    if _looks_province_wide(location_text_normalized):
        location_tier = "province_or_region"
        mapped_geo_level = "unmapped_region"
        mapping_method = "unmapped_region_text"
        mapping_confidence = "low"
        province_month_eligible = province_key in AB_BC_PROVINCES
    else:
        location_tier = "unmapped"
        mapped_geo_level = "unmapped"
        mapping_method = "unmapped"
        mapping_confidence = "unmapped"
        province_month_eligible = province_key in AB_BC_PROVINCES

    return {
        "location_tier": location_tier,
        "mapped_geo_level": mapped_geo_level,
        "mapped_geo_codes": [],
        "mapping_method": mapping_method,
        "mapping_confidence": mapping_confidence,
        "is_grid_backtest_eligible": False,
        "is_province_month_backtest_eligible": province_month_eligible,
    }


def _looks_province_wide(location_text_normalized: str | None) -> bool:
    if not location_text_normalized:
        return False

    patterns = [
        "province",
        "provincial",
        "throughout",
        "across",
        "wide",
        "all of",
        "entire",
        "various",
        "multiple",
        "several",
        "region",
        "southern alberta",
        "central alberta",
        "northern alberta",
        "interior bc",
        "interior british columbia",
        "vancouver island",
        "northern bc",
        "northern british columbia",
    ]

    return any(pattern in location_text_normalized for pattern in patterns)


def _order_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    ordered_columns = [
        "disaster_event_reference_key",
        "source_disaster_event_key",
        "source_row_number",
        "source_name",
        "source_geometry",
        "description",
        "province_key",
        "source_province_value",
        "reference_month",
        "event_year",
        "event_month_number",
        "normalized_event_type",
        "normalized_event_subtype",
        "disaster_domain",
        "is_wildfire_domain_relevant",
        "is_flood_domain_relevant",
        "is_climate_domain_relevant",
        "is_domain_relevant",
        "location_text",
        "location_text_normalized",
        "location_tier",
        "mapped_geo_level",
        "mapped_geo_codes_json",
        "mapping_method",
        "mapping_confidence",
        "is_grid_backtest_eligible",
        "is_province_month_backtest_eligible",
        "estimated_total_cost_cad",
        "normalized_total_cost_cad",
        "fatalities_total",
        "injured_total",
        "evacuated_total",
        "affected_total",
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_backtest_eligible",
    ]

    return dataframe[ordered_columns].copy()


def _require_unique_reference_key(dataframe: pd.DataFrame) -> None:
    if dataframe["disaster_event_reference_key"].isna().any():
        raise GoldDisasterReferenceError("disaster_event_reference_key contains nulls.")

    if dataframe["disaster_event_reference_key"].duplicated().any():
        raise GoldDisasterReferenceError("disaster_event_reference_key contains duplicates.")


def _build_summary(
    dataframe: pd.DataFrame,
    *,
    source_row_count: int,
    mapping_version: str,
    detected_columns: dict[str, str | None],
) -> dict[str, Any]:
    return {
        "table_name": "gold_disaster_event_reference",
        "source_row_count": int(source_row_count),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "mapping_version": mapping_version,
        "detected_columns": detected_columns,
        "minimum_month": (
            str(dataframe["reference_month"].dropna().min())
            if dataframe["reference_month"].notna().any()
            else None
        ),
        "maximum_month": (
            str(dataframe["reference_month"].dropna().max())
            if dataframe["reference_month"].notna().any()
            else None
        ),
        "minimum_event_year": (
            int(dataframe["event_year"].dropna().min())
            if dataframe["event_year"].notna().any()
            else None
        ),
        "maximum_event_year": (
            int(dataframe["event_year"].dropna().max())
            if dataframe["event_year"].notna().any()
            else None
        ),
        "province_counts": _value_counts(dataframe["province_key"]),
        "disaster_domain_counts": _value_counts(dataframe["disaster_domain"]),
        "location_tier_counts": _value_counts(dataframe["location_tier"]),
        "mapping_confidence_counts": _value_counts(dataframe["mapping_confidence"]),
        "backtest_window_event_count": int(dataframe["is_backtest_window"].sum()),
        "ab_bc_event_count": int(dataframe["is_ab_bc_scope"].sum()),
        "domain_relevant_event_count": int(dataframe["is_domain_relevant"].sum()),
        "backtest_eligible_event_count": int(dataframe["is_backtest_eligible"].sum()),
        "grid_backtest_eligible_event_count": int(dataframe["is_grid_backtest_eligible"].sum()),
        "all_grid_backtest_eligible_event_count": int(dataframe["is_grid_backtest_eligible"].sum()),
        "backtest_window_grid_eligible_event_count": int(
            (
                dataframe["is_backtest_window"]
                & dataframe["is_ab_bc_scope"]
                & dataframe["is_domain_relevant"]
                & dataframe["is_grid_backtest_eligible"]
            ).sum()
        ),
        "backtest_window_province_month_eligible_event_count": int(
            (
                dataframe["is_backtest_window"]
                & dataframe["is_ab_bc_scope"]
                & dataframe["is_domain_relevant"]
                & dataframe["is_province_month_backtest_eligible"]
            ).sum()
        ),
        "province_month_backtest_eligible_event_count": int(
            dataframe["is_province_month_backtest_eligible"].sum()
        ),
        "unique_location_count": int(dataframe["location_text_normalized"].nunique(dropna=True)),
        "top_locations": _top_value_counts(dataframe["location_text"], limit=30),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _top_value_counts(series: pd.Series, limit: int) -> list[dict[str, Any]]:
    counts = series.value_counts(dropna=False).head(limit)

    return [
        {
            "value": None if pd.isna(key) else str(key),
            "count": int(value),
        }
        for key, value in counts.to_dict().items()
    ]
