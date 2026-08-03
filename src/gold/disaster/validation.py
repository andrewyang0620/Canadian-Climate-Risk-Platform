from __future__ import annotations

import json
from typing import Any

import pandas as pd


class GoldDisasterReferenceValidationError(Exception):
    """Raised when Gold disaster event reference validation fails."""


REQUIRED_COLUMNS = [
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

ALLOWED_PROVINCES = {"AB", "BC"}

ALLOWED_DOMAINS = {
    "wildfire",
    "flood",
    "severe_storm_or_climate",
    "climate_extreme",
    "other_or_unmapped",
}

ALLOWED_MAPPING_CONFIDENCE = {
    "high",
    "medium",
    "low",
    "low_for_grid",
    "unmapped",
}

GRID_ELIGIBLE_GEO_LEVELS = {
    "CSD",
    "CD",
    "CD_GROUP",
}

NON_GRID_LOCATION_TIERS = {
    "province",
    "cross_province_region",
    "large_region",
    "unmapped_locality",
    "unmapped",
    "province_or_region",
}

VALUE_COLUMNS = [
    "estimated_total_cost_cad",
    "normalized_total_cost_cad",
    "fatalities_total",
    "injured_total",
    "evacuated_total",
    "affected_total",
]

BOOLEAN_COLUMNS = [
    "is_wildfire_domain_relevant",
    "is_flood_domain_relevant",
    "is_climate_domain_relevant",
    "is_domain_relevant",
    "is_grid_backtest_eligible",
    "is_province_month_backtest_eligible",
    "is_backtest_window",
    "is_ab_bc_scope",
    "is_backtest_eligible",
]


def validate_gold_disaster_event_reference(dataframe: pd.DataFrame) -> dict[str, Any]:
    failures: list[str] = []
    checks: list[str] = []

    _check_required_columns(dataframe, failures, checks)

    if failures:
        _raise(failures)

    _check_row_count(dataframe, failures, checks)
    _check_primary_key(dataframe, failures, checks)
    _check_province_values(dataframe, failures, checks)
    _check_reference_month_consistency(dataframe, failures, checks)
    _check_domain_flags(dataframe, failures, checks)
    _check_boolean_columns(dataframe, failures, checks)
    _check_mapping_semantics(dataframe, failures, checks)
    _check_backtest_flags(dataframe, failures, checks)
    _check_value_columns(dataframe, failures, checks)
    _check_backtest_coverage(dataframe, failures, checks)

    if failures:
        _raise(failures)

    return _build_report(dataframe, checks)


def _check_required_columns(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]

    if missing:
        failures.append(f"Missing required columns: {missing}")
        return

    checks.append("required_columns_present")


def _check_row_count(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    if dataframe.empty:
        failures.append("Gold disaster event reference is empty.")
        return

    checks.append("row_count_nonzero")


def _check_primary_key(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    key = dataframe["disaster_event_reference_key"]

    if key.isna().any():
        failures.append("disaster_event_reference_key contains nulls.")

    if key.duplicated().any():
        failures.append("disaster_event_reference_key contains duplicates.")

    if not key.astype(str).str.startswith("disaster_event_ref__").all():
        failures.append("disaster_event_reference_key has unexpected prefix.")

    checks.append("primary_key_valid")


def _check_province_values(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    observed = set(dataframe["province_key"].dropna().astype(str).unique())
    invalid = sorted(observed - ALLOWED_PROVINCES)

    if invalid:
        failures.append(f"Invalid province_key values: {invalid}")

    checks.append("province_values_valid")


def _check_reference_month_consistency(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    parsed_month = pd.to_datetime(
        dataframe["reference_month"].astype("string") + "-01",
        errors="coerce",
    )

    if parsed_month.isna().any():
        failures.append("reference_month contains unparsable values.")

    valid = parsed_month.notna()

    if valid.any():
        year_matches = (
            dataframe.loc[valid, "event_year"]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.year.astype("Int64"))
        )
        month_matches = (
            dataframe.loc[valid, "event_month_number"]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.month.astype("Int64"))
        )

        if not year_matches.all():
            failures.append("event_year does not match reference_month year.")

        if not month_matches.all():
            failures.append("event_month_number does not match reference_month month.")

    checks.append("reference_month_consistent")


def _check_domain_flags(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    observed = set(dataframe["disaster_domain"].dropna().astype(str).unique())
    invalid = sorted(observed - ALLOWED_DOMAINS)

    if invalid:
        failures.append(f"Invalid disaster_domain values: {invalid}")

    wildfire_expected = dataframe["disaster_domain"].eq("wildfire")
    flood_expected = dataframe["disaster_domain"].eq("flood")
    climate_expected = dataframe["disaster_domain"].isin(
        {"severe_storm_or_climate", "climate_extreme"}
    )
    domain_expected = dataframe["disaster_domain"].isin(
        {"wildfire", "flood", "severe_storm_or_climate", "climate_extreme"}
    )

    if not dataframe["is_wildfire_domain_relevant"].eq(wildfire_expected).all():
        failures.append("is_wildfire_domain_relevant is inconsistent.")

    if not dataframe["is_flood_domain_relevant"].eq(flood_expected).all():
        failures.append("is_flood_domain_relevant is inconsistent.")

    if not dataframe["is_climate_domain_relevant"].eq(climate_expected).all():
        failures.append("is_climate_domain_relevant is inconsistent.")

    if not dataframe["is_domain_relevant"].eq(domain_expected).all():
        failures.append("is_domain_relevant is inconsistent.")

    checks.append("domain_flags_consistent")


def _check_boolean_columns(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    for column in BOOLEAN_COLUMNS:
        if dataframe[column].isna().any():
            failures.append(f"{column} contains nulls.")

    checks.append("boolean_columns_non_null")


def _check_mapping_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    observed_confidence = set(dataframe["mapping_confidence"].dropna().astype(str).unique())
    invalid_confidence = sorted(observed_confidence - ALLOWED_MAPPING_CONFIDENCE)

    if invalid_confidence:
        failures.append(f"Invalid mapping_confidence values: {invalid_confidence}")

    parsed_codes = dataframe["mapped_geo_codes_json"].map(_parse_codes)

    invalid_json_count = int(parsed_codes.isna().sum())
    if invalid_json_count:
        failures.append(f"mapped_geo_codes_json has {invalid_json_count} invalid rows.")

    grid_eligible = dataframe["is_grid_backtest_eligible"].astype(bool)

    missing_grid_codes = grid_eligible & parsed_codes.map(lambda value: len(value) == 0)
    if missing_grid_codes.any():
        failures.append("Grid-eligible rows must have non-empty mapped_geo_codes_json.")

    invalid_grid_level = grid_eligible & ~dataframe["mapped_geo_level"].isin(
        GRID_ELIGIBLE_GEO_LEVELS
    )
    if invalid_grid_level.any():
        failures.append("Grid-eligible rows must use CSD, CD, or CD_GROUP geo levels.")

    invalid_grid_tier = grid_eligible & dataframe["location_tier"].isin(NON_GRID_LOCATION_TIERS)
    if invalid_grid_tier.any():
        failures.append("Non-grid location tiers cannot be grid-backtest eligible.")

    checks.append("mapping_semantics_valid")


def _check_backtest_flags(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    expected_window = dataframe["event_year"].between(2016, 2025, inclusive="both")
    expected_ab_bc = dataframe["province_key"].isin(ALLOWED_PROVINCES)
    expected_backtest = (
        expected_window
        & expected_ab_bc
        & dataframe["is_domain_relevant"].astype(bool)
        & dataframe["is_province_month_backtest_eligible"].astype(bool)
    )

    if not dataframe["is_backtest_window"].eq(expected_window).all():
        failures.append("is_backtest_window is inconsistent.")

    if not dataframe["is_ab_bc_scope"].eq(expected_ab_bc).all():
        failures.append("is_ab_bc_scope is inconsistent.")

    if not dataframe["is_backtest_eligible"].eq(expected_backtest).all():
        failures.append("is_backtest_eligible is inconsistent.")

    checks.append("backtest_flags_consistent")


def _check_value_columns(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    for column in VALUE_COLUMNS:
        numeric = pd.to_numeric(dataframe[column], errors="coerce")
        non_null = numeric.dropna()

        if (non_null < 0).any():
            failures.append(f"{column} contains negative values.")

    checks.append("value_columns_non_negative")


def _check_backtest_coverage(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    backtest_window_count = int(dataframe["is_backtest_window"].sum())
    backtest_eligible_count = int(dataframe["is_backtest_eligible"].sum())
    grid_eligible_count = int(
        (
            dataframe["is_backtest_window"]
            & dataframe["is_ab_bc_scope"]
            & dataframe["is_domain_relevant"]
            & dataframe["is_grid_backtest_eligible"]
        ).sum()
    )

    if backtest_window_count <= 0:
        failures.append("No events found in 2016-2025 backtest window.")

    if backtest_eligible_count <= 0:
        failures.append("No province-month eligible backtest events found.")

    if grid_eligible_count <= 0:
        failures.append("No grid-level eligible backtest events found.")

    checks.append("backtest_coverage_nonzero")


def _parse_codes(value: Any) -> list[str] | None:
    try:
        parsed = json.loads(value)
    except Exception:
        return None

    if not isinstance(parsed, list):
        return None

    return [str(item) for item in parsed]


def _build_report(dataframe: pd.DataFrame, checks: list[str]) -> dict[str, Any]:
    backtest_target = (
        dataframe["is_backtest_window"]
        & dataframe["is_ab_bc_scope"]
        & dataframe["is_domain_relevant"]
    )

    return {
        "table_name": "gold_disaster_event_reference",
        "validation_status": "passed",
        "checks_passed": checks,
        "check_count": len(checks),
        "row_count": int(len(dataframe)),
        "unique_reference_key_count": int(dataframe["disaster_event_reference_key"].nunique()),
        "minimum_month": str(dataframe["reference_month"].min()),
        "maximum_month": str(dataframe["reference_month"].max()),
        "province_counts": _value_counts(dataframe["province_key"]),
        "disaster_domain_counts": _value_counts(dataframe["disaster_domain"]),
        "location_tier_counts": _value_counts(dataframe["location_tier"]),
        "mapping_confidence_counts": _value_counts(dataframe["mapping_confidence"]),
        "backtest_window_event_count": int(dataframe["is_backtest_window"].sum()),
        "backtest_eligible_event_count": int(dataframe["is_backtest_eligible"].sum()),
        "backtest_target_event_count": int(backtest_target.sum()),
        "backtest_window_grid_eligible_event_count": int(
            (backtest_target & dataframe["is_grid_backtest_eligible"]).sum()
        ),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _raise(failures: list[str]) -> None:
    message = "Gold disaster event reference validation failed:\n"
    message += "\n".join(f"- {failure}" for failure in failures)
    raise GoldDisasterReferenceValidationError(message)
