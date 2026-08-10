from __future__ import annotations

from typing import Any

import pandas as pd


class GoldDisasterEventCDScopeValidationError(Exception):
    """Raised when Gold disaster event CD scope validation fails."""


REQUIRED_COLUMNS = [
    "event_cd_scope_key",
    "disaster_event_reference_key",
    "source_disaster_event_key",
    "reference_month",
    "event_year",
    "event_month_number",
    "province_key",
    "disaster_domain",
    "location_text",
    "location_tier",
    "source_mapped_geo_level",
    "source_mapped_geo_codes_json",
    "resolved_census_division_key",
    "census_division_name",
    "census_division_type",
    "census_division_province_key",
    "resolution_method",
    "is_csd_to_cd_approximation",
    "mapping_confidence",
    "mapping_method",
    "is_backtest_window",
    "is_ab_bc_scope",
    "is_domain_relevant",
    "is_grid_backtest_eligible",
]

ALLOWED_PROVINCES = {"AB", "BC"}
ALLOWED_SOURCE_LEVELS = {"CD", "CD_GROUP", "CSD"}
ALLOWED_RESOLUTION_METHODS = {"direct_cd", "csd_parent_cd"}
ALLOWED_DOMAINS = {
    "wildfire",
    "flood",
    "severe_storm_or_climate",
    "climate_extreme",
}


def validate_gold_disaster_event_cd_scope_reference(
    *,
    event_cd_scope: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
) -> dict[str, Any]:
    failures: list[str] = []
    checks: list[str] = []

    _check_required_columns(event_cd_scope, failures, checks)

    if failures:
        _raise(failures)

    _check_row_count(event_cd_scope, failures, checks)
    _check_primary_key(event_cd_scope, failures, checks)
    _check_event_cd_grain(event_cd_scope, failures, checks)
    _check_boolean_flags(event_cd_scope, failures, checks)
    _check_allowed_values(event_cd_scope, failures, checks)
    _check_reference_months(event_cd_scope, failures, checks)
    _check_cd_spatial_coverage(
        event_cd_scope,
        cd_spatial_reference,
        failures,
        checks,
    )
    _check_event_reference_coverage(
        event_cd_scope,
        disaster_event_reference,
        failures,
        checks,
    )
    _check_resolution_semantics(event_cd_scope, failures, checks)

    if failures:
        _raise(failures)

    return _build_report(event_cd_scope, disaster_event_reference, checks)


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
        failures.append("Gold disaster event CD scope reference is empty.")
        return

    checks.append("row_count_nonzero")


def _check_primary_key(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    key = dataframe["event_cd_scope_key"].astype("string")

    if key.isna().any():
        failures.append("event_cd_scope_key contains nulls.")

    if key.duplicated().any():
        failures.append("event_cd_scope_key contains duplicates.")

    if not key.str.startswith("disaster_event_cd_scope__", na=False).all():
        failures.append("event_cd_scope_key has unexpected prefix.")

    checks.append("primary_key_valid")


def _check_event_cd_grain(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    duplicated = dataframe.duplicated(
        ["disaster_event_reference_key", "resolved_census_division_key"]
    )

    if duplicated.any():
        failures.append(
            "Table contains duplicate disaster_event_reference_key + "
            "resolved_census_division_key rows."
        )

    checks.append("event_cd_grain_valid")


def _check_boolean_flags(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    boolean_columns = [
        "is_csd_to_cd_approximation",
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    ]

    for column in boolean_columns:
        if dataframe[column].isna().any():
            failures.append(f"{column} contains nulls.")

    required_true_columns = [
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    ]

    for column in required_true_columns:
        if not dataframe[column].astype(bool).all():
            failures.append(f"{column} must be true for all CD scope rows.")

    checks.append("boolean_flags_valid")


def _check_allowed_values(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    invalid_provinces = sorted(
        set(dataframe["province_key"].dropna().astype(str)) - ALLOWED_PROVINCES
    )

    if invalid_provinces:
        failures.append(f"Invalid province_key values: {invalid_provinces}")

    invalid_cd_provinces = sorted(
        set(dataframe["census_division_province_key"].dropna().astype(str)) - ALLOWED_PROVINCES
    )

    if invalid_cd_provinces:
        failures.append(f"Invalid census_division_province_key values: {invalid_cd_provinces}")

    invalid_levels = sorted(
        set(dataframe["source_mapped_geo_level"].dropna().astype(str)) - ALLOWED_SOURCE_LEVELS
    )

    if invalid_levels:
        failures.append(f"Invalid source_mapped_geo_level values: {invalid_levels}")

    invalid_methods = sorted(
        set(dataframe["resolution_method"].dropna().astype(str)) - ALLOWED_RESOLUTION_METHODS
    )

    if invalid_methods:
        failures.append(f"Invalid resolution_method values: {invalid_methods}")

    invalid_domains = sorted(
        set(dataframe["disaster_domain"].dropna().astype(str)) - ALLOWED_DOMAINS
    )

    if invalid_domains:
        failures.append(f"Invalid disaster_domain values: {invalid_domains}")

    checks.append("allowed_values_valid")


def _check_reference_months(
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

    year = pd.to_numeric(dataframe["event_year"], errors="coerce")
    month_number = pd.to_numeric(dataframe["event_month_number"], errors="coerce")

    if not year.between(2016, 2025, inclusive="both").all():
        failures.append("event_year must be within 2016-2025 for all CD scope rows.")

    valid = parsed_month.notna()

    if valid.any():
        if (
            not year.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.year.astype("Int64"))
            .all()
        ):
            failures.append("event_year does not match reference_month.")

        if (
            not month_number.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.month.astype("Int64"))
            .all()
        ):
            failures.append("event_month_number does not match reference_month.")

    checks.append("reference_months_valid")


def _check_cd_spatial_coverage(
    event_cd_scope: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    if "census_division_key" not in cd_spatial_reference.columns:
        failures.append("CD spatial reference is missing census_division_key.")
        return

    known_cd = set(cd_spatial_reference["census_division_key"].astype(str))
    observed_cd = set(event_cd_scope["resolved_census_division_key"].astype(str))

    missing = sorted(observed_cd - known_cd)

    if missing:
        failures.append(
            "resolved_census_division_key values missing from CD spatial reference: " f"{missing}"
        )

    checks.append("cd_spatial_coverage_valid")


def _check_event_reference_coverage(
    event_cd_scope: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    required = {
        "disaster_event_reference_key",
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    }

    missing_columns = required - set(disaster_event_reference.columns)

    if missing_columns:
        failures.append(
            f"Disaster event reference is missing required columns: " f"{sorted(missing_columns)}"
        )
        return

    target = disaster_event_reference[
        disaster_event_reference["is_backtest_window"].astype(bool)
        & disaster_event_reference["is_ab_bc_scope"].astype(bool)
        & disaster_event_reference["is_domain_relevant"].astype(bool)
        & disaster_event_reference["is_grid_backtest_eligible"].astype(bool)
    ]

    expected_events = set(target["disaster_event_reference_key"].astype(str))
    observed_events = set(event_cd_scope["disaster_event_reference_key"].astype(str))

    if observed_events != expected_events:
        missing = sorted(expected_events - observed_events)
        extra = sorted(observed_events - expected_events)

        failures.append(
            "CD scope event set does not match grid-backtest eligible event set. "
            f"missing={missing}, extra={extra}"
        )

    checks.append("event_reference_coverage_valid")


def _check_resolution_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    csd_rows = dataframe["source_mapped_geo_level"].eq("CSD")
    non_csd_rows = ~csd_rows

    if not dataframe.loc[csd_rows, "resolution_method"].eq("csd_parent_cd").all():
        failures.append("CSD rows must use csd_parent_cd resolution_method.")

    if not dataframe.loc[csd_rows, "is_csd_to_cd_approximation"].astype(bool).all():
        failures.append("CSD rows must have is_csd_to_cd_approximation = true.")

    if not dataframe.loc[non_csd_rows, "resolution_method"].eq("direct_cd").all():
        failures.append("Non-CSD rows must use direct_cd resolution_method.")

    if dataframe.loc[non_csd_rows, "is_csd_to_cd_approximation"].astype(bool).any():
        failures.append("Non-CSD rows must not be marked as CSD approximation.")

    checks.append("resolution_semantics_valid")


def _build_report(
    dataframe: pd.DataFrame,
    disaster_event_reference: pd.DataFrame,
    checks: list[str],
) -> dict[str, Any]:
    target = disaster_event_reference[
        disaster_event_reference["is_backtest_window"].astype(bool)
        & disaster_event_reference["is_ab_bc_scope"].astype(bool)
        & disaster_event_reference["is_domain_relevant"].astype(bool)
        & disaster_event_reference["is_grid_backtest_eligible"].astype(bool)
    ]

    return {
        "table_name": "gold_disaster_event_cd_scope_reference",
        "validation_status": "passed",
        "checks_passed": checks,
        "check_count": len(checks),
        "row_count": int(len(dataframe)),
        "source_grid_backtest_event_count": int(len(target)),
        "unique_event_count": int(dataframe["disaster_event_reference_key"].nunique()),
        "unique_census_division_count": int(dataframe["resolved_census_division_key"].nunique()),
        "minimum_reference_month": str(dataframe["reference_month"].min()),
        "maximum_reference_month": str(dataframe["reference_month"].max()),
        "province_counts": _value_counts(dataframe["province_key"]),
        "cd_province_counts": _value_counts(dataframe["census_division_province_key"]),
        "disaster_domain_counts": _value_counts(dataframe["disaster_domain"]),
        "source_mapped_geo_level_counts": _value_counts(dataframe["source_mapped_geo_level"]),
        "resolution_method_counts": _value_counts(dataframe["resolution_method"]),
        "csd_to_cd_approximation_row_count": int(dataframe["is_csd_to_cd_approximation"].sum()),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _raise(failures: list[str]) -> None:
    message = "Gold disaster event CD scope reference validation failed:\n"
    message += "\n".join(f"- {failure}" for failure in failures)
    raise GoldDisasterEventCDScopeValidationError(message)
