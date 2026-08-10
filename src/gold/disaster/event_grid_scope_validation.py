from __future__ import annotations

import json
from typing import Any

import pandas as pd


TABLE_NAME = "gold_disaster_event_grid_scope"

TARGET_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
ALLOWED_PROVINCES = {"AB", "BC"}
ALLOWED_DOMAINS = {
    "wildfire",
    "flood",
    "severe_storm_or_climate",
    "climate_extreme",
}
ALLOWED_MAPPED_LEVELS = {"CD", "CD_GROUP", "CSD"}
ALLOWED_RESOLUTION_METHODS = {"direct_cd", "csd_parent_cd"}

REQUIRED_COLUMNS = {
    "event_grid_scope_key",
    "disaster_event_reference_key",
    "source_disaster_event_key",
    "reference_month",
    "event_year",
    "event_month_number",
    "province_key",
    "disaster_domain",
    "location_text",
    "location_tier",
    "grid_cell_key",
    "grid_system",
    "grid_province_key",
    "grid_analysis_area_sq_km",
    "grid_geometry_area_sq_km",
    "matched_census_division_keys_json",
    "matched_census_division_count",
    "source_event_cd_scope_keys_json",
    "source_mapped_geo_levels_json",
    "resolution_methods_json",
    "mapping_confidences_json",
    "mapping_methods_json",
    "affected_overlap_area_sq_km",
    "affected_grid_coverage_ratio",
    "maximum_single_cd_coverage_ratio",
    "is_csd_to_cd_approximation",
    "is_backtest_window",
    "is_ab_bc_scope",
    "is_domain_relevant",
    "is_grid_backtest_eligible",
}


class GoldDisasterEventGridScopeValidationError(Exception):
    """Raised when Gold disaster event grid scope validation fails."""


def validate_gold_disaster_event_grid_scope(
    *,
    event_grid_scope: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
    grid_cell: pd.DataFrame,
) -> dict[str, Any]:
    failures: list[str] = []
    checks: list[str] = []

    _check_required_columns(event_grid_scope, failures, checks)

    if failures:
        _raise(failures)

    _check_nonempty(event_grid_scope, failures, checks)
    _check_primary_key(event_grid_scope, failures, checks)
    _check_event_grid_grain(event_grid_scope, failures, checks)
    _check_flags(event_grid_scope, failures, checks)
    _check_allowed_values(event_grid_scope, failures, checks)
    _check_time_fields(event_grid_scope, failures, checks)
    _check_numeric_spatial_fields(event_grid_scope, failures, checks)
    _check_json_fields(event_grid_scope, failures, checks)
    _check_event_coverage(
        event_grid_scope,
        event_cd_scope,
        failures,
        checks,
    )
    _check_event_cd_lineage(
        event_grid_scope,
        event_cd_scope,
        failures,
        checks,
    )
    _check_grid_coverage(
        event_grid_scope,
        grid_cell,
        failures,
        checks,
    )
    _check_cd_coverage(
        event_grid_scope,
        event_cd_scope,
        cd_spatial_reference,
        failures,
        checks,
    )
    _check_province_grid_semantics(event_grid_scope, failures, checks)

    if failures:
        _raise(failures)

    return _build_report(
        event_grid_scope=event_grid_scope,
        event_cd_scope=event_cd_scope,
        checks=checks,
    )


def _check_required_columns(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(dataframe.columns))

    if missing:
        failures.append(f"Missing required columns: {missing}")
        return

    checks.append("required_columns_present")


def _check_nonempty(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    if dataframe.empty:
        failures.append("Gold disaster event grid scope is empty.")
        return

    checks.append("row_count_nonzero")


def _check_primary_key(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    key = dataframe["event_grid_scope_key"].astype("string")

    if key.isna().any():
        failures.append("event_grid_scope_key contains nulls.")

    if key.duplicated().any():
        failures.append("event_grid_scope_key contains duplicates.")

    if not key.str.startswith(
        "disaster_event_grid_scope__",
        na=False,
    ).all():
        failures.append("event_grid_scope_key has an unexpected prefix.")

    checks.append("primary_key_valid")


def _check_event_grid_grain(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    duplicates = dataframe.duplicated(["disaster_event_reference_key", "grid_cell_key"])

    if duplicates.any():
        failures.append("Duplicate disaster_event_reference_key + grid_cell_key rows found.")

    checks.append("event_grid_grain_valid")


def _check_flags(
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

    required_true = [
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    ]

    for column in required_true:
        if not dataframe[column].fillna(False).astype(bool).all():
            failures.append(f"{column} must be true for every output row.")

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

    invalid_grid_provinces = sorted(
        set(dataframe["grid_province_key"].dropna().astype(str)) - ALLOWED_PROVINCES
    )

    if invalid_grid_provinces:
        failures.append(f"Invalid grid_province_key values: {invalid_grid_provinces}")

    invalid_grid_systems = sorted(
        set(dataframe["grid_system"].dropna().astype(str)) - TARGET_GRID_SYSTEMS
    )

    if invalid_grid_systems:
        failures.append(f"Invalid grid_system values: {invalid_grid_systems}")

    invalid_domains = sorted(
        set(dataframe["disaster_domain"].dropna().astype(str)) - ALLOWED_DOMAINS
    )

    if invalid_domains:
        failures.append(f"Invalid disaster_domain values: {invalid_domains}")

    checks.append("allowed_values_valid")


def _check_time_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    parsed_month = pd.to_datetime(
        dataframe["reference_month"].astype("string") + "-01",
        errors="coerce",
    )
    event_year = pd.to_numeric(
        dataframe["event_year"],
        errors="coerce",
    )
    event_month_number = pd.to_numeric(
        dataframe["event_month_number"],
        errors="coerce",
    )

    if parsed_month.isna().any():
        failures.append("reference_month contains unparsable values.")

    if event_year.isna().any():
        failures.append("event_year contains invalid values.")

    if event_month_number.isna().any():
        failures.append("event_month_number contains invalid values.")

    if not event_year.between(2016, 2025, inclusive="both").all():
        failures.append("event_year must be within 2016-2025.")

    valid = parsed_month.notna() & event_year.notna() & event_month_number.notna()

    if valid.any():
        if (
            not event_year.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.year.astype("Int64"))
            .all()
        ):
            failures.append("event_year does not match reference_month.")

        if (
            not event_month_number.loc[valid]
            .astype("Int64")
            .eq(parsed_month.loc[valid].dt.month.astype("Int64"))
            .all()
        ):
            failures.append("event_month_number does not match reference_month.")

    checks.append("time_fields_valid")


def _check_numeric_spatial_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    positive_columns = [
        "grid_analysis_area_sq_km",
        "grid_geometry_area_sq_km",
        "affected_overlap_area_sq_km",
    ]

    for column in positive_columns:
        values = pd.to_numeric(dataframe[column], errors="coerce")

        if values.isna().any():
            failures.append(f"{column} contains invalid numeric values.")
        elif not values.gt(0.0).all():
            failures.append(f"{column} must be greater than zero.")

    ratio_columns = [
        "affected_grid_coverage_ratio",
        "maximum_single_cd_coverage_ratio",
    ]

    for column in ratio_columns:
        values = pd.to_numeric(dataframe[column], errors="coerce")

        if values.isna().any():
            failures.append(f"{column} contains invalid numeric values.")
        elif not values.between(
            0.0,
            1.0,
            inclusive="right",
        ).all():
            failures.append(f"{column} must be within (0, 1].")

    matched_count = pd.to_numeric(
        dataframe["matched_census_division_count"],
        errors="coerce",
    )

    if matched_count.isna().any():
        failures.append("matched_census_division_count contains invalid values.")
    elif not matched_count.ge(1).all():
        failures.append("matched_census_division_count must be at least one.")

    checks.append("numeric_spatial_fields_valid")


def _check_json_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    json_columns = [
        "matched_census_division_keys_json",
        "source_event_cd_scope_keys_json",
        "source_mapped_geo_levels_json",
        "resolution_methods_json",
        "mapping_confidences_json",
        "mapping_methods_json",
    ]

    parsed_columns: dict[str, list[list[str]]] = {}

    for column in json_columns:
        parsed_values: list[list[str]] = []

        for row_number, value in enumerate(dataframe[column]):
            try:
                parsed = json.loads(str(value))
            except Exception:
                failures.append(f"{column} contains invalid JSON at row {row_number}.")
                continue

            if not isinstance(parsed, list):
                failures.append(f"{column} must contain a JSON list at row {row_number}.")
                continue

            normalized = [str(item) for item in parsed]

            if not normalized:
                failures.append(f"{column} contains an empty list at row {row_number}.")

            parsed_values.append(normalized)

        parsed_columns[column] = parsed_values

    if failures:
        return

    matched_lists = parsed_columns["matched_census_division_keys_json"]
    expected_counts = pd.to_numeric(
        dataframe["matched_census_division_count"],
        errors="coerce",
    ).astype("Int64")

    actual_counts = pd.Series(
        [len(set(values)) for values in matched_lists],
        index=dataframe.index,
        dtype="Int64",
    )

    if not actual_counts.eq(expected_counts).all():
        failures.append("matched_census_division_count does not match JSON list length.")

    mapped_levels = {
        value for values in parsed_columns["source_mapped_geo_levels_json"] for value in values
    }

    invalid_levels = sorted(mapped_levels - ALLOWED_MAPPED_LEVELS)

    if invalid_levels:
        failures.append(f"Invalid mapped geo levels in JSON: {invalid_levels}")

    resolution_methods = {
        value for values in parsed_columns["resolution_methods_json"] for value in values
    }

    invalid_methods = sorted(resolution_methods - ALLOWED_RESOLUTION_METHODS)

    if invalid_methods:
        failures.append(f"Invalid resolution methods in JSON: {invalid_methods}")

    checks.append("json_fields_valid")


def _check_event_coverage(
    event_grid_scope: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    expected = set(event_cd_scope["disaster_event_reference_key"].astype(str))
    observed = set(event_grid_scope["disaster_event_reference_key"].astype(str))

    if observed != expected:
        failures.append(
            "Event-grid event set does not match event-CD source. "
            f"missing={sorted(expected - observed)}, "
            f"extra={sorted(observed - expected)}"
        )

    checks.append("event_coverage_valid")


def _check_event_cd_lineage(
    event_grid_scope: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    expected_scope_keys = set(event_cd_scope["event_cd_scope_key"].astype(str))
    observed_scope_keys: set[str] = set()

    event_to_allowed_cd = (
        event_cd_scope.groupby("disaster_event_reference_key")["resolved_census_division_key"]
        .apply(lambda values: set(values.astype(str)))
        .to_dict()
    )

    for row in event_grid_scope[
        [
            "disaster_event_reference_key",
            "matched_census_division_keys_json",
            "source_event_cd_scope_keys_json",
        ]
    ].itertuples(index=False):
        event_key = str(row.disaster_event_reference_key)

        matched_cd = {
            str(value) for value in json.loads(str(row.matched_census_division_keys_json))
        }
        source_scope_keys = {
            str(value) for value in json.loads(str(row.source_event_cd_scope_keys_json))
        }

        observed_scope_keys.update(source_scope_keys)

        allowed_cd = event_to_allowed_cd.get(event_key, set())

        if not matched_cd.issubset(allowed_cd):
            failures.append(
                "Event-grid row contains a CD outside the event-CD scope: "
                f"event={event_key}, "
                f"unexpected={sorted(matched_cd - allowed_cd)}"
            )
            break

    if observed_scope_keys != expected_scope_keys:
        failures.append(
            "Event-grid lineage does not cover the complete event-CD scope. "
            f"missing={sorted(expected_scope_keys - observed_scope_keys)}, "
            f"extra={sorted(observed_scope_keys - expected_scope_keys)}"
        )

    checks.append("event_cd_lineage_valid")


def _check_grid_coverage(
    event_grid_scope: pd.DataFrame,
    grid_cell: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    target_grid = grid_cell[grid_cell["grid_system"].isin(TARGET_GRID_SYSTEMS)]

    known_grid_keys = set(target_grid["grid_cell_key"].astype(str))
    observed_grid_keys = set(event_grid_scope["grid_cell_key"].astype(str))

    unknown = sorted(observed_grid_keys - known_grid_keys)

    if unknown:
        failures.append(f"Unknown grid_cell_key values found: {unknown[:20]}")

    checks.append("grid_coverage_valid")


def _check_cd_coverage(
    event_grid_scope: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    known_cd = set(cd_spatial_reference["census_division_key"].astype(str))
    source_cd = set(event_cd_scope["resolved_census_division_key"].astype(str))
    observed_cd: set[str] = set()

    for value in event_grid_scope["matched_census_division_keys_json"]:
        observed_cd.update(str(item) for item in json.loads(str(value)))

    unknown = sorted(observed_cd - known_cd)

    if unknown:
        failures.append(f"Matched CD keys missing from spatial reference: {unknown}")

    if observed_cd != source_cd:
        failures.append(
            "Matched CD set does not match event-CD source. "
            f"missing={sorted(source_cd - observed_cd)}, "
            f"extra={sorted(observed_cd - source_cd)}"
        )

    checks.append("cd_coverage_valid")


def _check_province_grid_semantics(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    province_mismatch = dataframe[dataframe["province_key"].ne(dataframe["grid_province_key"])]

    if not province_mismatch.empty:
        failures.append("province_key and grid_province_key must match.")

    expected_grid_system = dataframe["grid_province_key"].map(
        {
            "AB": "ab_10km",
            "BC": "bc_10km",
        }
    )

    if not dataframe["grid_system"].eq(expected_grid_system).all():
        failures.append("grid_system does not match grid_province_key.")

    checks.append("province_grid_semantics_valid")


def _build_report(
    *,
    event_grid_scope: pd.DataFrame,
    event_cd_scope: pd.DataFrame,
    checks: list[str],
) -> dict[str, Any]:
    grids_per_event = event_grid_scope.groupby("disaster_event_reference_key").size()

    return {
        "table_name": TABLE_NAME,
        "validation_status": "passed",
        "checks_passed": checks,
        "check_count": len(checks),
        "row_count": int(len(event_grid_scope)),
        "source_event_cd_scope_row_count": int(len(event_cd_scope)),
        "unique_event_count": int(event_grid_scope["disaster_event_reference_key"].nunique()),
        "unique_grid_cell_count": int(event_grid_scope["grid_cell_key"].nunique()),
        "unique_census_division_count": int(
            event_cd_scope["resolved_census_division_key"].nunique()
        ),
        "minimum_reference_month": str(event_grid_scope["reference_month"].min()),
        "maximum_reference_month": str(event_grid_scope["reference_month"].max()),
        "province_counts": _value_counts(event_grid_scope["province_key"]),
        "grid_system_counts": _value_counts(event_grid_scope["grid_system"]),
        "disaster_domain_counts": _value_counts(event_grid_scope["disaster_domain"]),
        "csd_approximation_event_grid_row_count": int(
            event_grid_scope["is_csd_to_cd_approximation"].sum()
        ),
        "grids_per_event_min": int(grids_per_event.min()),
        "grids_per_event_median": float(grids_per_event.median()),
        "grids_per_event_mean": float(grids_per_event.mean()),
        "grids_per_event_max": int(grids_per_event.max()),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _raise(failures: list[str]) -> None:
    message = "Gold disaster event grid scope validation failed:\n"
    message += "\n".join(f"- {failure}" for failure in failures)
    raise GoldDisasterEventGridScopeValidationError(message)
