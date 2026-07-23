from __future__ import annotations

from typing import Any

import pandas as pd


class GoldDisasterCDSpatialReferenceValidationError(Exception):
    """Raised when Gold disaster CD spatial reference validation fails."""


REQUIRED_COLUMNS = [
    "census_division_key",
    "census_division_name",
    "census_division_type",
    "dguid",
    "province_uid",
    "province_key",
    "land_area_sq_km",
    "geometry_area_m2",
    "geometry_crs_epsg",
    "geometry_wkt",
]

ALLOWED_PROVINCES = {"AB", "BC"}
EXPECTED_EPSG = 3347


def validate_gold_disaster_cd_spatial_reference(
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    failures: list[str] = []
    checks: list[str] = []

    _check_required_columns(dataframe, failures, checks)

    if failures:
        _raise(failures)

    _check_row_count(dataframe, failures, checks)
    _check_primary_key(dataframe, failures, checks)
    _check_province_values(dataframe, failures, checks)
    _check_expected_ab_bc_counts(dataframe, failures, checks)
    _check_geometry_fields(dataframe, failures, checks)
    _check_area_fields(dataframe, failures, checks)
    _check_crs(dataframe, failures, checks)
    _check_known_mapping_support(dataframe, failures, checks)

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
        failures.append("Gold disaster CD spatial reference is empty.")
        return

    checks.append("row_count_nonzero")


def _check_primary_key(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    key = dataframe["census_division_key"].astype("string")

    if key.isna().any():
        failures.append("census_division_key contains nulls.")

    if key.duplicated().any():
        failures.append("census_division_key contains duplicates.")

    invalid_length = ~key.str.fullmatch(r"\d{4}")
    if invalid_length.any():
        failures.append("census_division_key must be a 4-digit CDUID string.")

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

    expected_from_key = dataframe["census_division_key"].astype(str).str[:2].map(
        {"48": "AB", "59": "BC"}
    )

    if not dataframe["province_key"].astype(str).eq(expected_from_key).all():
        failures.append("province_key is inconsistent with census_division_key prefix.")

    checks.append("province_values_valid")


def _check_expected_ab_bc_counts(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    counts = dataframe["province_key"].value_counts().to_dict()

    ab_count = int(counts.get("AB", 0))
    bc_count = int(counts.get("BC", 0))

    if ab_count != 19:
        failures.append(f"Expected 19 Alberta Census Divisions, found {ab_count}.")

    if bc_count != 29:
        failures.append(f"Expected 29 British Columbia Census Divisions, found {bc_count}.")

    if len(dataframe) != 48:
        failures.append(f"Expected 48 AB/BC Census Divisions, found {len(dataframe)}.")

    checks.append("expected_ab_bc_cd_counts_valid")


def _check_geometry_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    geometry = dataframe["geometry_wkt"].astype("string")

    if geometry.isna().any():
        failures.append("geometry_wkt contains nulls.")

    invalid_prefix = ~geometry.str.startswith(("POLYGON", "MULTIPOLYGON"), na=False)
    if invalid_prefix.any():
        failures.append("geometry_wkt must start with POLYGON or MULTIPOLYGON.")

    checks.append("geometry_fields_valid")


def _check_area_fields(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    land_area = pd.to_numeric(dataframe["land_area_sq_km"], errors="coerce")
    geometry_area = pd.to_numeric(dataframe["geometry_area_m2"], errors="coerce")

    if land_area.isna().any():
        failures.append("land_area_sq_km contains nulls or non-numeric values.")

    if geometry_area.isna().any():
        failures.append("geometry_area_m2 contains nulls or non-numeric values.")

    if not (land_area > 0).all():
        failures.append("land_area_sq_km must be positive.")

    if not (geometry_area > 0).all():
        failures.append("geometry_area_m2 must be positive.")

    checks.append("area_fields_positive")


def _check_crs(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    crs_values = set(pd.to_numeric(dataframe["geometry_crs_epsg"], errors="coerce").dropna())

    if crs_values != {EXPECTED_EPSG}:
        failures.append(f"geometry_crs_epsg must be {EXPECTED_EPSG}, found {sorted(crs_values)}.")

    checks.append("crs_valid")


def _check_known_mapping_support(
    dataframe: pd.DataFrame,
    failures: list[str],
    checks: list[str],
) -> None:
    required_cd_keys = {
        "4801",
        "4802",
        "4803",
        "4804",
        "4805",
        "4806",
        "4811",
        "4816",
        "4817",
        "5905",
        "5909",
        "5915",
        "5929",
        "5931",
        "5933",
        "5935",
        "5955",
        "5959",
    }

    observed = set(dataframe["census_division_key"].astype(str))
    missing = sorted(required_cd_keys - observed)

    if missing:
        failures.append(
            "Missing CD keys required by disaster location mapping v1: "
            f"{missing}"
        )

    checks.append("known_disaster_mapping_cd_keys_present")


def _build_report(dataframe: pd.DataFrame, checks: list[str]) -> dict[str, Any]:
    return {
        "table_name": "gold_disaster_cd_spatial_reference",
        "validation_status": "passed",
        "checks_passed": checks,
        "check_count": len(checks),
        "row_count": int(len(dataframe)),
        "unique_census_division_key_count": int(dataframe["census_division_key"].nunique()),
        "province_counts": _value_counts(dataframe["province_key"]),
        "minimum_census_division_key": str(dataframe["census_division_key"].min()),
        "maximum_census_division_key": str(dataframe["census_division_key"].max()),
        "geometry_null_count": int(dataframe["geometry_wkt"].isna().sum()),
        "geometry_area_positive_count": int((dataframe["geometry_area_m2"] > 0).sum()),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _raise(failures: list[str]) -> None:
    message = "Gold disaster CD spatial reference validation failed:\n"
    message += "\n".join(f"- {failure}" for failure in failures)
    raise GoldDisasterCDSpatialReferenceValidationError(message)
