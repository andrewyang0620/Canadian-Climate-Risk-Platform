from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


TABLE_NAME = "gold_disaster_event_cd_scope_reference"

GRID_SCOPE_LEVELS = {"CD", "CD_GROUP", "CSD"}


class GoldDisasterEventCDScopeError(Exception):
    """Raised when disaster event CD scope reference build fails."""


def build_gold_disaster_event_cd_scope_reference(
    *,
    disaster_event_reference: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _validate_inputs(disaster_event_reference, cd_spatial_reference)

    known_cd = set(cd_spatial_reference["census_division_key"].astype(str))

    target = disaster_event_reference[
        disaster_event_reference["is_backtest_window"].astype(bool)
        & disaster_event_reference["is_ab_bc_scope"].astype(bool)
        & disaster_event_reference["is_domain_relevant"].astype(bool)
        & disaster_event_reference["is_grid_backtest_eligible"].astype(bool)
    ].copy()

    rows_by_event_cd: dict[tuple[str, str], dict[str, Any]] = {}

    for _, event in target.iterrows():
        event_key = str(event["disaster_event_reference_key"])
        mapped_level = str(event["mapped_geo_level"])
        mapped_codes = _parse_codes(event["mapped_geo_codes_json"])

        if mapped_level not in GRID_SCOPE_LEVELS:
            raise GoldDisasterEventCDScopeError(
                f"Grid-eligible event has unsupported mapped_geo_level: "
                f"{event_key} -> {mapped_level}"
            )

        if not mapped_codes:
            raise GoldDisasterEventCDScopeError(
                f"Grid-eligible event has no mapped geo codes: {event_key}"
            )

        for source_code in mapped_codes:
            resolved_cd, resolution_method = _resolve_to_cd(
                source_code=source_code,
                mapped_level=mapped_level,
                known_cd=known_cd,
                event_key=event_key,
            )

            row_key = (event_key, resolved_cd)

            if row_key not in rows_by_event_cd:
                rows_by_event_cd[row_key] = {
                    "event_cd_scope_key": (
                        f"disaster_event_cd_scope__{event_key}__cd_{resolved_cd}"
                    ),
                    "disaster_event_reference_key": event_key,
                    "source_disaster_event_key": _nullable_str(
                        event.get("source_disaster_event_key")
                    ),
                    "reference_month": _nullable_str(event.get("reference_month")),
                    "event_year": _nullable_int(event.get("event_year")),
                    "event_month_number": _nullable_int(event.get("event_month_number")),
                    "province_key": _nullable_str(event.get("province_key")),
                    "disaster_domain": _nullable_str(event.get("disaster_domain")),
                    "location_text": _nullable_str(event.get("location_text")),
                    "location_tier": _nullable_str(event.get("location_tier")),
                    "source_mapped_geo_level": mapped_level,
                    "source_mapped_geo_codes": [],
                    "resolved_census_division_key": resolved_cd,
                    "resolution_methods": [],
                    "is_csd_to_cd_approximation": False,
                    "mapping_confidence": _nullable_str(event.get("mapping_confidence")),
                    "mapping_method": _nullable_str(event.get("mapping_method")),
                    "is_backtest_window": bool(event.get("is_backtest_window")),
                    "is_ab_bc_scope": bool(event.get("is_ab_bc_scope")),
                    "is_domain_relevant": bool(event.get("is_domain_relevant")),
                    "is_grid_backtest_eligible": bool(event.get("is_grid_backtest_eligible")),
                }

            rows_by_event_cd[row_key]["source_mapped_geo_codes"].append(source_code)
            rows_by_event_cd[row_key]["resolution_methods"].append(resolution_method)

            if resolution_method == "csd_parent_cd":
                rows_by_event_cd[row_key]["is_csd_to_cd_approximation"] = True

    rows = []

    for row in rows_by_event_cd.values():
        source_codes = sorted(set(row.pop("source_mapped_geo_codes")))
        methods = sorted(set(row.pop("resolution_methods")))

        row["source_mapped_geo_codes_json"] = json.dumps(source_codes)
        row["resolution_method"] = methods[0] if len(methods) == 1 else "mixed"
        rows.append(row)

    result = pd.DataFrame(rows)

    if result.empty:
        raise GoldDisasterEventCDScopeError(
            "No grid-eligible disaster event CD scope rows were produced."
        )

    result = result.merge(
        cd_spatial_reference[
            [
                "census_division_key",
                "census_division_name",
                "census_division_type",
                "province_key",
            ]
        ].rename(
            columns={
                "census_division_key": "resolved_census_division_key",
                "province_key": "census_division_province_key",
            }
        ),
        on="resolved_census_division_key",
        how="left",
        validate="many_to_one",
    )

    result = (
        result[
            [
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
        ]
        .sort_values(
            [
                "reference_month",
                "disaster_event_reference_key",
                "resolved_census_division_key",
            ]
        )
        .reset_index(drop=True)
    )

    _validate_result(result, target, known_cd)

    summary = _build_summary(result, target)

    return result, summary


def _validate_inputs(
    disaster_event_reference: pd.DataFrame,
    cd_spatial_reference: pd.DataFrame,
) -> None:
    required_event_columns = {
        "disaster_event_reference_key",
        "source_disaster_event_key",
        "reference_month",
        "event_year",
        "event_month_number",
        "province_key",
        "disaster_domain",
        "location_text",
        "location_tier",
        "mapped_geo_level",
        "mapped_geo_codes_json",
        "mapping_confidence",
        "mapping_method",
        "is_backtest_window",
        "is_ab_bc_scope",
        "is_domain_relevant",
        "is_grid_backtest_eligible",
    }

    required_cd_columns = {
        "census_division_key",
        "census_division_name",
        "census_division_type",
        "province_key",
    }

    missing_event = required_event_columns - set(disaster_event_reference.columns)
    missing_cd = required_cd_columns - set(cd_spatial_reference.columns)

    if missing_event:
        raise GoldDisasterEventCDScopeError(
            f"Missing event reference columns: {sorted(missing_event)}"
        )

    if missing_cd:
        raise GoldDisasterEventCDScopeError(
            f"Missing CD spatial reference columns: {sorted(missing_cd)}"
        )


def _resolve_to_cd(
    *,
    source_code: str,
    mapped_level: str,
    known_cd: set[str],
    event_key: str,
) -> tuple[str, str]:
    code = str(source_code)

    if mapped_level in {"CD", "CD_GROUP"}:
        if code not in known_cd:
            raise GoldDisasterEventCDScopeError(
                f"Mapped CD code not found in CD spatial reference: " f"{event_key} -> {code}"
            )

        return code, "direct_cd"

    if mapped_level == "CSD":
        parent_cd = code[:4]

        if parent_cd not in known_cd:
            raise GoldDisasterEventCDScopeError(
                f"CSD parent CD not found in CD spatial reference: "
                f"{event_key} -> {code} -> {parent_cd}"
            )

        return parent_cd, "csd_parent_cd"

    raise GoldDisasterEventCDScopeError(
        f"Unsupported mapped_geo_level for grid scope: {event_key} -> {mapped_level}"
    )


def _parse_codes(value: Any) -> list[str]:
    try:
        parsed = json.loads(value)
    except Exception as exc:
        raise GoldDisasterEventCDScopeError(
            f"mapped_geo_codes_json is invalid JSON: {value}"
        ) from exc

    if not isinstance(parsed, list):
        raise GoldDisasterEventCDScopeError(f"mapped_geo_codes_json must be a list: {value}")

    return [str(item) for item in parsed]


def _validate_result(
    result: pd.DataFrame,
    target: pd.DataFrame,
    known_cd: set[str],
) -> None:
    if result.empty:
        raise GoldDisasterEventCDScopeError("Output table is empty.")

    if result["event_cd_scope_key"].isna().any():
        raise GoldDisasterEventCDScopeError("event_cd_scope_key contains nulls.")

    if result["event_cd_scope_key"].duplicated().any():
        raise GoldDisasterEventCDScopeError("event_cd_scope_key contains duplicates.")

    duplicate_event_cd = result.duplicated(
        ["disaster_event_reference_key", "resolved_census_division_key"]
    )
    if duplicate_event_cd.any():
        raise GoldDisasterEventCDScopeError("Output contains duplicate event-CD scope rows.")

    output_events = set(result["disaster_event_reference_key"].astype(str))
    expected_events = set(target["disaster_event_reference_key"].astype(str))

    if output_events != expected_events:
        missing = sorted(expected_events - output_events)
        extra = sorted(output_events - expected_events)
        raise GoldDisasterEventCDScopeError(
            f"Output event set does not match target event set. "
            f"missing={missing}, extra={extra}"
        )

    output_cd = set(result["resolved_census_division_key"].astype(str))
    missing_cd = sorted(output_cd - known_cd)

    if missing_cd:
        raise GoldDisasterEventCDScopeError(
            f"Output has CD keys missing from spatial reference: {missing_cd}"
        )


def _build_summary(result: pd.DataFrame, target: pd.DataFrame) -> dict[str, Any]:
    return {
        "table_name": TABLE_NAME,
        "row_count": int(len(result)),
        "source_grid_backtest_event_count": int(len(target)),
        "unique_event_count": int(result["disaster_event_reference_key"].nunique()),
        "unique_census_division_count": int(result["resolved_census_division_key"].nunique()),
        "minimum_reference_month": str(result["reference_month"].min()),
        "maximum_reference_month": str(result["reference_month"].max()),
        "province_counts": _value_counts(result["province_key"]),
        "cd_province_counts": _value_counts(result["census_division_province_key"]),
        "disaster_domain_counts": _value_counts(result["disaster_domain"]),
        "source_mapped_geo_level_counts": _value_counts(result["source_mapped_geo_level"]),
        "resolution_method_counts": _value_counts(result["resolution_method"]),
        "csd_to_cd_approximation_row_count": int(result["is_csd_to_cd_approximation"].sum()),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _nullable_str(value: Any) -> str | None:
    if pd.isna(value):
        return None

    return str(value)


def _nullable_int(value: Any) -> int | None:
    if pd.isna(value):
        return None

    return int(value)
