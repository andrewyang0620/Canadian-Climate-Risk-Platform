from __future__ import annotations

import pytest

from src.gold.disaster.reference import build_gold_disaster_event_reference
from src.gold.disaster.validation import (
    GoldDisasterReferenceValidationError,
    validate_gold_disaster_event_reference,
)

from tests.unit.test_gold_disaster_event_reference import (
    _location_mapping,
    _source_frame,
)


def _valid_reference():
    result, _ = build_gold_disaster_event_reference(
        disaster_event_month=_source_frame(),
        location_mapping=_location_mapping(),
    )
    return result


def test_validate_gold_disaster_event_reference_passes_for_valid_table() -> None:
    result = _valid_reference()

    report = validate_gold_disaster_event_reference(result)

    assert report["validation_status"] == "passed"
    assert report["row_count"] == 3
    assert report["backtest_eligible_event_count"] == 3
    assert report["backtest_window_grid_eligible_event_count"] == 2


def test_validate_gold_disaster_event_reference_rejects_duplicate_primary_key() -> None:
    result = _valid_reference()
    result.loc[1, "disaster_event_reference_key"] = result.loc[0, "disaster_event_reference_key"]

    with pytest.raises(GoldDisasterReferenceValidationError, match="duplicates"):
        validate_gold_disaster_event_reference(result)


def test_validate_gold_disaster_event_reference_rejects_grid_rows_without_codes() -> None:
    result = _valid_reference()
    result.loc[0, "mapped_geo_codes_json"] = "[]"

    with pytest.raises(
        GoldDisasterReferenceValidationError,
        match="Grid-eligible rows must have non-empty",
    ):
        validate_gold_disaster_event_reference(result)


def test_validate_gold_disaster_event_reference_rejects_province_as_grid_eligible() -> None:
    result = _valid_reference()
    result.loc[1, "is_grid_backtest_eligible"] = True

    with pytest.raises(GoldDisasterReferenceValidationError):
        validate_gold_disaster_event_reference(result)


def test_validate_gold_disaster_event_reference_rejects_inconsistent_domain_flag() -> None:
    result = _valid_reference()
    result.loc[0, "is_wildfire_domain_relevant"] = False

    with pytest.raises(
        GoldDisasterReferenceValidationError,
        match="is_wildfire_domain_relevant is inconsistent",
    ):
        validate_gold_disaster_event_reference(result)
