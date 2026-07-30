from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.disaster.event_cd_scope_validation import (
    validate_gold_disaster_event_cd_scope_reference,
)


GOLD_ROOT = Path("lakehouse/gold")
EVENT_CD_SCOPE_TABLE = "gold_disaster_event_cd_scope_reference"
EVENT_REFERENCE_TABLE = "gold_disaster_event_reference"
CD_SPATIAL_REFERENCE_TABLE = "gold_disaster_cd_spatial_reference"


def main() -> None:
    event_cd_scope_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=EVENT_CD_SCOPE_TABLE,
    )
    event_reference_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=EVENT_REFERENCE_TABLE,
    )
    cd_spatial_reference_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=CD_SPATIAL_REFERENCE_TABLE,
    )

    event_cd_scope = pd.read_parquet(event_cd_scope_path)
    event_reference = pd.read_parquet(event_reference_path)
    cd_spatial_reference = pd.read_parquet(cd_spatial_reference_path)

    report = validate_gold_disaster_event_cd_scope_reference(
        event_cd_scope=event_cd_scope,
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_spatial_reference,
    )

    print(
        "[OK] Gold disaster event CD scope reference validation passed | "
        f"checks={report['check_count']} "
        f"rows={report['row_count']} "
        f"events={report['unique_event_count']} "
        f"cds={report['unique_census_division_count']}"
    )
    print(json.dumps(_json_safe(report), indent=2, ensure_ascii=False))


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_json_safe(item) for item in value]

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


if __name__ == "__main__":
    main()
