from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.disaster.grid_month_label_validation import (
    validate_gold_grid_month_disaster_event_label,
)


GOLD_ROOT = Path("lakehouse/gold")

GRID_MONTH_LABEL_TABLE = "gold_grid_month_disaster_event_label"
EVENT_GRID_SCOPE_TABLE = "gold_disaster_event_grid_scope"
DISASTER_EVENT_REFERENCE_TABLE = "gold_disaster_event_reference"
GRID_CELL_TABLE = "gold_grid_cell"


def main() -> None:
    grid_month_label_path = _latest_table_parquet(GRID_MONTH_LABEL_TABLE)
    event_grid_scope_path = _latest_table_parquet(EVENT_GRID_SCOPE_TABLE)
    disaster_reference_path = _latest_table_parquet(DISASTER_EVENT_REFERENCE_TABLE)
    grid_cell_path = _latest_table_parquet(GRID_CELL_TABLE)

    grid_month_label = pd.read_parquet(grid_month_label_path)
    event_grid_scope = pd.read_parquet(event_grid_scope_path)
    disaster_event_reference = pd.read_parquet(disaster_reference_path)
    grid_cell = pd.read_parquet(grid_cell_path)

    report = validate_gold_grid_month_disaster_event_label(
        grid_month_label=grid_month_label,
        event_grid_scope=event_grid_scope,
        disaster_event_reference=disaster_event_reference,
        grid_cell=grid_cell,
    )

    print(
        "[OK] Gold grid-month disaster event label validation passed | "
        f"checks={report['check_count']} "
        f"rows={report['row_count']} "
        f"grids={report['grid_count']} "
        f"months={report['month_count']} "
        f"positive={report['positive_label_row_count']}"
    )
    print(
        json.dumps(
            _json_safe(report),
            indent=2,
            ensure_ascii=False,
        )
    )


def _latest_table_parquet(table_name: str) -> Path:
    table_path = GOLD_ROOT / table_name
    files = list(table_path.rglob(f"{table_name}.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found for {table_name}: " f"{table_path}")

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )


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
