from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.disaster.event_grid_scope_validation import (
    validate_gold_disaster_event_grid_scope,
)


GOLD_ROOT = Path("lakehouse/gold")

EVENT_GRID_SCOPE_TABLE = "gold_disaster_event_grid_scope"
EVENT_CD_SCOPE_TABLE = "gold_disaster_event_cd_scope_reference"
CD_SPATIAL_TABLE = "gold_disaster_cd_spatial_reference"
GRID_CELL_TABLE = "gold_grid_cell"


def main() -> None:
    event_grid_scope_path = _latest_table_parquet(EVENT_GRID_SCOPE_TABLE)
    event_cd_scope_path = _latest_table_parquet(EVENT_CD_SCOPE_TABLE)
    cd_spatial_path = _latest_table_parquet(CD_SPATIAL_TABLE)
    grid_cell_path = _latest_table_parquet(GRID_CELL_TABLE)

    event_grid_scope = pd.read_parquet(event_grid_scope_path)
    event_cd_scope = pd.read_parquet(event_cd_scope_path)
    cd_spatial_reference = pd.read_parquet(cd_spatial_path)
    grid_cell = pd.read_parquet(grid_cell_path)

    report = validate_gold_disaster_event_grid_scope(
        event_grid_scope=event_grid_scope,
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_spatial_reference,
        grid_cell=grid_cell,
    )

    print(
        "[OK] Gold disaster event grid scope validation passed | "
        f"checks={report['check_count']} "
        f"rows={report['row_count']} "
        f"events={report['unique_event_count']} "
        f"grids={report['unique_grid_cell_count']}"
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
