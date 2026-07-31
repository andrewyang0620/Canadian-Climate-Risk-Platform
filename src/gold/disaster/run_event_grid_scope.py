from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.disaster.event_grid_scope import (
    TABLE_NAME,
    build_gold_disaster_event_grid_scope,
)


GOLD_ROOT = Path("lakehouse/gold")

EVENT_CD_SCOPE_TABLE = "gold_disaster_event_cd_scope_reference"
CD_SPATIAL_TABLE = "gold_disaster_cd_spatial_reference"
GRID_CELL_TABLE = "gold_grid_cell"


def main() -> None:
    event_cd_scope_path = _latest_table_parquet(EVENT_CD_SCOPE_TABLE)
    cd_spatial_path = _latest_table_parquet(CD_SPATIAL_TABLE)
    grid_cell_path = _latest_table_parquet(GRID_CELL_TABLE)

    event_cd_scope = pd.read_parquet(event_cd_scope_path)
    cd_spatial_reference = pd.read_parquet(cd_spatial_path)
    grid_cell = pd.read_parquet(grid_cell_path)

    result, summary = build_gold_disaster_event_grid_scope(
        event_cd_scope=event_cd_scope,
        cd_spatial_reference=cd_spatial_reference,
        grid_cell=grid_cell,
    )

    run_id = str(uuid4())
    extract_date = date.today().isoformat()

    output_dir = GOLD_ROOT / TABLE_NAME / f"extract_date={extract_date}" / f"run_id={run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"
    result.to_parquet(output_path, index=False)

    metadata_dir = (
        GOLD_ROOT / "_metadata" / TABLE_NAME / f"extract_date={extract_date}" / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        **summary,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "output_path": str(output_path),
        "input_tables": [
            EVENT_CD_SCOPE_TABLE,
            CD_SPATIAL_TABLE,
            GRID_CELL_TABLE,
        ],
        "input_paths": {
            EVENT_CD_SCOPE_TABLE: str(event_cd_scope_path),
            CD_SPATIAL_TABLE: str(cd_spatial_path),
            GRID_CELL_TABLE: str(grid_cell_path),
        },
    }

    metadata_path = metadata_dir / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            _json_safe(metadata),
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Gold disaster event grid scope | "
        f"rows={summary['row_count']} "
        f"events={summary['unique_event_count']} "
        f"grids={summary['unique_grid_cell_count']} "
        f"run_id={run_id}"
    )
    print(
        json.dumps(
            _json_safe(metadata),
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
