from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.disaster.event_cd_scope import (
    TABLE_NAME,
    build_gold_disaster_event_cd_scope_reference,
)


GOLD_ROOT = Path("lakehouse/gold")
EVENT_REFERENCE_TABLE = "gold_disaster_event_reference"
CD_SPATIAL_TABLE = "gold_disaster_cd_spatial_reference"


def main() -> None:
    event_reference_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=EVENT_REFERENCE_TABLE,
    )
    cd_spatial_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=CD_SPATIAL_TABLE,
    )

    event_reference = pd.read_parquet(event_reference_path)
    cd_spatial_reference = pd.read_parquet(cd_spatial_path)

    event_cd_scope, summary = build_gold_disaster_event_cd_scope_reference(
        disaster_event_reference=event_reference,
        cd_spatial_reference=cd_spatial_reference,
    )

    extract_date = date.today().isoformat()
    run_id = str(uuid.uuid4())

    output_dir = GOLD_ROOT / TABLE_NAME / f"extract_date={extract_date}" / f"run_id={run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"
    event_cd_scope.to_parquet(output_path, index=False)

    metadata = {
        **summary,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "output_path": output_path.as_posix(),
        "input_tables": [
            EVENT_REFERENCE_TABLE,
            CD_SPATIAL_TABLE,
        ],
        "input_paths": {
            EVENT_REFERENCE_TABLE: event_reference_path.as_posix(),
            CD_SPATIAL_TABLE: cd_spatial_path.as_posix(),
        },
    }

    metadata_path = (
        GOLD_ROOT
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "metadata.json"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(_json_safe(metadata), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Gold disaster event CD scope reference | "
        f"rows={summary['row_count']} "
        f"events={summary['unique_event_count']} "
        f"cds={summary['unique_census_division_count']} "
        f"run_id={run_id}"
    )
    print(json.dumps(_json_safe(metadata), indent=2, ensure_ascii=False))


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
