from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.disaster.reference import (
    build_gold_disaster_event_reference,
    load_location_mapping,
)


SILVER_ROOT = Path("lakehouse/silver")
GOLD_ROOT = Path("lakehouse/gold")
TABLE_NAME = "gold_disaster_event_reference"
MAPPING_PATH = Path("configs/backtesting/disaster_location_mapping.json")


def main() -> None:
    extract_date = date.today().isoformat()
    run_id = str(uuid.uuid4())

    source_path = latest_table_parquet(
        root=SILVER_ROOT,
        table_name="silver_disaster_event_month",
    )

    disaster_event_month = pd.read_parquet(source_path)
    location_mapping = load_location_mapping(MAPPING_PATH)

    reference, summary = build_gold_disaster_event_reference(
        disaster_event_month=disaster_event_month,
        location_mapping=location_mapping,
    )

    output_dir = GOLD_ROOT / TABLE_NAME / f"extract_date={extract_date}" / f"run_id={run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"
    reference.to_parquet(output_path, index=False)

    metadata = {
        **summary,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "output_path": output_path.as_posix(),
        "input_tables": [
            "silver_disaster_event_month",
        ],
        "input_paths": {
            "silver_disaster_event_month": source_path.as_posix(),
            "location_mapping": MAPPING_PATH.as_posix(),
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
        "[OK] wrote Gold disaster event reference | "
        f"rows={summary['row_count']} "
        f"backtest_window={summary['backtest_window_event_count']} "
        f"backtest_eligible={summary['backtest_eligible_event_count']} "
        f"grid_backtest_eligible={summary['grid_backtest_eligible_event_count']} "
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
