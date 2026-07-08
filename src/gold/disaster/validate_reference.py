from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.disaster.validation import validate_gold_disaster_event_reference


GOLD_ROOT = Path("lakehouse/gold")
TABLE_NAME = "gold_disaster_event_reference"


def main() -> None:
    source_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=TABLE_NAME,
    )

    dataframe = pd.read_parquet(source_path)
    report = validate_gold_disaster_event_reference(dataframe)

    print(
        "[OK] Gold disaster event reference validation passed | "
        f"checks={report['check_count']} "
        f"rows={report['row_count']} "
        f"backtest_eligible={report['backtest_eligible_event_count']} "
        f"grid_backtest_eligible={report['backtest_window_grid_eligible_event_count']}"
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
