from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.disaster.cd_spatial_validation import (
    validate_gold_disaster_cd_spatial_reference,
)


GOLD_ROOT = Path("lakehouse/gold")
TABLE_NAME = "gold_disaster_cd_spatial_reference"


def main() -> None:
    source_path = latest_table_parquet(
        root=GOLD_ROOT,
        table_name=TABLE_NAME,
    )

    dataframe = pd.read_parquet(source_path)
    report = validate_gold_disaster_cd_spatial_reference(dataframe)

    print(
        "[OK] Gold disaster CD spatial reference validation passed | "
        f"checks={report['check_count']} "
        f"rows={report['row_count']} "
        f"provinces={report['province_counts']}"
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
