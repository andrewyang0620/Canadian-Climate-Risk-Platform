from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.climate.monthly_features import (
    build_gold_climate_station_month_feature,
    build_gold_grid_month_climate_feature,
    read_silver_climate_daily,
)
from src.gold.common.io import latest_table_parquet


STATION_MONTH_TABLE = "gold_climate_station_month_feature"
GRID_MONTH_TABLE = "gold_grid_month_climate_feature"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold monthly climate features.")
    parser.add_argument("--silver-root", default="lakehouse/silver")
    parser.add_argument("--gold-root", default="lakehouse/gold")
    parser.add_argument("--extract-date", default=None)
    return parser.parse_args()


def run_climate_monthly_features(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = extract_date or datetime.now(timezone.utc).date().isoformat()
    run_id = str(uuid4())

    silver_climate_root = Path(silver_root) / "silver_climate_daily"
    grid_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )

    climate_daily = read_silver_climate_daily(silver_climate_root=silver_climate_root)
    grid = pd.read_parquet(grid_path)

    station_month = build_gold_climate_station_month_feature(climate_daily)
    grid_month, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    station_output_path = _write_table(
        dataframe=station_month,
        gold_root=gold_root,
        table_name=STATION_MONTH_TABLE,
        extract_date=final_extract_date,
        run_id=run_id,
    )
    grid_output_path = _write_table(
        dataframe=grid_month,
        gold_root=gold_root,
        table_name=GRID_MONTH_TABLE,
        extract_date=final_extract_date,
        run_id=run_id,
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / GRID_MONTH_TABLE
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = metadata_dir / "metadata.json"

    metadata = {
        "table_name": GRID_MONTH_TABLE,
        "run_id": run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        **summary.to_dict(),
        "station_month_output_path": station_output_path.as_posix(),
        "grid_month_output_path": grid_output_path.as_posix(),
        "input_paths": {
            "silver_climate_daily_root": str(silver_climate_root),
            "gold_grid_cell": grid_path.as_posix(),
        },
    }

    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    result = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }

    print(
        "[OK] wrote Gold monthly climate features | "
        f"station_month_rows={len(station_month)} "
        f"grid_month_rows={len(grid_month)} "
        f"run_id={run_id}"
    )

    return result


def _write_table(
    *,
    dataframe: pd.DataFrame,
    gold_root: str | Path,
    table_name: str,
    extract_date: str,
    run_id: str,
) -> Path:
    output_dir = Path(gold_root) / table_name / f"extract_date={extract_date}" / f"run_id={run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{table_name}.parquet"
    dataframe.to_parquet(output_path, index=False)

    return output_path


def main() -> None:
    args = parse_args()

    result = run_climate_monthly_features(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        extract_date=args.extract_date,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
