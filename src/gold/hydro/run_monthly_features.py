from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.gold.common.io import latest_table_parquet
from src.gold.hydro.monthly_features import (
    build_gold_grid_month_hydro_feature,
    build_gold_hydro_station_month_feature,
    read_silver_hydro_daily,
    read_silver_hydro_station,
    summarize_hydro_station_month,
)

GOLD_ROOT = Path("lakehouse/gold")
SILVER_ROOT = Path("lakehouse/silver")

STATION_MONTH_TABLE = "gold_hydro_station_month_feature"
GRID_MONTH_TABLE = "gold_grid_month_hydro_feature"


def run_gold_hydro_monthly_features(
    *,
    silver_root: str | Path = SILVER_ROOT,
    gold_root: str | Path = GOLD_ROOT,
    extract_date: str | None = None,
    run_id: str | None = None,
) -> dict:
    final_extract_date = extract_date or date.today().isoformat()
    final_run_id = run_id or str(uuid4())

    hydro_station = read_silver_hydro_station(
        silver_root=silver_root,
    )
    hydro_daily = read_silver_hydro_daily(
        silver_root=silver_root,
    )

    grid_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )
    grid = pd.read_parquet(grid_path)

    station_month = build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
    )

    grid_month, grid_summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=grid,
    )

    station_month_output_path = _write_gold_table(
        dataframe=station_month,
        gold_root=gold_root,
        table_name=STATION_MONTH_TABLE,
        extract_date=final_extract_date,
        run_id=final_run_id,
    )

    grid_month_output_path = _write_gold_table(
        dataframe=grid_month,
        gold_root=gold_root,
        table_name=GRID_MONTH_TABLE,
        extract_date=final_extract_date,
        run_id=final_run_id,
    )

    station_summary = summarize_hydro_station_month(station_month)

    metadata = {
        "table_name": GRID_MONTH_TABLE,
        "run_id": final_run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        "station_month": station_summary,
        "grid_month": grid_summary,
        "station_month_output_path": station_month_output_path.as_posix(),
        "grid_month_output_path": grid_month_output_path.as_posix(),
        "input_paths": {
            "silver_hydro_station_root": (Path(silver_root) / "silver_hydro_station").as_posix(),
            "silver_hydro_daily_root": (Path(silver_root) / "silver_hydro_daily").as_posix(),
            "gold_grid_cell": grid_path.as_posix(),
        },
    }

    metadata_path = (
        Path(gold_root)
        / "_metadata"
        / GRID_MONTH_TABLE
        / f"extract_date={final_extract_date}"
        / f"run_id={final_run_id}"
        / "metadata.json"
    )
    metadata_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    metadata["metadata_path"] = metadata_path.as_posix()

    return metadata


def _write_gold_table(
    *,
    dataframe: pd.DataFrame,
    gold_root: str | Path,
    table_name: str,
    extract_date: str,
    run_id: str,
) -> Path:
    output_path = (
        Path(gold_root)
        / table_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / f"{table_name}.parquet"
    )
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    dataframe.to_parquet(
        output_path,
        index=False,
    )

    return output_path


def main() -> None:
    metadata = run_gold_hydro_monthly_features()

    print(
        "[OK] wrote Gold hydro monthly features | "
        f"station_month_rows={metadata['station_month']['row_count']} "
        f"grid_month_rows={metadata['grid_month']['grid_month_row_count']} "
        f"run_id={metadata['run_id']}"
    )
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
