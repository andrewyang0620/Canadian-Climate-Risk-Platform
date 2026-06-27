from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.gold.mart.risk_monthly_grid import (
    MART_TABLE_NAME,
    build_gold_grid_month_risk_feature_mart,
    read_gold_risk_mart_inputs,
)


GOLD_ROOT = Path("lakehouse/gold")


def run_gold_grid_month_risk_feature_mart(
    *,
    gold_root: str | Path = GOLD_ROOT,
    extract_date: str | None = None,
    run_id: str | None = None,
) -> dict:
    final_extract_date = extract_date or date.today().isoformat()
    final_run_id = run_id or str(uuid4())

    inputs = read_gold_risk_mart_inputs(
        gold_root=gold_root,
    )

    mart, summary = build_gold_grid_month_risk_feature_mart(
        grid=inputs["grid"],
        municipality_bridge=inputs["municipality_bridge"],
        climate_grid_month=inputs["climate_grid_month"],
        hydro_grid_month=inputs["hydro_grid_month"],
    )

    output_path = (
        Path(gold_root)
        / MART_TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={final_run_id}"
        / f"{MART_TABLE_NAME}.parquet"
    )
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    mart.to_parquet(
        output_path,
        index=False,
    )

    metadata = {
        "table_name": MART_TABLE_NAME,
        "run_id": final_run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        **summary,
        "output_path": output_path.as_posix(),
        "input_tables": [
            "gold_grid_cell",
            "gold_grid_municipality_bridge",
            "gold_grid_month_climate_feature",
            "gold_grid_month_hydro_feature",
        ],
    }

    metadata_path = (
        Path(gold_root)
        / "_metadata"
        / MART_TABLE_NAME
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


def main() -> None:
    metadata = run_gold_grid_month_risk_feature_mart()

    print(
        "[OK] wrote Gold grid-month risk feature mart | "
        f"rows={metadata['row_count']} "
        f"grid_cells={metadata['grid_cell_count']} "
        f"months={metadata['month_count']} "
        f"run_id={metadata['run_id']}"
    )
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
