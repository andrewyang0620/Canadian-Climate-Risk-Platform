from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.common.io import latest_table_parquet

from src.gold.spatial.municipality_bridge import (
    build_gold_grid_municipality_bridge,
)


TABLE_NAME = "gold_grid_municipality_bridge"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("Build the Gold grid-to-municipality bridge."))

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
    )
    parser.add_argument(
        "--gold-root",
        default="lakehouse/gold",
    )
    parser.add_argument(
        "--extract-date",
        default=None,
    )

    return parser.parse_args()


def run_grid_municipality_bridge(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = extract_date or datetime.now(timezone.utc).date().isoformat()
    run_id = str(uuid4())

    grid_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )
    municipality_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_boundary_municipality",
    )

    grid = pd.read_parquet(grid_path)
    municipality = pd.read_parquet(municipality_path)

    bridge, summary = build_gold_grid_municipality_bridge(
        grid_dataframe=grid,
        municipality_dataframe=municipality,
    )

    output_dir = (
        Path(gold_root) / TABLE_NAME / f"extract_date={final_extract_date}" / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"

    bridge.to_parquet(output_path, index=False)

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = metadata_dir / "metadata.json"

    metadata = {
        "table_name": TABLE_NAME,
        "run_id": run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        **summary,
        "bridge_row_counts_by_grid_system": {
            str(key): int(value)
            for key, value in bridge["grid_system"].value_counts().to_dict().items()
        },
        "matched_grid_counts_by_grid_system": {
            str(key): int(value)
            for key, value in bridge.groupby("grid_system")["grid_cell_key"]
            .nunique()
            .to_dict()
            .items()
        },
        "primary_municipality_row_count": int(bridge["is_primary_municipality"].sum()),
        "output_path": output_path.as_posix(),
        "input_paths": {
            "gold_grid_cell": grid_path.as_posix(),
            "silver_boundary_municipality": (municipality_path.as_posix()),
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
        "[OK] wrote Gold grid municipality bridge | "
        f"rows={len(bridge)} "
        f"matched_grids="
        f"{summary['matched_grid_cell_count']} "
        f"unmatched_grids="
        f"{summary['unmatched_grid_cell_count']} "
        f"run_id={run_id}"
    )

    return result


def main() -> None:
    args = parse_args()

    result = run_grid_municipality_bridge(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        extract_date=args.extract_date,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
