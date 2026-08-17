from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_parcel_flood_scenario_exposure import (
    build_gold_vancouver_parcel_flood_scenario_exposure,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_parcel_flood_scenario_exposure"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Vancouver parcel × flood scenario Gold exposure."
        )
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


def run_vancouver_parcel_flood_scenario_exposure(
    *,
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = (
        extract_date
        or datetime.now(timezone.utc).date().isoformat()
    )

    run_id = str(uuid4())

    overlay_path = latest_table_parquet(
        root=gold_root,
        table_name=(
            "gold_vancouver_parcel_flood_zone_overlay"
        ),
    )

    overlay = pd.read_parquet(
        overlay_path,
        columns=[
            "property_parcel_key",
            "source_parcel_id",
            "flood_hazard_zone_key",
            "scenario_name",
            "parcel_area_sq_m",
            "intersection_area_sq_m",
            "intersection_geometry_wkt_3347",
            "crs_epsg",
        ],
    )

    result, summary = (
        build_gold_vancouver_parcel_flood_scenario_exposure(
            overlay_dataframe=overlay,
        )
    )

    output_dir = (
        Path(gold_root)
        / TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / f"{TABLE_NAME}.parquet"

    result.to_parquet(
        output_path,
        index=False,
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_path = metadata_dir / "metadata.json"

    metadata = {
        "table_name": TABLE_NAME,
        "run_id": run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        **summary,
        "output_path": output_path.as_posix(),
        "input_path": overlay_path.as_posix(),
    }

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Vancouver parcel flood scenario exposure | "
        f"rows={summary['scenario_exposure_row_count']} "
        f"parcels={summary['parcel_count']} "
        f"scenarios={summary['scenario_count']} "
        f"multi_zone_groups="
        f"{summary['multi_zone_scenario_row_count']} "
        f"union_adjusted="
        f"{summary['union_adjusted_row_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = (
        run_vancouver_parcel_flood_scenario_exposure(
            gold_root=args.gold_root,
            extract_date=args.extract_date,
        )
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()