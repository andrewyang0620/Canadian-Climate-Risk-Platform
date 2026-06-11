from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from uuid import uuid4

from src.silver.statcan_building_permit_month import (
    DEFAULT_TARGET_GEOS,
    build_statcan_building_permit_month_silver,
    latest_statcan_building_permits_raw_path,
)
from src.utils.config import load_project_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build StatCan building permit monthly Silver table."
    )

    parser.add_argument(
        "--bronze-root",
        default="lakehouse/bronze",
        help="Bronze input root.",
    )

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--extract-date",
        default=date.today().isoformat(),
        help="Silver extract date partition.",
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=250_000,
        help="CSV chunksize for large StatCan raw file.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_project_config("source_config.yml")
    source = config["sources"]["statcan_building_permits"]
    table_name = source.get(
        "target_silver_table",
        "silver_statcan_building_permit_month",
    )

    raw_path = latest_statcan_building_permits_raw_path(
        bronze_root=args.bronze_root,
    )

    dataframe = build_statcan_building_permit_month_silver(
        raw_path,
        target_geos=DEFAULT_TARGET_GEOS,
        chunksize=args.chunksize,
    )

    run_id = str(uuid4())
    output_dir = (
        Path(args.silver_root)
        / table_name
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{table_name}.parquet"
    dataframe.to_parquet(output_path, index=False)

    metadata_dir = (
        Path(args.silver_root)
        / "_metadata"
        / "statcan_building_permit_month"
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "table_name": table_name,
        "run_id": run_id,
        "extract_date": args.extract_date,
        "row_count": int(len(dataframe)),
        "source_names": sorted(dataframe["source_name"].dropna().unique().tolist()),
        "statcan_table_ids": sorted(dataframe["statcan_table_id"].dropna().unique().tolist()),
        "target_geos": sorted(DEFAULT_TARGET_GEOS),
        "reference_year_min": int(dataframe["reference_year"].dropna().min()),
        "reference_year_max": int(dataframe["reference_year"].dropna().max()),
        "output_path": output_path.as_posix(),
        "raw_path": Path(raw_path).as_posix(),
    }

    (metadata_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(
        "[OK] wrote StatCan building permit monthly Silver outputs | "
        f"table={table_name} "
        f"rows={len(dataframe)} "
        f"years={metadata['reference_year_min']}-{metadata['reference_year_max']} "
        f"run_id={run_id}"
    )


if __name__ == "__main__":
    main()
