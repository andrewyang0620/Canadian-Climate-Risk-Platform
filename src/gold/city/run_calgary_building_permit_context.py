from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.calgary_building_permit_context import (
    build_gold_calgary_building_permit_context,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_calgary_building_permit_context"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Calgary building-permit "
            "property-risk context Gold."
        )
    )

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


def run_calgary_building_permit_context(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    extract_date = (
        extract_date
        or datetime.now(
            timezone.utc
        ).date().isoformat()
    )

    run_id = str(uuid4())

    permit_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_building_permit",
    )

    location_path = latest_table_parquet(
        root=gold_root,
        table_name=(
            "gold_calgary_property_location_assessment"
        ),
    )

    flood_path = latest_table_parquet(
        root=gold_root,
        table_name=(
            "gold_calgary_property_location_flood_exposure"
        ),
    )

    permits = pd.read_parquet(
        permit_path
    )

    locations = pd.read_parquet(
        location_path
    )

    floods = pd.read_parquet(
        flood_path
    )

    result, summary = (
        build_gold_calgary_building_permit_context(
            permit_dataframe=permits,
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    output_dir = (
        Path(gold_root)
        / TABLE_NAME
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"{TABLE_NAME}.parquet"
    )

    result.to_parquet(
        output_path,
        index=False,
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )

    metadata_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_path = (
        metadata_dir / "metadata.json"
    )

    metadata = {
        "table_name": TABLE_NAME,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        **summary,
        "output_path": (
            output_path.as_posix()
        ),
        "input_paths": {
            "silver_building_permit": (
                permit_path.as_posix()
            ),
            "property_location_assessment": (
                location_path.as_posix()
            ),
            "property_location_flood_exposure": (
                flood_path.as_posix()
            ),
        },
    }

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Calgary building permit context | "
        f"rows={summary['output_row_count']} "
        f"housing="
        f"{summary['housing_related_permit_count']} "
        f"new_units="
        f"{summary['new_housing_units_created_sum']:.0f} "
        f"exact_matches="
        f"{summary['exact_location_match_count']} "
        f"flood_housing="
        f"{summary['flood_exposed_housing_permit_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": (
            metadata_path.as_posix()
        ),
    }


def main() -> None:
    args = parse_args()

    result = (
        run_calgary_building_permit_context(
            silver_root=args.silver_root,
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