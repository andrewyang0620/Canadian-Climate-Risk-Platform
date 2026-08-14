from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_property_parcel_bridge import (
    build_gold_vancouver_property_parcel_bridge,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_property_parcel_bridge"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Gold Vancouver property-to-parcel bridge."
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


def run_vancouver_property_parcel_bridge(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = (
        extract_date
        or datetime.now(timezone.utc).date().isoformat()
    )
    run_id = str(uuid4())

    parcel_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_property_parcel",
    )

    tax_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_property_tax_assessment",
    )

    parcels = pd.read_parquet(
        parcel_path,
        columns=[
            "property_parcel_key",
            "source_parcel_id",
            "source_tax_coord",
        ],
    )

    property_tax = pd.read_parquet(
        tax_path,
        columns=[
            "source_land_coordinate",
        ],
    )

    bridge, summary = (
        build_gold_vancouver_property_parcel_bridge(
            parcel_dataframe=parcels,
            property_tax_dataframe=property_tax,
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

    bridge.to_parquet(
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
        "input_paths": {
            "silver_property_parcel": parcel_path.as_posix(),
            "silver_property_tax_assessment": (
                tax_path.as_posix()
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

    result = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }

    print(
        "[OK] wrote Vancouver property-to-parcel bridge | "
        f"rows={len(bridge)} "
        f"land_coordinates="
        f"{summary['matched_land_coordinate_count']} "
        f"tax_row_match_rate="
        f"{summary['tax_row_match_rate']:.4%} "
        f"ambiguous_land_coordinates="
        f"{summary['ambiguous_land_coordinate_count']} "
        f"max_parcels_per_land_coordinate="
        f"{summary['maximum_parcel_count_for_land_coordinate']} "
        f"run_id={run_id}"
    )

    return result


def main() -> None:
    args = parse_args()

    result = run_vancouver_property_parcel_bridge(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        extract_date=args.extract_date,
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()