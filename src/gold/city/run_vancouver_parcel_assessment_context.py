from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_parcel_assessment_context import (
    build_gold_vancouver_parcel_assessment_context,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_parcel_assessment_context"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build latest Vancouver parcel assessment context."
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


def run_vancouver_parcel_assessment_context(
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

    bridge_path = latest_table_parquet(
        root=gold_root,
        table_name=(
            "gold_vancouver_property_parcel_bridge"
        ),
    )

    assessment_path = latest_table_parquet(
        root=gold_root,
        table_name=(
            "gold_vancouver_land_coordinate_assessment"
        ),
    )

    parcels = pd.read_parquet(
        parcel_path,
        columns=[
            "property_parcel_key",
            "city",
            "source_parcel_id",
            "source_tax_coord",
        ],
    )

    bridge = pd.read_parquet(
        bridge_path
    )

    assessments = pd.read_parquet(
        assessment_path
    )

    result, summary = (
        build_gold_vancouver_parcel_assessment_context(
            parcel_dataframe=parcels,
            bridge_dataframe=bridge,
            assessment_dataframe=assessments,
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

    output_path = (
        output_dir / f"{TABLE_NAME}.parquet"
    )

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
        "input_paths": {
            "silver_property_parcel": (
                parcel_path.as_posix()
            ),
            "gold_vancouver_property_parcel_bridge": (
                bridge_path.as_posix()
            ),
            "gold_vancouver_land_coordinate_assessment": (
                assessment_path.as_posix()
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
        "[OK] wrote Vancouver parcel assessment context | "
        f"rows={summary['output_row_count']} "
        f"latest_assessment="
        f"{summary['latest_assessment_parcel_count']} "
        f"exact_1to1="
        f"{summary['exact_1_to_1_assessment_parcel_count']} "
        f"ambiguous="
        f"{summary['ambiguous_assessment_parcel_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = run_vancouver_parcel_assessment_context(
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