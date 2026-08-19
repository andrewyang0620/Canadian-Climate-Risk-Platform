from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.gold.city.calgary_development_permit_context import (
    build_gold_calgary_development_permit_context,
)
from src.gold.common.io import latest_table_parquet


BRIDGE_TABLE = (
    "gold_calgary_development_permit_location_bridge"
)

CONTEXT_TABLE = (
    "gold_calgary_development_permit_context"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Calgary development-permit "
            "property-location context Gold."
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


def main() -> None:
    args = parse_args()

    extract_date = (
        args.extract_date
        or datetime.now(
            timezone.utc
        ).date().isoformat()
    )

    run_id = str(
        uuid4()
    )

    permit_path = latest_table_parquet(
        root=args.silver_root,
        table_name="silver_development_permit",
    )

    location_path = latest_table_parquet(
        root=args.gold_root,
        table_name=(
            "gold_calgary_property_location_assessment"
        ),
    )

    flood_path = latest_table_parquet(
        root=args.gold_root,
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

    bridge, context, summary = (
        build_gold_calgary_development_permit_context(
            permit_dataframe=permits,
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    output_paths = {}

    for table_name, dataframe in (
        (
            BRIDGE_TABLE,
            bridge,
        ),
        (
            CONTEXT_TABLE,
            context,
        ),
    ):
        output_dir = (
            Path(args.gold_root)
            / table_name
            / f"extract_date={extract_date}"
            / f"run_id={run_id}"
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            output_dir
            / f"{table_name}.parquet"
        )

        dataframe.to_parquet(
            output_path,
            index=False,
        )

        output_paths[
            table_name
        ] = output_path.as_posix()

    metadata_dir = (
        Path(args.gold_root)
        / "_metadata"
        / CONTEXT_TABLE
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
        "table_name": CONTEXT_TABLE,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        **summary,
        "output_paths": output_paths,
        "input_paths": {
            "silver_development_permit": (
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
        "[OK] wrote Calgary development permit context | "
        f"permits={summary['context_output_row_count']} "
        f"bridge_rows={summary['bridge_row_count']} "
        f"mapped={summary['mapped_permit_count']} "
        f"multi_property="
        f"{summary['multi_property_permit_count']} "
        f"flood_exposed="
        f"{summary['flood_exposed_permit_count']} "
        f"run_id={run_id}"
    )

    print(
        json.dumps(
            {
                **metadata,
                "metadata_path": (
                    metadata_path.as_posix()
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()