from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.gold.city.calgary_property_location_flood import (
    build_gold_calgary_property_location_flood,
)
from src.gold.common.io import latest_table_parquet


OVERLAY_TABLE = (
    "gold_calgary_property_location_flood_overlay"
)

EXPOSURE_TABLE = (
    "gold_calgary_property_location_flood_exposure"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Calgary property-location "
            "flood overlay and exposure Gold."
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

    run_id = str(uuid4())

    location_path = latest_table_parquet(
        root=args.gold_root,
        table_name=(
            "gold_calgary_property_location_assessment"
        ),
    )

    flood_path = latest_table_parquet(
        root=args.silver_root,
        table_name="silver_flood_hazard_zone",
    )

    locations = pd.read_parquet(
        location_path,
        columns=[
            "source_parcel_id",
            "geometry_wkt",
        ],
    )

    floods = pd.read_parquet(
        flood_path,
        columns=[
            "flood_hazard_zone_key",
            "city",
            "source_zone_id",
            "hazard_class",
            "geometry_wkt",
        ],
    )

    overlay, exposure, summary = (
        build_gold_calgary_property_location_flood(
            location_dataframe=locations,
            flood_dataframe=floods,
        )
    )

    output_paths = {}

    for table_name, dataframe in (
        (OVERLAY_TABLE, overlay),
        (EXPOSURE_TABLE, exposure),
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
        / EXPOSURE_TABLE
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
        "table_name": EXPOSURE_TABLE,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        **summary,
        "output_paths": output_paths,
        "input_paths": {
            "property_location_assessment": (
                location_path.as_posix()
            ),
            "silver_flood_hazard_zone": (
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
        "[OK] wrote Calgary property-location flood | "
        f"overlay_rows="
        f"{summary['overlay_row_count']} "
        f"locations="
        f"{summary['location_output_count']} "
        f"flood_exposed="
        f"{summary['flood_exposed_location_count']} "
        f"river_channel_only="
        f"{summary['normal_river_channel_only_location_count']} "
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