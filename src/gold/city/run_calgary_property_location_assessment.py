from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.calgary_property_location_assessment import (
    build_gold_calgary_property_location_assessment,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_calgary_property_location_assessment"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Calgary property-location assessment Gold."
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


def run_calgary_property_location_assessment(
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

    input_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_property_assessment",
    )

    dataframe = pd.read_parquet(
        input_path,
        columns=[
            "property_assessment_key",
            "city",
            "source_property_id",
            "source_parcel_id",
            "source_unique_key",
            "assessment_year",
            "assessed_value_total",
            "assessed_value_residential",
            "assessed_value_non_residential",
            "assessed_value_farmland",
            "assessment_class",
            "assessment_class_description",
            "community_code",
            "community_name",
            "year_of_construction",
            "land_use_designation",
            "property_type",
            "sub_property_use",
            "geometry_wkt",
            "source_name",
        ],
    )

    result, summary = (
        build_gold_calgary_property_location_assessment(
            assessment_dataframe=dataframe,
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
        "input_path": input_path.as_posix(),
    }

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Calgary property-location assessment | "
        f"rows={summary['output_row_count']} "
        f"multi_record="
        f"{summary['multi_record_location_count']} "
        f"max_records="
        f"{summary['maximum_assessment_records_per_location']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = (
        run_calgary_property_location_assessment(
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