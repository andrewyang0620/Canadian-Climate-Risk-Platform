from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_land_coordinate_assessment import (
    build_gold_vancouver_land_coordinate_assessment,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_land_coordinate_assessment"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Vancouver land-coordinate/year "
            "assessment Gold."
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


def run_vancouver_land_coordinate_assessment(
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
        table_name="silver_property_tax_assessment",
    )

    dataframe = pd.read_parquet(
        input_path,
        columns=[
            "source_land_coordinate",
            "source_pid",
            "source_folio",
            "current_land_value",
            "current_improvement_value",
            "current_total_assessed_value",
            "previous_land_value",
            "previous_improvement_value",
            "previous_total_assessed_value",
            "tax_levy",
            "tax_assessment_year",
            "report_year",
            "zoning_district",
            "zoning_classification",
            "neighbourhood_code",
        ],
    )

    result, summary = (
        build_gold_vancouver_land_coordinate_assessment(
            property_tax_dataframe=dataframe,
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
        "[OK] wrote Vancouver land-coordinate assessment | "
        f"rows={summary['output_row_count']} "
        f"years={summary['report_year_min']}-"
        f"{summary['report_year_max']} "
        f"latest_rows="
        f"{summary['latest_report_year_row_count']} "
        f"multi_record_groups="
        f"{summary['multi_record_group_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = run_vancouver_land_coordinate_assessment(
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