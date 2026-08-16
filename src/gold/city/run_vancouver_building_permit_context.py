from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_building_permit_context import (
    build_gold_vancouver_building_permit_context,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_building_permit_context"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Vancouver building permit Gold context."
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


def run_vancouver_building_permit_context(
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

    permit_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_building_permit",
    )

    parcel_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_property_parcel",
    )

    flood_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_vancouver_parcel_flood_exposure",
    )

    assessment_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_vancouver_parcel_assessment_context",
    )

    permits = pd.read_parquet(
        permit_path,
        columns=[
            "building_permit_key",
            "city",
            "permit_number",
            "permit_type_mapped",
            "permit_class_group",
            "permit_class_mapped",
            "work_class_mapped",
            "issue_date",
            "issue_year",
            "year_month",
            "address_text",
            "project_description",
            "estimated_project_cost",
            "neighbourhood_name",
            "latitude",
            "longitude",
            "geometry_wkt",
        ],
    )

    parcels = pd.read_parquet(
        parcel_path,
        columns=[
            "property_parcel_key",
            "city",
            "geometry_wkt",
        ],
    )

    flood = pd.read_parquet(
        flood_path,
        columns=[
            "property_parcel_key",
            "is_flood_exposed",
            "scenario_count",
            "designated_floodplain_flag",
            "designated_floodplain_overlap_ratio",
            "fraser_risk_today_flag",
            "fraser_risk_today_overlap_ratio",
            "still_creek_floodplain_flag",
            "still_creek_floodplain_overlap_ratio",
            "wave_effect_zone_flag",
            "wave_effect_zone_overlap_ratio",
        ],
    )

    assessment = pd.read_parquet(
        assessment_path,
        columns=[
            "property_parcel_key",
            "has_latest_assessment",
            "assessment_mapping_ambiguous",
            "assessment_mapping_exact_1_to_1",
            "report_year",
            "land_coordinate_current_land_value",
            "land_coordinate_current_improvement_value",
            "land_coordinate_current_total_assessed_value",
            "exact_mapped_current_land_value",
            "exact_mapped_current_improvement_value",
            "exact_mapped_current_total_assessed_value",
        ],
    )

    result, summary = build_gold_vancouver_building_permit_context(
        permit_dataframe=permits,
        parcel_dataframe=parcels,
        flood_dataframe=flood,
        assessment_dataframe=assessment,
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
        "input_paths": {
            "silver_building_permit": permit_path.as_posix(),
            "silver_property_parcel": parcel_path.as_posix(),
            "gold_vancouver_parcel_flood_exposure": flood_path.as_posix(),
            "gold_vancouver_parcel_assessment_context": (
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
        "[OK] wrote Vancouver building permit context | "
        f"rows={summary['output_row_count']} "
        f"housing_related={summary['housing_related_permit_count']} "
        f"exact_matches={summary['exact_parcel_match_count']} "
        f"ambiguous_matches={summary['ambiguous_parcel_match_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = run_vancouver_building_permit_context(
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