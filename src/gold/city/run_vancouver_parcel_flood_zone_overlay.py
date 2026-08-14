from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.vancouver_parcel_flood_zone_overlay import (
    ANALYSIS_CRS_EPSG,
    SOURCE_CRS_EPSG,
    build_gold_vancouver_parcel_flood_zone_overlay,
)
from src.gold.common.io import latest_table_parquet


TABLE_NAME = "gold_vancouver_parcel_flood_zone_overlay"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Vancouver parcel -- flood source-zone Gold overlay."
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
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=10_000,
    )

    return parser.parse_args()


def run_vancouver_parcel_flood_zone_overlay(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
    progress_interval: int = 10_000,
) -> dict[str, Any]:
    final_extract_date = (
        extract_date
        or datetime.now(timezone.utc).date().isoformat()
    )

    created_at = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid4())

    parcel_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_property_parcel",
    )
    flood_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_flood_hazard_zone",
    )

    parcels = pd.read_parquet(
        parcel_path,
        columns=[
            "property_parcel_key",
            "city",
            "source_name",
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
            "source_name",
            "source_properties_json",
        ],
    )

    overlay, summary = build_gold_vancouver_parcel_flood_zone_overlay(
        parcel_dataframe=parcels,
        flood_dataframe=floods,
        progress_interval=progress_interval,
    )

    output_dir = (
        Path(gold_root)
        / TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"

    overlay.to_parquet(
        output_path,
        index=False,
    )

    spatial_audit = {
        "run_id": run_id,
        "join_name": "vancouver_parcel_flood_zone_overlay",
        "left_table": "silver_property_parcel",
        "right_table": "silver_flood_hazard_zone",
        "left_count": summary["parcel_input_count"],
        "matched_count": summary["matched_parcel_count"],
        "unmatched_count": summary["unmatched_parcel_count"],
        "match_rate": summary["parcel_match_rate"],
        "median_distance_km": None,
        "p95_distance_km": None,
        "geometry_invalid_count": (
            summary["parcel_geometry_invalid_count"]
            + summary["flood_geometry_invalid_count"]
        ),
        "geometry_repaired_count": (
            summary["parcel_geometry_repaired_count"]
            + summary["flood_geometry_repaired_count"]
        ),
        "crs_source": f"EPSG:{SOURCE_CRS_EPSG}",
        "crs_target": f"EPSG:{ANALYSIS_CRS_EPSG}",
        "created_at": created_at,
        "severity": "info",
    }

    audit_dir = (
        Path(gold_root)
        / "_audit"
        / "spatial_join"
        / "vancouver_parcel_flood_zone_overlay"
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    audit_dir.mkdir(parents=True, exist_ok=True)

    audit_path = audit_dir / "audit.json"

    audit_path.write_text(
        json.dumps(
            spatial_audit,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = metadata_dir / "metadata.json"

    metadata = {
        "table_name": TABLE_NAME,
        "run_id": run_id,
        "extract_date": final_extract_date,
        "created_at": created_at,
        "load_status": "success",
        **summary,
        "spatial_audit": spatial_audit,
        "output_path": output_path.as_posix(),
        "audit_path": audit_path.as_posix(),
        "input_paths": {
            "silver_property_parcel": parcel_path.as_posix(),
            "silver_flood_hazard_zone": flood_path.as_posix(),
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
        "[OK] wrote Vancouver parcel flood-zone overlay | "
        f"rows={summary['overlay_row_count']} "
        f"matched_parcels={summary['matched_parcel_count']} "
        f"match_rate={summary['parcel_match_rate']:.4%} "
        f"candidate_pairs={summary['candidate_pair_count']} "
        f"boundary_touches={summary['boundary_touch_only_pair_count']} "
        f"repairs={spatial_audit['geometry_repaired_count']} "
        f"run_id={run_id}"
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = run_vancouver_parcel_flood_zone_overlay(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        extract_date=args.extract_date,
        progress_interval=args.progress_interval,
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()