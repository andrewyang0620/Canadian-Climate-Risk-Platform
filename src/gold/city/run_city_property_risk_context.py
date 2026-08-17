from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.city.city_property_risk_context import (
    build_gold_calgary_property_risk_context,
    build_gold_vancouver_parcel_risk_context,
)
from src.gold.common.io import latest_table_parquet


VANCOUVER_TABLE = "gold_vancouver_parcel_risk_context"
CALGARY_TABLE = "gold_calgary_property_risk_context"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build final Vancouver and Calgary "
            "property-level spatial risk context Gold."
        )
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


def run_city_property_risk_context(
    *,
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = (
        extract_date
        or datetime.now(timezone.utc).date().isoformat()
    )

    run_id = str(uuid4())

    paths = {
        "grid": latest_table_parquet(
            root=gold_root,
            table_name="gold_grid_cell",
        ),
        "vancouver_assessment": latest_table_parquet(
            root=gold_root,
            table_name="gold_vancouver_parcel_assessment_context",
        ),
        "vancouver_flood": latest_table_parquet(
            root=gold_root,
            table_name="gold_vancouver_parcel_flood_exposure",
        ),
        "calgary_assessment": latest_table_parquet(
            root=gold_root,
            table_name="gold_calgary_property_location_assessment",
        ),
        "calgary_flood": latest_table_parquet(
            root=gold_root,
            table_name="gold_calgary_property_location_flood_exposure",
        ),
    }

    grids = pd.read_parquet(
        paths["grid"],
        columns=[
            "grid_cell_key",
            "grid_system",
            "cell_size_m",
            "full_cell_geometry_wkt",
            "crs_epsg",
        ],
    )

    vancouver_assessment = pd.read_parquet(
        paths["vancouver_assessment"]
    )
    vancouver_flood = pd.read_parquet(
        paths["vancouver_flood"]
    )
    calgary_assessment = pd.read_parquet(
        paths["calgary_assessment"]
    )
    calgary_flood = pd.read_parquet(
        paths["calgary_flood"]
    )

    vancouver, vancouver_summary = (
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=vancouver_assessment,
            flood_dataframe=vancouver_flood,
            grid_dataframe=grids,
        )
    )

    calgary, calgary_summary = (
        build_gold_calgary_property_risk_context(
            assessment_dataframe=calgary_assessment,
            flood_dataframe=calgary_flood,
            grid_dataframe=grids,
        )
    )

    vancouver_metadata = _write_output(
        dataframe=vancouver,
        table_name=VANCOUVER_TABLE,
        gold_root=gold_root,
        extract_date=final_extract_date,
        run_id=run_id,
        summary=vancouver_summary,
        input_paths={
            "parcel_assessment_context": paths[
                "vancouver_assessment"
            ].as_posix(),
            "parcel_flood_exposure": paths[
                "vancouver_flood"
            ].as_posix(),
            "gold_grid_cell": paths["grid"].as_posix(),
        },
    )

    calgary_metadata = _write_output(
        dataframe=calgary,
        table_name=CALGARY_TABLE,
        gold_root=gold_root,
        extract_date=final_extract_date,
        run_id=run_id,
        summary=calgary_summary,
        input_paths={
            "property_location_assessment": paths[
                "calgary_assessment"
            ].as_posix(),
            "property_location_flood_exposure": paths[
                "calgary_flood"
            ].as_posix(),
            "gold_grid_cell": paths["grid"].as_posix(),
        },
    )

    print(
        "[OK] wrote city property risk context | "
        f"vancouver={len(vancouver):,} "
        f"calgary={len(calgary):,} "
        f"run_id={run_id}"
    )

    return {
        "run_id": run_id,
        "extract_date": final_extract_date,
        "load_status": "success",
        "tables": {
            VANCOUVER_TABLE: vancouver_metadata,
            CALGARY_TABLE: calgary_metadata,
        },
    }


def _write_output(
    *,
    dataframe: pd.DataFrame,
    table_name: str,
    gold_root: str | Path,
    extract_date: str,
    run_id: str,
    summary: dict[str, Any],
    input_paths: dict[str, str],
) -> dict[str, Any]:
    output_dir = (
        Path(gold_root)
        / table_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / f"{table_name}.parquet"

    dataframe.to_parquet(
        output_path,
        index=False,
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / table_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_path = metadata_dir / "metadata.json"

    metadata = {
        "table_name": table_name,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        **summary,
        "output_path": output_path.as_posix(),
        "input_paths": input_paths,
    }

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }


def main() -> None:
    args = parse_args()

    result = run_city_property_risk_context(
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