from __future__ import annotations

import argparse
import json
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from src.gold.disaster.cd_spatial_reference import (
    build_gold_disaster_cd_spatial_reference,
)


GOLD_ROOT = Path("lakehouse/gold")
TABLE_NAME = "gold_disaster_cd_spatial_reference"
DEFAULT_SOURCE_DIR = Path(
    "lakehouse/bronze/census_boundaries/census_division_boundary_2021"
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Gold disaster Census Division spatial reference."
    )
    parser.add_argument(
        "--source-path",
        type=Path,
        default=None,
        help="Path to StatCan 2021 Census Division boundary file zip/shapefile.",
    )

    args = parser.parse_args()

    source_path = args.source_path or _latest_boundary_file(DEFAULT_SOURCE_DIR)

    extract_date = date.today().isoformat()
    run_id = str(uuid.uuid4())

    reference, summary = build_gold_disaster_cd_spatial_reference(
        source_path=source_path,
    )

    output_dir = (
        GOLD_ROOT
        / TABLE_NAME
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}.parquet"
    reference.to_parquet(output_path, index=False)

    metadata = {
        **summary,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "output_path": output_path.as_posix(),
        "input_paths": {
            "statcan_census_division_boundary": str(source_path),
        },
    }

    metadata_path = (
        GOLD_ROOT
        / "_metadata"
        / TABLE_NAME
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "metadata.json"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(_json_safe(metadata), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(
        "[OK] wrote Gold disaster CD spatial reference | "
        f"rows={summary['row_count']} "
        f"provinces={summary['province_counts']} "
        f"run_id={run_id}"
    )
    print(json.dumps(_json_safe(metadata), indent=2, ensure_ascii=False))


def _latest_boundary_file(source_dir: Path) -> Path:
    if not source_dir.exists():
        raise FileNotFoundError(
            f"Missing source directory: {source_dir}. "
            "Download the StatCan 2021 Census Division boundary file first."
        )

    candidates: list[Path] = []

    for pattern in ["*.zip", "*.shp"]:
        candidates.extend(source_dir.rglob(pattern))

    if not candidates:
        raise FileNotFoundError(
            f"No boundary files found in {source_dir}. "
            "Expected .zip or .shp."
        )

    return max(candidates, key=lambda path: path.stat().st_mtime)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_json_safe(item) for item in value]

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


if __name__ == "__main__":
    main()
