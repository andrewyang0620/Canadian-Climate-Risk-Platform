from __future__ import annotations

import argparse
from pathlib import Path

from src.silver.wildfire_perimeter_polygon import (
    run_wildfire_perimeter_polygon_silver,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Silver standardization for NFDB wildfire perimeter polygons."
    )

    parser.add_argument(
        "--bronze-manifest-path",
        default="lakehouse/bronze/_manifests/bronze_runs.jsonl",
        help="Path to Bronze manifest JSONL.",
    )

    parser.add_argument(
        "--output-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--silver-manifest-path",
        default="lakehouse/silver/_manifests/silver_runs.jsonl",
        help="Path to Silver manifest JSONL.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_wildfire_perimeter_polygon_silver(
        bronze_manifest_path=Path(args.bronze_manifest_path),
        output_root=Path(args.output_root),
        silver_manifest_path=Path(args.silver_manifest_path),
    )


if __name__ == "__main__":
    main()
