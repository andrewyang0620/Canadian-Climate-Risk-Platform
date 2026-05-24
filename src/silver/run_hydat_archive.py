from __future__ import annotations

import argparse

from src.silver.hydat_archive import run_hydat_archive_silver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver standardization for HYDAT archive.")

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
        help="Silver manifest JSONL path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_hydat_archive_silver(
        bronze_manifest_path=args.bronze_manifest_path,
        output_root=args.output_root,
        silver_manifest_path=args.silver_manifest_path,
    )


if __name__ == "__main__":
    main()
