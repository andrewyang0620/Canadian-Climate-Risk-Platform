from __future__ import annotations

import argparse
import json

from src.gold.validation import (
    validate_spatial_grid_foundation_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("Validate Gold spatial grid foundation outputs."))

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
    )
    parser.add_argument(
        "--gold-root",
        default="lakehouse/gold",
    )
    parser.add_argument(
        "--output-json",
        default=("lakehouse/gold/_validation/" "spatial_grid_foundation/" "latest_validation.json"),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_spatial_grid_foundation_outputs(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        output_json_path=args.output_json,
    )

    payload = report.to_dict()

    print(json.dumps(payload, indent=2))

    if not report.passed:
        raise SystemExit("Gold spatial grid foundation validation failed.")

    print("[OK] Gold spatial grid foundation validation " f"passed | checks={len(report.checks)}")


if __name__ == "__main__":
    main()
