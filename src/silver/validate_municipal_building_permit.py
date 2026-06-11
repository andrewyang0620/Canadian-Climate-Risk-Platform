from __future__ import annotations

import argparse
import json

from src.silver.validation import validate_municipal_building_permit_silver_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Silver municipal building permit outputs."
    )

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/municipal_building_permit/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_municipal_building_permit_silver_outputs(
        silver_root=args.silver_root,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver municipal building permit validation failed.")

    print(
        "[OK] Silver municipal building permit validation passed | " f"checks={len(report.checks)}"
    )


if __name__ == "__main__":
    main()
