from __future__ import annotations

import argparse
import json

from src.silver.validation import validate_hydat_archive_silver_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Silver HYDAT archive outputs.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--expected-start-year",
        type=int,
        default=1901,
        help="Expected first observation year.",
    )

    parser.add_argument(
        "--expected-end-year",
        type=int,
        default=2026,
        help="Expected final observation year.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/hydat_archive/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_hydat_archive_silver_outputs(
        silver_root=args.silver_root,
        expected_start_year=args.expected_start_year,
        expected_end_year=args.expected_end_year,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver HYDAT archive validation failed.")

    print("[OK] Silver HYDAT archive validation passed | " f"checks={len(report.checks)}")


if __name__ == "__main__":
    main()
