from __future__ import annotations

import argparse
import json

from src.silver.validation import validate_eccc_climate_daily_silver_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Silver ECCC daily climate outputs.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--expected-start-year",
        type=int,
        default=2016,
        help="Expected first observation year.",
    )

    parser.add_argument(
        "--expected-end-year",
        type=int,
        default=2025,
        help="Expected final observation year.",
    )

    parser.add_argument(
        "--min-measurement-presence-rate",
        type=float,
        default=0.95,
        help="Minimum acceptable share of rows with at least one climate measurement.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/eccc_climate_daily/latest_validation.json",
        help="Validation report JSON path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_eccc_climate_daily_silver_outputs(
        silver_root=args.silver_root,
        expected_years=list(range(args.expected_start_year, args.expected_end_year + 1)),
        min_measurement_presence_rate=args.min_measurement_presence_rate,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver ECCC climate daily validation failed.")

    print("[OK] Silver ECCC climate daily validation passed | " f"checks={len(report.checks)}")


if __name__ == "__main__":
    main()
