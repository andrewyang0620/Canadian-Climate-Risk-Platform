from __future__ import annotations

import argparse
import json

from src.silver.validation import (
    validate_eccc_hydro_realtime_observation_silver_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Validate ECCC hydrometric realtime Silver observations.")
    )

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
    )

    parser.add_argument(
        "--max-freshness-hours",
        type=float,
        default=24.0,
    )

    parser.add_argument(
        "--output-json",
        default=(
            "lakehouse/silver/_validation/" "eccc_hydro_realtime_observation/latest_validation.json"
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_eccc_hydro_realtime_observation_silver_outputs(
        silver_root=args.silver_root,
        max_freshness_hours=args.max_freshness_hours,
        output_json_path=args.output_json,
    )

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("Silver ECCC hydro realtime observation validation failed.")

    print(
        "[OK] Silver ECCC hydro realtime observation validation passed | "
        f"checks={len(report.checks)}"
    )


if __name__ == "__main__":
    main()
