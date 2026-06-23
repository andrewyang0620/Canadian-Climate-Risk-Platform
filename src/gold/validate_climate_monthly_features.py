from __future__ import annotations

import argparse
import json

from src.gold.climate_validation import (
    validate_climate_monthly_feature_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Gold monthly climate feature outputs.")
    parser.add_argument("--gold-root", default="lakehouse/gold")
    parser.add_argument(
        "--output-json",
        default=("lakehouse/gold/_validation/" "climate_monthly_features/latest_validation.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = validate_climate_monthly_feature_outputs(
        gold_root=args.gold_root,
        output_json_path=args.output_json,
    )

    payload = report.to_dict()
    print(json.dumps(payload, indent=2))

    if not report.passed:
        raise SystemExit("Gold monthly climate feature validation failed.")

    print("[OK] Gold monthly climate feature validation " f"passed | checks={len(report.checks)}")


if __name__ == "__main__":
    main()
