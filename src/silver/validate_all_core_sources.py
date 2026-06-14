from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path
from typing import Any, Callable

from src.silver.common import utc_now_iso, write_json
from src.silver.validation import (
    validate_canadian_disaster_database_silver_outputs,
    validate_census_boundary_silver_outputs,
    validate_eccc_climate_daily_silver_outputs,
    validate_hydat_archive_silver_outputs,
    validate_wildfire_history_silver_outputs,
    validate_statcan_building_permit_month_silver_outputs,
)


ValidationFunction = Callable[..., Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate all core Silver outputs.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/all_core_sources/latest_validation_summary.json",
        help="Combined validation summary JSON path.",
    )

    return parser.parse_args()


def run_core_silver_validations(
    *,
    silver_root: str | Path = "lakehouse/silver",
    validation_root: str | Path = "lakehouse/silver/_validation",
) -> dict[str, Any]:
    silver_root = Path(silver_root)
    validation_root = Path(validation_root)

    validation_specs = [
        {
            "source_name": "census_boundaries",
            "function": validate_census_boundary_silver_outputs,
            "output_json_path": validation_root / "census_boundaries" / "latest_validation.json",
            "kwargs": {},
        },
        {
            "source_name": "eccc_historical_climate",
            "function": validate_eccc_climate_daily_silver_outputs,
            "output_json_path": validation_root / "eccc_climate_daily" / "latest_validation.json",
            "kwargs": {},
        },
        {
            "source_name": "wildfire_history",
            "function": validate_wildfire_history_silver_outputs,
            "output_json_path": validation_root / "wildfire_history" / "latest_validation.json",
            "kwargs": {},
        },
        {
            "source_name": "hydat_archive",
            "function": validate_hydat_archive_silver_outputs,
            "output_json_path": validation_root / "hydat_archive" / "latest_validation.json",
            "kwargs": {},
        },
        {
            "source_name": "canadian_disaster_database",
            "function": validate_canadian_disaster_database_silver_outputs,
            "output_json_path": validation_root
            / "canadian_disaster_database"
            / "latest_validation.json",
            "kwargs": {},
        },
        {
            "source_name": "statcan_building_permits",
            "function": validate_statcan_building_permit_month_silver_outputs,
            "output_json_path": validation_root
            / "statcan_building_permit_month"
            / "latest_validation.json",
            "kwargs": {},
        },
    ]

    results = []

    for spec in validation_specs:
        result = run_single_validation(
            source_name=spec["source_name"],
            validation_function=spec["function"],
            silver_root=silver_root,
            output_json_path=spec["output_json_path"],
            kwargs=spec["kwargs"],
        )
        results.append(result)

        status = "PASSED" if result["passed"] else "FAILED"
        print(
            f"[{status}] {result['source_name']} | "
            f"checks={result['passed_check_count']}/{result['check_count']}"
        )

    return build_validation_summary(
        results=results,
        generated_at=utc_now_iso(),
    )


def run_single_validation(
    *,
    source_name: str,
    validation_function: ValidationFunction,
    silver_root: Path,
    output_json_path: Path,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    try:
        report = validation_function(
            silver_root=silver_root,
            output_json_path=output_json_path,
            **kwargs,
        )
        report_payload = report.to_dict()

        checks = report_payload.get("checks", [])
        failed_checks = [check["name"] for check in checks if check.get("passed") is not True]

        return {
            "source_name": source_name,
            "passed": bool(report_payload.get("passed")),
            "check_count": len(checks),
            "passed_check_count": len(checks) - len(failed_checks),
            "failed_check_count": len(failed_checks),
            "failed_checks": failed_checks,
            "output_paths": report_payload.get("output_paths", {}),
            "validation_report_path": output_json_path.as_posix(),
            "error": None,
        }

    except Exception as exc:
        return {
            "source_name": source_name,
            "passed": False,
            "check_count": 0,
            "passed_check_count": 0,
            "failed_check_count": 1,
            "failed_checks": ["validation_exception"],
            "output_paths": {},
            "validation_report_path": output_json_path.as_posix(),
            "error": {
                "type": exc.__class__.__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }


def build_validation_summary(
    *,
    results: list[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    passed_source_count = sum(1 for result in results if result["passed"])
    failed_source_count = len(results) - passed_source_count

    return {
        "validation_name": "core_silver_validation_summary",
        "generated_at": generated_at,
        "passed": failed_source_count == 0,
        "source_count": len(results),
        "passed_source_count": passed_source_count,
        "failed_source_count": failed_source_count,
        "total_check_count": sum(result["check_count"] for result in results),
        "total_failed_check_count": sum(result["failed_check_count"] for result in results),
        "results": results,
    }


def main() -> None:
    args = parse_args()

    output_json_path = Path(args.output_json)
    validation_root = output_json_path.parent.parent

    summary = run_core_silver_validations(
        silver_root=args.silver_root,
        validation_root=validation_root,
    )

    write_json(output_json_path, summary)

    print(json.dumps(summary, indent=2))

    if not summary["passed"]:
        raise SystemExit("Core Silver validation failed.")

    print(
        "[OK] Core Silver validation passed | "
        f"sources={summary['passed_source_count']}/{summary['source_count']} "
        f"checks={summary['total_check_count']}"
    )


if __name__ == "__main__":
    main()
