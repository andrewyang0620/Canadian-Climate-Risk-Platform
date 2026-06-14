from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

from src.silver.validation import (
    SilverValidationReport,
    validate_municipal_building_permit_silver_outputs,
    validate_municipal_development_permit_silver_outputs,
    validate_municipal_flood_hazard_silver_outputs,
    validate_municipal_property_assessment_silver_outputs,
    validate_municipal_property_parcel_silver_outputs,
    validate_municipal_property_tax_assessment_silver_outputs,
)


Validator = Callable[..., SilverValidationReport]


MUNICIPAL_VALIDATORS: list[tuple[str, Validator]] = [
    ("municipal_flood_hazard", validate_municipal_flood_hazard_silver_outputs),
    ("municipal_property_assessment", validate_municipal_property_assessment_silver_outputs),
    ("municipal_building_permit", validate_municipal_building_permit_silver_outputs),
    ("municipal_property_parcel", validate_municipal_property_parcel_silver_outputs),
    (
        "municipal_property_tax_assessment",
        validate_municipal_property_tax_assessment_silver_outputs,
    ),
    ("municipal_development_permit", validate_municipal_development_permit_silver_outputs),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate all municipal Silver source outputs.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/silver/_validation/municipal_sources/latest_validation.json",
        help="Combined municipal validation report JSON path.",
    )

    return parser.parse_args()


def build_all_municipal_validation_report(
    *,
    silver_root: str | Path = "lakehouse/silver",
) -> dict[str, Any]:
    report_dicts: list[dict[str, Any]] = []

    for validator_name, validator in MUNICIPAL_VALIDATORS:
        report = validator(silver_root=silver_root)
        report_dict = report.to_dict()
        report_dict["validator_name"] = validator_name
        report_dicts.append(report_dict)

    summary = summarize_validation_report_dicts(report_dicts)

    return {
        "validation_name": "municipal_silver_sources_validation",
        "passed": summary["failed_report_count"] == 0,
        "summary": summary,
        "reports": report_dicts,
    }


def summarize_validation_report_dicts(
    report_dicts: list[dict[str, Any]],
) -> dict[str, Any]:
    failed_reports = [report for report in report_dicts if not bool(report.get("passed", False))]

    failed_checks = []

    for report in report_dicts:
        validation_name = str(report.get("validation_name"))
        validator_name = str(report.get("validator_name", validation_name))

        for check in report.get("checks", []):
            if not bool(check.get("passed", False)):
                failed_checks.append(
                    {
                        "validator_name": validator_name,
                        "validation_name": validation_name,
                        "check_name": check.get("name"),
                        "details": check.get("details", {}),
                    }
                )

    check_count = sum(len(report.get("checks", [])) for report in report_dicts)
    passed_check_count = sum(
        1
        for report in report_dicts
        for check in report.get("checks", [])
        if bool(check.get("passed", False))
    )

    return {
        "report_count": len(report_dicts),
        "passed_report_count": len(report_dicts) - len(failed_reports),
        "failed_report_count": len(failed_reports),
        "check_count": check_count,
        "passed_check_count": passed_check_count,
        "failed_check_count": len(failed_checks),
        "failed_validations": [report.get("validation_name") for report in failed_reports],
        "failed_checks": failed_checks,
    }


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()

    combined_report = build_all_municipal_validation_report(
        silver_root=args.silver_root,
    )

    write_json(args.output_json, combined_report)

    print(json.dumps(combined_report, indent=2))

    summary = combined_report["summary"]

    if not combined_report["passed"]:
        raise SystemExit(
            "Municipal Silver validation failed | "
            f"failed_reports={summary['failed_report_count']} "
            f"failed_checks={summary['failed_check_count']}"
        )

    print(
        "[OK] Municipal Silver validation passed | "
        f"reports={summary['report_count']} "
        f"checks={summary['check_count']}"
    )


if __name__ == "__main__":
    main()
