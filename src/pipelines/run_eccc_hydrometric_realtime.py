from __future__ import annotations

import argparse
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.ingestion.run_national_core_ingestion import (
    download_eccc_hydrometric_realtime,
)
from src.silver.run_eccc_hydro_realtime_observation import (
    run_eccc_hydro_realtime_observation_silver,
)
from src.silver.validation import (
    validate_eccc_hydro_realtime_observation_silver_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Run ECCC hydrometric realtime Bronze, Silver, " "and validation pipeline.")
    )

    parser.add_argument(
        "--bronze-root",
        default="lakehouse/bronze",
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
        "--pipeline-report-root",
        default="lakehouse/_pipeline_runs/eccc_hydrometric_realtime",
    )

    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_eccc_hydrometric_realtime_pipeline(
    *,
    bronze_root: str | Path = "lakehouse/bronze",
    silver_root: str | Path = "lakehouse/silver",
    max_freshness_hours: float = 24.0,
    pipeline_report_root: str | Path = ("lakehouse/_pipeline_runs/eccc_hydrometric_realtime"),
) -> dict[str, Any]:
    """Run the complete ECCC hydrometric realtime micro-batch pipeline."""
    pipeline_run_id = str(uuid4())
    started_at = utc_now_iso()

    report: dict[str, Any] = {
        "pipeline_name": "eccc_hydrometric_realtime_pipeline",
        "pipeline_run_id": pipeline_run_id,
        "started_at": started_at,
        "completed_at": None,
        "status": "running",
        "steps": [],
        "error": None,
    }

    report_path = Path(pipeline_report_root) / f"run_id={pipeline_run_id}" / "pipeline_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        bronze_result = download_eccc_hydrometric_realtime(
            output_root=bronze_root,
            manifest_path=(Path(bronze_root) / "_manifests" / "bronze_runs.jsonl"),
        )

        report["steps"].append(
            {
                "name": "bronze_ingestion",
                "status": "passed",
                "run_id": bronze_result.get("run_id"),
                "raw_file_path": bronze_result.get("raw_file_path"),
                "row_count": bronze_result.get("row_count"),
            }
        )

        silver_result = run_eccc_hydro_realtime_observation_silver(
            bronze_root=bronze_root,
            silver_root=silver_root,
            raw_path=bronze_result["raw_file_path"],
            extract_date=started_at[:10],
        )

        report["steps"].append(
            {
                "name": "silver_build",
                "status": "passed",
                "run_id": silver_result["run_id"],
                "output_path": silver_result["output_path"],
                "row_count": silver_result["row_count"],
                "station_count": silver_result["station_count"],
            }
        )

        validation_path = (
            Path(silver_root)
            / "_validation"
            / "eccc_hydro_realtime_observation"
            / "latest_validation.json"
        )

        validation_report = validate_eccc_hydro_realtime_observation_silver_outputs(
            silver_root=silver_root,
            max_freshness_hours=max_freshness_hours,
            output_json_path=validation_path,
        )

        validation_payload = validation_report.to_dict()

        report["steps"].append(
            {
                "name": "silver_validation",
                "status": ("passed" if validation_report.passed else "failed"),
                "passed": validation_report.passed,
                "check_count": len(validation_report.checks),
                "failed_checks": [
                    check["name"]
                    for check in validation_payload["checks"]
                    if check["passed"] is not True
                ],
                "validation_report_path": validation_path.as_posix(),
            }
        )

        if not validation_report.passed:
            raise RuntimeError("ECCC hydrometric realtime Silver validation failed.")

        report["status"] = "success"

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }

    report["completed_at"] = utc_now_iso()
    report["pipeline_report_path"] = report_path.as_posix()

    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    return report


def main() -> None:
    args = parse_args()

    report = run_eccc_hydrometric_realtime_pipeline(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        max_freshness_hours=args.max_freshness_hours,
        pipeline_report_root=args.pipeline_report_root,
    )

    print(json.dumps(report, indent=2))

    if report["status"] != "success":
        raise SystemExit("ECCC hydrometric realtime pipeline failed.")

    print(
        "[OK] ECCC hydrometric realtime pipeline completed | " f"run_id={report['pipeline_run_id']}"
    )


if __name__ == "__main__":
    main()
