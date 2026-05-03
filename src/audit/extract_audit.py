from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.audit.audit_models import BronzeManifestError, BronzeRunRecord
from src.audit.bronze_manifest_reader import BronzeManifestReader
from src.utils.config import load_project_config
from src.utils.time import utc_now_iso


class ExtractAuditError(Exception):
    """Raised when extract audit cannot be completed."""


@dataclass(frozen=True)
class SourceExtractAuditResult:
    """Audit result for one configured source."""

    source_name: str
    source_group: str
    status: str  # "pass" / "warn" / "fail" / "missing"
    checks: dict[
        str, bool | None
    ]  # {"has_successful_bronze_run": True, "file_size_positive": True, ...}
    messages: list[str]
    latest_run: dict[str, Any] | None
    metadata: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "source_group": self.source_group,
            "status": self.status,
            "checks": self.checks,
            "messages": self.messages,
            "latest_run": self.latest_run,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class ExtractAuditReport:
    """Audit report for Bronze extract runs."""

    audited_at: str
    manifest_path: str
    source_config_path: str
    source_groups: list[str] | None
    overall_status: str  # "pass" / "warn" / "fail"
    configured_source_count: int
    passed_source_count: int
    warning_source_count: int
    failed_source_count: int
    missing_source_count: int
    results: list[SourceExtractAuditResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "audited_at": self.audited_at,
            "manifest_path": self.manifest_path,
            "source_config_path": self.source_config_path,
            "source_groups": self.source_groups,
            "overall_status": self.overall_status,
            "configured_source_count": self.configured_source_count,
            "passed_source_count": self.passed_source_count,
            "warning_source_count": self.warning_source_count,
            "failed_source_count": self.failed_source_count,
            "missing_source_count": self.missing_source_count,
            "results": [result.to_dict() for result in self.results],
        }


class ExtractAuditor:
    """Audit Bronze extract completeness and metadata quality.
    Workflows:
    - 从`yml`取出每个数据源的成功记录
    - 从`manifest`取出每个数据源最新的成功记录
    - 对每个数据源调用 _audit_one_source
    - 统计 pass/warn/fail/missing 数量，生成 ExtractAuditReport
    - 决定overall_status (有fail或missing则fail,否则有warn则warn,否则pass)
    """

    def __init__(
        self,
        *,
        manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
        source_config_path: str = "source_config.yml",
        source_config: dict[str, Any] | None = None,
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.source_config_path = source_config_path
        self.source_config = source_config or load_project_config(source_config_path)

    def run(
        self,
        *,
        source_groups: list[str] | None = None,
    ) -> ExtractAuditReport:
        """Run extract audit for configured sources."""
        sources = self._configured_sources(source_groups)

        try:
            latest_by_source = BronzeManifestReader(
                self.manifest_path
            ).latest_successful_by_source()
        except BronzeManifestError as exc:
            raise ExtractAuditError(f"Cannot read Bronze manifest: {exc}") from exc

        results = [
            self._audit_one_source(
                source_name=source_name,
                source_metadata=source_metadata,
                latest_run=latest_by_source.get(source_name),
            )
            for source_name, source_metadata in sources.items()
        ]

        passed = sum(result.status == "pass" for result in results)
        warnings = sum(result.status == "warn" for result in results)
        failed = sum(result.status == "fail" for result in results)
        missing = sum(result.status == "missing" for result in results)

        if failed or missing:
            overall_status = "fail"
        elif warnings:
            overall_status = "warn"
        else:
            overall_status = "pass"

        return ExtractAuditReport(
            audited_at=utc_now_iso(),
            manifest_path=self.manifest_path.as_posix(),
            source_config_path=self.source_config_path,
            source_groups=source_groups,
            overall_status=overall_status,
            configured_source_count=len(results),
            passed_source_count=passed,
            warning_source_count=warnings,
            failed_source_count=failed,
            missing_source_count=missing,
            results=results,
        )

    def write_report(
        self,
        *,
        output_path: str | Path = "lakehouse/audit/extract_audit.json",
        source_groups: list[str] | None = None,
    ) -> ExtractAuditReport:
        """Run extract audit and write JSON report."""
        report = self.run(source_groups=source_groups)

        final_output_path = Path(output_path)
        final_output_path.parent.mkdir(parents=True, exist_ok=True)
        final_output_path.write_text(
            json.dumps(report.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        return report

    def _configured_sources(
        self,
        source_groups: list[str] | None,
    ) -> dict[str, dict[str, Any]]:
        sources = self.source_config["sources"]

        if not source_groups:
            return sources

        allowed = set(source_groups)
        return {
            source_name: metadata
            for source_name, metadata in sources.items()
            if metadata["source_group"] in allowed
        }

    def _audit_one_source(
        self,
        *,
        source_name: str,
        source_metadata: dict[str, Any],
        latest_run: BronzeRunRecord | None,
    ) -> SourceExtractAuditResult:
        if latest_run is None:
            return SourceExtractAuditResult(
                source_name=source_name,
                source_group=source_metadata["source_group"],
                status="missing",
                checks={
                    "has_successful_bronze_run": False,
                    "load_status_success": None,
                    "file_size_positive": None,
                    "checksum_present": None,
                    "raw_file_exists": None,
                    "metadata_file_exists": None,
                    "table_contract_matches_config": None,
                    "row_count_validation_passed_if_available": None,
                },
                messages=["No successful Bronze run found for configured source."],
                latest_run=None,
                metadata=None,
            )

        metadata_payload = _read_json_if_exists(latest_run.metadata_file_path)

        checks: dict[str, bool | None] = {
            "has_successful_bronze_run": True,
            "load_status_success": latest_run.load_status == "success",
            "file_size_positive": latest_run.file_size_bytes > 0,
            "checksum_present": bool(latest_run.file_checksum)
            and latest_run.checksum_algorithm == "sha256",
            "raw_file_exists": latest_run.raw_path.exists(),
            "metadata_file_exists": latest_run.metadata_file_path.exists(),
            "table_contract_matches_config": self._table_contract_matches(
                source_metadata=source_metadata,
                latest_run=latest_run,
            ),
            "row_count_validation_passed_if_available": (
                _row_count_validation_passed_if_available(metadata_payload)
            ),
        }

        messages = _messages_from_checks(checks)

        metadata_summary = _metadata_summary(metadata_payload)
        latest_run_summary = _latest_run_summary(latest_run)

        if False in checks.values():
            status = "fail"
        elif None in checks.values():
            status = "warn"
        else:
            status = "pass"

        return SourceExtractAuditResult(
            source_name=source_name,
            source_group=source_metadata["source_group"],
            status=status,
            checks=checks,
            messages=messages,
            latest_run=latest_run_summary,
            metadata=metadata_summary,
        )

    @staticmethod
    def _table_contract_matches(
        *,
        source_metadata: dict[str, Any],
        latest_run: BronzeRunRecord,
    ) -> bool:
        return (
            latest_run.target_bronze_table == source_metadata["target_bronze_table"]
            and latest_run.target_silver_table == source_metadata["target_silver_table"]
        )


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None

    payload = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(payload, dict):
        raise ExtractAuditError(f"Metadata file must contain JSON object: {path}")

    return payload


def _row_count_validation_passed_if_available(
    metadata_payload: dict[str, Any] | None,
) -> bool | None:
    if metadata_payload is None:
        return None

    extra_metadata = metadata_payload.get("extra_metadata", {})
    if not isinstance(extra_metadata, dict):
        return None

    supported = extra_metadata.get("row_count_validation_supported")
    passed = extra_metadata.get("socrata_row_count_validation_passed")

    if supported is True:
        return passed is True

    if supported is False:
        return None

    return None


def _metadata_summary(metadata_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if metadata_payload is None:
        return None

    extra_metadata = metadata_payload.get("extra_metadata", {})
    if not isinstance(extra_metadata, dict):
        extra_metadata = {}

    return {
        "run_id": metadata_payload.get("run_id"),
        "source_name": metadata_payload.get("source_name"),
        "file_size_bytes": metadata_payload.get("file_size_bytes"),
        "file_checksum": metadata_payload.get("file_checksum"),
        "load_status": metadata_payload.get("load_status"),
        "row_count": metadata_payload.get("row_count"),
        "ingestion_method": metadata_payload.get("ingestion_method"),
        "row_count_validation_supported": extra_metadata.get("row_count_validation_supported"),
        "socrata_expected_row_count": extra_metadata.get("socrata_expected_row_count"),
        "socrata_actual_row_count": extra_metadata.get("socrata_actual_row_count"),
        "socrata_row_count_validation_passed": extra_metadata.get(
            "socrata_row_count_validation_passed"
        ),
    }


def _latest_run_summary(record: BronzeRunRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "source_name": record.source_name,
        "extract_timestamp": record.extract_timestamp,
        "extract_date": record.extract_date,
        "raw_file_path": record.raw_file_path,
        "metadata_path": record.metadata_path,
        "file_name": record.file_name,
        "file_size_bytes": record.file_size_bytes,
        "row_count": record.row_count,
        "target_bronze_table": record.target_bronze_table,
        "target_silver_table": record.target_silver_table,
        "load_status": record.load_status,
    }


def _messages_from_checks(checks: dict[str, bool | None]) -> list[str]:
    messages: list[str] = []

    for check_name, value in checks.items():
        if value is False:
            messages.append(f"FAILED: {check_name}")
        elif value is None:
            messages.append(f"WARN: {check_name} not available or not applicable")

    return messages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze extract audit.")

    parser.add_argument(
        "--manifest-path",
        default="lakehouse/bronze/_manifests/bronze_runs.jsonl",
        help="Path to Bronze run manifest JSONL.",
    )

    parser.add_argument(
        "--source-config-path",
        default="source_config.yml",
        help="Config file name under configs/.",
    )

    parser.add_argument(
        "--output",
        default="lakehouse/audit/extract_audit.json",
        help="Output audit report path.",
    )

    parser.add_argument(
        "--source-group",
        action="append",
        default=None,
        help="Optional source group filter. Can be repeated.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    auditor = ExtractAuditor(
        manifest_path=args.manifest_path,
        source_config_path=args.source_config_path,
    )

    report = auditor.write_report(
        output_path=args.output,
        source_groups=args.source_group,
    )

    print(
        f"[OK] wrote extract audit -> {args.output} | "
        f"overall_status={report.overall_status} | "
        f"pass={report.passed_source_count} | "
        f"warn={report.warning_source_count} | "
        f"fail={report.failed_source_count} | "
        f"missing={report.missing_source_count}"
    )


if __name__ == "__main__":
    main()
