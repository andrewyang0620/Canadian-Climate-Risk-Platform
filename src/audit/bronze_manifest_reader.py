from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from src.audit.audit_models import BronzeManifestError, BronzeRunRecord


class BronzeManifestReader:
    """Read and query Bronze ingestion manifest records."""

    def __init__(
        self,
        manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    ) -> None:
        self.manifest_path = Path(manifest_path)

    def read_records(self) -> list[BronzeRunRecord]:
        """Read all records from a Bronze JSONL manifest."""
        if not self.manifest_path.exists():
            raise BronzeManifestError(f"Bronze manifest not found: {self.manifest_path}")

        records: list[BronzeRunRecord] = []

        with self.manifest_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                stripped = line.strip()

                if not stripped:
                    continue

                try:
                    payload = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise BronzeManifestError(
                        f"Invalid JSON in Bronze manifest at line {line_number}: "
                        f"{self.manifest_path}"
                    ) from exc

                if not isinstance(payload, dict):
                    raise BronzeManifestError(
                        f"Bronze manifest line {line_number} must be a JSON object."
                    )

                try:
                    records.append(BronzeRunRecord.from_dict(payload))
                except BronzeManifestError as exc:
                    raise BronzeManifestError(
                        f"Invalid Bronze manifest record at line {line_number}: {exc}"
                    ) from exc

        return records

    def successful_records(self) -> list[BronzeRunRecord]:
        """Return all successful Bronze runs."""
        return [record for record in self.read_records() if record.is_successful]

    def records_by_source(self) -> dict[str, list[BronzeRunRecord]]:
        """Group all records by source name."""
        grouped: dict[str, list[BronzeRunRecord]] = defaultdict(list)

        for record in self.read_records():
            grouped[record.source_name].append(record)

        return dict(grouped)

    def latest_successful_by_source(self) -> dict[str, BronzeRunRecord]:
        """Return the latest successful run for each source.

        Latest is determined by extract_timestamp, then manifest_record_created_at,
        both represented as ISO timestamp strings.
        """
        latest: dict[str, BronzeRunRecord] = {}

        for record in self.successful_records():
            current = latest.get(record.source_name)

            if current is None or _sort_key(record) > _sort_key(current):
                latest[record.source_name] = record

        return latest

    def latest_successful_for_source(self, source_name: str) -> BronzeRunRecord | None:
        """Return the latest successful run for one source."""
        return self.latest_successful_by_source().get(source_name)

    def source_names_with_successful_runs(self) -> set[str]:
        """Return source names that have at least one successful Bronze run."""
        return set(self.latest_successful_by_source())

    def source_names_with_any_runs(self) -> set[str]:
        """Return source names that have at least one manifest record."""
        return {record.source_name for record in self.read_records()}


def _sort_key(record: BronzeRunRecord) -> tuple[str, str]:
    return (record.extract_timestamp, record.manifest_record_created_at)
