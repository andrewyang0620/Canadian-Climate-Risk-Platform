from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


class AuditError(Exception):
    """Base exception for audit framework errors."""


class BronzeManifestError(AuditError):
    """Raised when Bronze manifest loading or parsing fails."""


@dataclass(frozen=True)
class BronzeRunRecord:
    """One record from the Bronze run manifest."""

    run_id: str
    source_name: str
    source_group: str
    provider: str
    extract_timestamp: str
    extract_date: str
    raw_file_path: str
    metadata_path: str
    file_name: str
    file_size_bytes: int
    file_checksum: str
    checksum_algorithm: str
    ingestion_method: str
    row_count: int | None
    target_bronze_table: str
    target_silver_table: str
    load_status: str
    manifest_record_created_at: str
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BronzeRunRecord":
        """Create a BronzeRunRecord from a manifest JSON object."""
        required_fields = [
            "run_id",
            "source_name",
            "source_group",
            "provider",
            "extract_timestamp",
            "extract_date",
            "raw_file_path",
            "metadata_path",
            "file_name",
            "file_size_bytes",
            "file_checksum",
            "checksum_algorithm",
            "ingestion_method",
            "target_bronze_table",
            "target_silver_table",
            "load_status",
            "manifest_record_created_at",
        ]

        missing = [field for field in required_fields if field not in payload]
        if missing:
            raise BronzeManifestError(f"Bronze manifest record missing required fields: {missing}")

        file_size = payload["file_size_bytes"]
        if not isinstance(file_size, int):
            raise BronzeManifestError(
                f"file_size_bytes must be int. Got {type(file_size).__name__}"
            )

        row_count = payload.get("row_count")
        if row_count is not None and not isinstance(row_count, int):
            raise BronzeManifestError(
                f"row_count must be int or null. Got {type(row_count).__name__}"
            )

        return cls(
            run_id=str(payload["run_id"]),
            source_name=str(payload["source_name"]),
            source_group=str(payload["source_group"]),
            provider=str(payload["provider"]),
            extract_timestamp=str(payload["extract_timestamp"]),
            extract_date=str(payload["extract_date"]),
            raw_file_path=str(payload["raw_file_path"]),
            metadata_path=str(payload["metadata_path"]),
            file_name=str(payload["file_name"]),
            file_size_bytes=file_size,
            file_checksum=str(payload["file_checksum"]),
            checksum_algorithm=str(payload["checksum_algorithm"]),
            ingestion_method=str(payload["ingestion_method"]),
            row_count=row_count,
            target_bronze_table=str(payload["target_bronze_table"]),
            target_silver_table=str(payload["target_silver_table"]),
            load_status=str(payload["load_status"]),
            manifest_record_created_at=str(payload["manifest_record_created_at"]),
            raw=payload,
        )

    @property
    def is_successful(self) -> bool:
        """Return whether this run completed successfully."""
        return self.load_status == "success"

    @property
    def raw_path(self) -> Path:
        """Return raw file path as Path."""
        return Path(self.raw_file_path)

    @property
    def metadata_file_path(self) -> Path:
        """Return metadata path as Path."""
        return Path(self.metadata_path)
