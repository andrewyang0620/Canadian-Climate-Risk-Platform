from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.ingestion.source_registry import SourceDefinition
from src.utils.checksum import compute_file_checksum
from src.utils.time import current_extract_date, generate_run_id, utc_now_iso


class BronzeWriterError(Exception):
    """Raised when Bronze writing fails."""


@dataclass(frozen=True)
class BronzeRunPaths:
    """Paths for one Bronze ingestion run."""

    run_id: str
    source_name: str
    extract_date: str
    run_dir: Path
    raw_dir: Path
    metadata_path: Path


@dataclass(frozen=True)
class BronzeWriteResult:
    """Result returned after writing a Bronze raw snapshot."""

    run_id: str
    source_name: str
    raw_file_path: Path
    metadata_path: Path
    file_checksum: str
    load_status: str


class BronzeWriter:
    """Write raw source snapshots and metadata into the Bronze lakehouse layer."""

    def __init__(self, bronze_base_path: str | Path = "lakehouse/bronze") -> None:
        self.bronze_base_path = Path(bronze_base_path)

    def build_run_paths(
        self,
        source_name: str,
        run_id: str | None = None,
        extract_date: str | None = None,
    ) -> BronzeRunPaths:
        """Build deterministic Bronze paths for one source run."""
        safe_source_name = _safe_path_part(source_name)
        final_run_id = run_id or generate_run_id()
        final_extract_date = extract_date or current_extract_date()

        run_dir = (
            self.bronze_base_path
            / safe_source_name
            / f"extract_date={final_extract_date}"
            / f"run_id={final_run_id}"
        )

        raw_dir = run_dir / "raw"
        metadata_path = run_dir / "metadata.json"

        return BronzeRunPaths(
            run_id=final_run_id,
            source_name=safe_source_name,
            extract_date=final_extract_date,
            run_dir=run_dir,
            raw_dir=raw_dir,
            metadata_path=metadata_path,
        )

    def write_bytes(
        self,
        source: SourceDefinition,
        filename: str,
        content: bytes,
        *,
        run_id: str | None = None,
        extract_timestamp: str | None = None,
        source_period_start: str | None = None,
        source_period_end: str | None = None,
        row_count: int | None = None,
        schema_hash: str | None = None,
        ingestion_method: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> BronzeWriteResult:
        """Write bytes to a Bronze raw snapshot and create metadata.json."""
        if not content:
            raise BronzeWriterError("Cannot write empty Bronze content.")

        paths = self.build_run_paths(source.name, run_id=run_id)
        paths.raw_dir.mkdir(parents=True, exist_ok=True)

        safe_filename = _safe_filename(filename)
        raw_file_path = paths.raw_dir / safe_filename
        raw_file_path.write_bytes(content)

        file_checksum = compute_file_checksum(raw_file_path)
        timestamp = extract_timestamp or utc_now_iso()

        metadata = {
            "run_id": paths.run_id,
            "source_name": source.name,
            "display_name": source.display_name,
            "source_group": source.source_group,
            "provider": source.provider,
            "source_url": source.source_url,
            "extract_timestamp": timestamp,
            "extract_date": paths.extract_date,
            "raw_file_path": _path_as_posix(raw_file_path),
            "file_name": safe_filename,
            "file_size_bytes": raw_file_path.stat().st_size,
            "file_checksum": file_checksum,
            "checksum_algorithm": "sha256",
            "schema_hash": schema_hash,
            "ingestion_method": ingestion_method or source.access_method,
            "source_period_start": source_period_start,
            "source_period_end": source_period_end,
            "row_count": row_count,
            "target_bronze_table": source.target_bronze_table,
            "target_silver_table": source.target_silver_table,
            "load_status": "success",
        }

        if extra_metadata:
            metadata["extra_metadata"] = extra_metadata

        paths.metadata_path.write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        return BronzeWriteResult(
            run_id=paths.run_id,
            source_name=source.name,
            raw_file_path=raw_file_path,
            metadata_path=paths.metadata_path,
            file_checksum=file_checksum,
            load_status="success",
        )

    def write_text(
        self,
        source: SourceDefinition,
        filename: str,
        content: str,
        **kwargs: Any,
    ) -> BronzeWriteResult:
        """Write text content to Bronze."""
        return self.write_bytes(
            source=source,
            filename=filename,
            content=content.encode("utf-8"),
            **kwargs,
        )


def _safe_path_part(value: str) -> str:
    """Validate a path component used in the Bronze folder structure."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise BronzeWriterError(f"Unsafe path component: {value}")

    if value in {".", ".."}:
        raise BronzeWriterError(f"Unsafe path component: {value}")

    return value


def _safe_filename(filename: str) -> str:
    """Validate file name to prevent path traversal."""
    if Path(filename).name != filename:
        raise BronzeWriterError(f"Filename must not include directories: {filename}")

    return _safe_path_part(filename)


def _path_as_posix(path: Path) -> str:
    """Convert a path to a stable POSIX-style string for metadata."""
    return path.as_posix()
