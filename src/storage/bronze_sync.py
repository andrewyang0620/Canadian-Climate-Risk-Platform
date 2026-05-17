from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

from src.storage.backends import StorageBackend
from src.storage.bronze_layout import bronze_relative_path


@dataclass(frozen=True)
class BronzeSyncObject:
    """One local Bronze file selected for backend sync."""

    local_path: Path
    relative_path: str
    size_bytes: int
    content_type: str | None


@dataclass(frozen=True)
class BronzeSyncResult:
    """Result for one uploaded or dry-run Bronze object."""

    local_path: str
    relative_path: str
    target_uri: str | None
    size_bytes: int
    dry_run: bool


class BronzeSyncer:
    """Sync local Bronze files to a configured storage backend."""

    def __init__(
        self,
        *,
        bronze_root: str | Path = "lakehouse/bronze",
        storage_backend: StorageBackend,
    ) -> None:
        self.bronze_root = Path(bronze_root)
        self.storage_backend = storage_backend

    def plan(
        self,
        *,
        include_sources: list[str] | None = None,
        include_manifests: bool = True,
        latest_successful_only: bool = False,
        exclude_smoke_tests: bool = False,
    ) -> list[BronzeSyncObject]:
        """Build a deterministic list of local Bronze files to sync."""
        if not self.bronze_root.exists():
            raise FileNotFoundError(f"Bronze root does not exist: {self.bronze_root}")

        paths: list[Path] = []

        if latest_successful_only:
            run_dirs = self._latest_successful_run_dirs(
                include_sources=include_sources,
                exclude_smoke_tests=exclude_smoke_tests,
            )
            for run_dir in run_dirs:
                if run_dir.exists():
                    paths.extend(_iter_files(run_dir))
        else:
            if include_sources:
                for source_name in include_sources:
                    source_root = self.bronze_root / source_name

                    if not source_root.exists():
                        continue

                    paths.extend(_iter_files(source_root))
            else:
                for child in sorted(self.bronze_root.iterdir()):
                    if child.name == "_manifests":
                        continue
                    if child.is_dir():
                        paths.extend(_iter_files(child))

            if exclude_smoke_tests:
                paths = self._remove_smoke_test_paths(paths)

        if include_manifests:
            manifest_root = self.bronze_root / "_manifests"
            if manifest_root.exists():
                paths.extend(_iter_files(manifest_root))

        unique_paths = sorted(set(paths), key=lambda path: path.as_posix())

        return [
            BronzeSyncObject(
                local_path=path,
                relative_path=bronze_relative_path(path, bronze_root=self.bronze_root),
                size_bytes=path.stat().st_size,
                content_type=guess_content_type(path),
            )
            for path in unique_paths
        ]

    def sync(
        self,
        *,
        include_sources: list[str] | None = None,
        include_manifests: bool = True,
        dry_run: bool = False,
        latest_successful_only: bool = False,
        exclude_smoke_tests: bool = False,
    ) -> list[BronzeSyncResult]:
        """Upload planned Bronze files to the backend or return a dry-run plan."""
        objects = self.plan(
            include_sources=include_sources,
            include_manifests=include_manifests,
            latest_successful_only=latest_successful_only,
            exclude_smoke_tests=exclude_smoke_tests,
        )

        results: list[BronzeSyncResult] = []

        for obj in objects:
            target_uri = None

            if not dry_run:
                target_uri = self.storage_backend.upload_file(
                    obj.local_path,
                    obj.relative_path,
                    content_type=obj.content_type,
                )
            else:
                target_uri = self.storage_backend.uri(obj.relative_path)

            results.append(
                BronzeSyncResult(
                    local_path=obj.local_path.as_posix(),
                    relative_path=obj.relative_path,
                    target_uri=target_uri,
                    size_bytes=obj.size_bytes,
                    dry_run=dry_run,
                )
            )

        return results

    def _latest_successful_run_dirs(
        self,
        *,
        include_sources: list[str] | None,
        exclude_smoke_tests: bool,
    ) -> list[Path]:
        records = self._load_manifest_records()
        source_filter = set(include_sources or [])
        latest_by_source: dict[str, dict[str, Any]] = {}

        for record in records:
            source_name = record.get("source_name")
            if not source_name:
                continue

            if source_filter and source_name not in source_filter:
                continue

            if record.get("load_status") != "success":
                continue

            if exclude_smoke_tests and _is_smoke_test_record(record):
                continue

            raw_file_path = record.get("raw_file_path")
            if not raw_file_path:
                continue

            current = latest_by_source.get(source_name)
            if current is None or _record_sort_key(record) > _record_sort_key(current):
                latest_by_source[source_name] = record

        run_dirs = []

        for record in latest_by_source.values():
            raw_file_path = Path(record["raw_file_path"])
            # raw_file_path points to <run_dir>/raw/<file>; parent.parent is run_dir.
            run_dirs.append(raw_file_path.parent.parent)

        return sorted(set(run_dirs), key=lambda path: path.as_posix())

    def _remove_smoke_test_paths(self, paths: list[Path]) -> list[Path]:
        records = self._load_manifest_records()
        smoke_run_dirs = set()

        for record in records:
            if not _is_smoke_test_record(record):
                continue

            raw_file_path = record.get("raw_file_path")
            if not raw_file_path:
                continue

            smoke_run_dirs.add(Path(raw_file_path).parent.parent.resolve())

        return [path for path in paths if path.resolve().parent.parent not in smoke_run_dirs]

    def _load_manifest_records(self) -> list[dict[str, Any]]:
        manifest_path = self.bronze_root / "_manifests" / "bronze_runs.jsonl"

        if not manifest_path.exists():
            return []

        records: list[dict[str, Any]] = []

        with manifest_path.open("r", encoding="utf-8") as file:
            for line in file:
                stripped = line.strip()
                if not stripped:
                    continue
                records.append(json.loads(stripped))

        return records


def _is_smoke_test_record(record: dict[str, Any]) -> bool:
    extra_metadata = record.get("extra_metadata") or {}
    return bool(extra_metadata.get("smoke_test"))


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("extract_timestamp") or ""),
        str(record.get("run_id") or ""),
    )


def _iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def guess_content_type(path: str | Path) -> str | None:
    suffix = Path(path).suffix.lower()
    name = Path(path).name.lower()

    if suffix == ".json":
        return "application/json"

    if suffix == ".jsonl":
        return "application/x-ndjson"

    if name.endswith(".jsonl.gz"):
        return "application/gzip"

    if suffix == ".csv":
        return "text/csv"

    if suffix == ".txt":
        return "text/plain"

    if suffix == ".geojson":
        return "application/geo+json"

    if suffix == ".zip":
        return "application/zip"

    if suffix in {".parquet", ".pq"}:
        return "application/octet-stream"

    if suffix in {".xlsx", ".xls"}:
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    return None
