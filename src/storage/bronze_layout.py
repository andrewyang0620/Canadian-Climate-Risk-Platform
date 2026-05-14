from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class BronzeLayoutError(Exception):
    """Raised when Bronze layout paths are invalid."""


@dataclass(frozen=True)
class BronzeRunLayout:
    """Relative object layout for one Bronze run."""

    source_name: str
    extract_date: str
    run_id: str

    @property
    def run_prefix(self) -> str:
        return f"{self.source_name}/" f"extract_date={self.extract_date}/" f"run_id={self.run_id}"

    def raw_path(self, file_name: str | Path) -> str:
        return f"{self.run_prefix}/raw/{_safe_name(file_name)}"

    @property
    def metadata_path(self) -> str:
        return f"{self.run_prefix}/metadata.json"

    @property
    def run_manifest_path(self) -> str:
        return (
            "_manifests/runs/"
            f"source_name={self.source_name}/"
            f"extract_date={self.extract_date}/"
            f"run_id={self.run_id}.json"
        )


def bronze_relative_path(path: str | Path, *, bronze_root: str | Path) -> str:
    """Return path relative to a Bronze root with normalized separators."""
    full_path = Path(path).resolve()
    root_path = Path(bronze_root).resolve()

    try:
        relative = full_path.relative_to(root_path)
    except ValueError as exc:
        raise BronzeLayoutError(
            f"Path is not under Bronze root. path={full_path}, root={root_path}"
        ) from exc

    return relative.as_posix()


def _safe_name(value: str | Path) -> str:
    raw = str(value).replace("\\", "/").strip()

    if not raw:
        raise BronzeLayoutError("File name must be non-empty.")

    if "/" in raw:
        raise BronzeLayoutError(f"File name cannot include directories: {value}")

    if raw in {".", ".."}:
        raise BronzeLayoutError(f"Unsafe file name: {value}")

    return raw
