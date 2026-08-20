from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


VALID_ZONES = {"bronze", "silver", "gold", "audit"}
SUPPORTED_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class CanonicalBackfillEntry:
    name: str
    zone: str
    local_path: Path
    remote_path: str


@dataclass(frozen=True)
class CanonicalBackfillObject:
    entry_name: str
    zone: str
    local_path: Path
    remote_path: str
    size_bytes: int


@dataclass(frozen=True)
class CanonicalBackfillPlan:
    manifest_version: int
    entries: tuple[CanonicalBackfillEntry, ...]
    objects: tuple[CanonicalBackfillObject, ...]

    @property
    def object_count(self) -> int:
        return len(self.objects)

    @property
    def total_size_bytes(self) -> int:
        return sum(obj.size_bytes for obj in self.objects)

    def zone_summary(self) -> dict[str, dict[str, int]]:
        summary: dict[str, dict[str, int]] = {}

        for obj in self.objects:
            stats = summary.setdefault(
                obj.zone,
                {
                    "object_count": 0,
                    "total_size_bytes": 0,
                },
            )
            stats["object_count"] += 1
            stats["total_size_bytes"] += obj.size_bytes

        return summary


def build_canonical_backfill_plan(
    manifest_path: str | Path,
    *,
    zones: Iterable[str] | None = None,
) -> CanonicalBackfillPlan:
    manifest_path = Path(manifest_path)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    version = data.get("manifest_version")
    if version != SUPPORTED_MANIFEST_VERSION:
        raise ValueError(f"Unsupported manifest_version: {version}")

    raw_entries = data.get("entries")
    if not isinstance(raw_entries, list):
        raise ValueError("Manifest entries must be a list.")

    entries = tuple(_parse_entry(raw) for raw in raw_entries)
    _validate_entries(entries)

    selected_zones = None

    if zones is not None:
        selected_zones = {zone.strip().lower() for zone in zones}

        invalid = selected_zones - VALID_ZONES
        if invalid:
            raise ValueError(f"Invalid requested zones: {sorted(invalid)}")

    selected_entries = tuple(
        entry
        for entry in entries
        if selected_zones is None or entry.zone in selected_zones
    )

    objects = _expand_entries(selected_entries)
    _validate_objects(objects)

    return CanonicalBackfillPlan(
        manifest_version=version,
        entries=selected_entries,
        objects=tuple(objects),
    )


def _parse_entry(raw: dict) -> CanonicalBackfillEntry:
    required = {
        "name",
        "zone",
        "local_path",
        "remote_path",
    }

    missing = required - raw.keys()
    if missing:
        raise ValueError(f"Manifest entry missing fields: {sorted(missing)}")

    name = str(raw["name"]).strip()
    zone = str(raw["zone"]).strip().lower()
    local_path = Path(raw["local_path"])
    remote_path = _clean_remote_path(str(raw["remote_path"]))

    if not name:
        raise ValueError("Entry name must be non-empty.")

    if zone not in VALID_ZONES:
        raise ValueError(f"Invalid zone for {name}: {zone}")

    if not local_path.exists():
        raise FileNotFoundError(f"Missing local path for {name}: {local_path}")

    return CanonicalBackfillEntry(
        name=name,
        zone=zone,
        local_path=local_path,
        remote_path=remote_path,
    )


def _clean_remote_path(value: str) -> str:
    raw = value.replace("\\", "/").strip()

    if not raw:
        raise ValueError("remote_path must be non-empty.")

    path = PurePosixPath(raw)

    if path.is_absolute():
        raise ValueError(f"Absolute remote_path is not allowed: {value}")

    parts = raw.split("/")

    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Unsafe remote_path: {value}")

    return "/".join(parts)


def _validate_entries(
    entries: tuple[CanonicalBackfillEntry, ...],
) -> None:
    names: set[str] = set()

    for entry in entries:
        if entry.name in names:
            raise ValueError(f"Duplicate entry name: {entry.name}")

        names.add(entry.name)

    resolved = [
        (entry.name, entry.local_path.resolve())
        for entry in entries
    ]

    for i, (name_a, path_a) in enumerate(resolved):
        for name_b, path_b in resolved[i + 1 :]:
            if (
                path_a != path_b
                and (
                    path_a.is_relative_to(path_b)
                    or path_b.is_relative_to(path_a)
                )
            ):
                raise ValueError(
                    f"Overlapping local entries: {name_a} <-> {name_b}"
                )


def _expand_entries(
    entries: tuple[CanonicalBackfillEntry, ...],
) -> list[CanonicalBackfillObject]:
    objects: list[CanonicalBackfillObject] = []

    for entry in entries:
        local = entry.local_path

        if local.is_file():
            objects.append(
                CanonicalBackfillObject(
                    entry_name=entry.name,
                    zone=entry.zone,
                    local_path=local,
                    remote_path=entry.remote_path,
                    size_bytes=local.stat().st_size,
                )
            )
            continue

        files = sorted(
            path
            for path in local.rglob("*")
            if path.is_file()
        )

        for file_path in files:
            relative = file_path.relative_to(local)

            remote = (
                PurePosixPath(entry.remote_path)
                / PurePosixPath(relative.as_posix())
            )

            objects.append(
                CanonicalBackfillObject(
                    entry_name=entry.name,
                    zone=entry.zone,
                    local_path=file_path,
                    remote_path=remote.as_posix(),
                    size_bytes=file_path.stat().st_size,
                )
            )

    return sorted(
        objects,
        key=lambda obj: (obj.zone, obj.remote_path),
    )


def _validate_objects(
    objects: list[CanonicalBackfillObject],
) -> None:
    remote_targets: set[tuple[str, str]] = set()
    local_files: set[Path] = set()

    for obj in objects:
        remote_key = (obj.zone, obj.remote_path)

        if remote_key in remote_targets:
            raise ValueError(
                f"Duplicate remote target: {obj.zone}/{obj.remote_path}"
            )

        remote_targets.add(remote_key)

        local_key = obj.local_path.resolve()

        if local_key in local_files:
            raise ValueError(f"Local file selected twice: {obj.local_path}")

        local_files.add(local_key)

        lower_path = str(obj.local_path).lower()

        if "_smoke_test" in lower_path:
            raise ValueError(f"Smoke artifact selected: {obj.local_path}")

        if "azure_storage_test" in lower_path:
            raise ValueError(
                f"Azure storage test selected: {obj.local_path}"
            )