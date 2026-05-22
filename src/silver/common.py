from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SilverRunResult:
    # Result table from the runs
    source_name: str
    run_id: str
    extract_date: str
    output_tables: list[dict[str, Any]]
    metadata_path: str


def utc_now_iso() -> str:
    # date to datetime
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_today() -> str:
    # datetime to day
    return datetime.now(timezone.utc).date().isoformat()


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    # write json metadata
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def append_jsonl(path: str | Path, payload: dict[str, Any]) -> None:
    # write silver runs
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, sort_keys=True) + "\n")


def file_sha256(path: str | Path) -> str:
    # check the completeness of the output
    hasher = sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_parquet(path: str | Path, dataframe: pd.DataFrame) -> None:
    # DataFrame to Parquet format
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(target, index=False)


def latest_successful_bronze_raw_path(
    *,
    source_name: str,
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> Path:
    # Find the latest successful bronze run
    manifest = Path(manifest_path)

    if not manifest.exists():
        raise FileNotFoundError(f"Bronze manifest does not exist: {manifest}")

    records: list[dict[str, Any]] = []

    with manifest.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue

            record = json.loads(stripped)

            if record.get("source_name") != source_name:
                continue

            if record.get("load_status") != "success":
                continue

            extra_metadata = record.get("extra_metadata") or {}
            if extra_metadata.get("smoke_test"):
                continue

            if not record.get("raw_file_path"):
                continue

            records.append(record)

    if not records:
        raise FileNotFoundError(
            f"No successful non-smoke Bronze run found for source={source_name}"
        )

    latest = max(
        records,
        key=lambda record: (
            str(record.get("extract_timestamp") or ""),
            str(record.get("run_id") or ""),
        ),
    )

    raw_path = Path(latest["raw_file_path"])

    if not raw_path.exists():
        raise FileNotFoundError(f"Latest Bronze raw file does not exist: {raw_path}")

    return raw_path


def latest_successful_bronze_record(
    *,
    source_name: str,
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> dict[str, Any]:
    """Return latest successful non-smoke Bronze manifest record for one source."""
    manifest = Path(manifest_path)

    if not manifest.exists():
        raise FileNotFoundError(f"Bronze manifest does not exist: {manifest}")

    records: list[dict[str, Any]] = []

    with manifest.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue

            record = json.loads(stripped)

            if record.get("source_name") != source_name:
                continue

            if record.get("load_status") != "success":
                continue

            extra_metadata = record.get("extra_metadata") or {}
            if extra_metadata.get("smoke_test"):
                continue

            records.append(record)

    if not records:
        raise FileNotFoundError(
            f"No successful non-smoke Bronze run found for source={source_name}"
        )

    return max(
        records,
        key=lambda record: (
            str(record.get("extract_timestamp") or ""),
            str(record.get("run_id") or ""),
        ),
    )
