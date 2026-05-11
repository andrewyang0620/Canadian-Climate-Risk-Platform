from __future__ import annotations

import argparse
import json
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from hashlib import sha256

from src.ingestion.downloaders.http_downloader import HttpDownloader
from src.utils.config import load_project_config


class NationalCoreIngestionError(Exception):
    """Raised when national core Bronze ingestion fails."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def compute_bytes_sha256(content: bytes) -> str:
    return sha256(content).hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, sort_keys=True) + "\n")


def download_census_boundaries(
    *,
    output_root: str | Path = "lakehouse/bronze",
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> dict[str, Any]:
    config = load_project_config("source_config.yml")
    source = config["sources"]["census_boundaries"]

    downloads = source.get("boundary_downloads")
    if not downloads:
        raise NationalCoreIngestionError(
            "census_boundaries.boundary_downloads is missing from source_config.yml"
        )

    run_id = str(uuid.uuid4())
    extract_timestamp = utc_now_iso()
    extract_date = utc_today()

    source_name = "census_boundaries"
    base_dir = Path(output_root) / source_name / f"extract_date={extract_date}" / f"run_id={run_id}"
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    package_path = raw_dir / "census_boundaries_2021_raw_package.zip"

    http = HttpDownloader()
    downloaded_members: list[dict[str, Any]] = []

    with zipfile.ZipFile(package_path, mode="w", compression=zipfile.ZIP_DEFLATED) as out_zip:
        for download_name, download_cfg in downloads.items():
            url = download_cfg["url"]
            filename = download_cfg["filename"]

            result = http.get(url)
            member_path = f"{download_name}/{filename}"
            out_zip.writestr(member_path, result.content)

            downloaded_members.append(
                {
                    "download_name": download_name,
                    "boundary_level": download_cfg.get("boundary_level"),
                    "boundary_type": download_cfg.get("boundary_type"),
                    "census_year": download_cfg.get("census_year"),
                    "url": url,
                    "filename": filename,
                    "package_member_path": member_path,
                    "download_status_code": result.status_code,
                    "download_content_type": result.content_type,
                    "download_size_bytes": result.size_bytes,
                    "download_checksum": result.checksum,
                    "download_final_url": result.final_url,
                }
            )

    package_content = package_path.read_bytes()
    package_checksum = compute_bytes_sha256(package_content)
    package_size = package_path.stat().st_size

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "display_name": source["display_name"],
        "source_group": source["source_group"],
        "provider": source["provider"],
        "source_url": source["source_url"],
        "extract_timestamp": extract_timestamp,
        "extract_date": extract_date,
        "raw_file_path": package_path.as_posix(),
        "file_name": package_path.name,
        "file_size_bytes": package_size,
        "file_checksum": package_checksum,
        "checksum_algorithm": "sha256",
        "ingestion_method": "direct_boundary_file_download",
        "row_count": None,
        "schema_hash": None,
        "source_period_start": None,
        "source_period_end": None,
        "target_bronze_table": source["target_bronze_table"],
        "target_silver_table": source["target_silver_table"],
        "load_status": "success",
        "extra_metadata": {
            "boundary_package_type": "combined_raw_zip_package",
            "downloaded_member_count": len(downloaded_members),
            "downloads": downloaded_members,
            "note": (
                "Raw package contains Statistics Canada 2021 province and census "
                "subdivision boundary zip files. Silver processing must extract and "
                "standardize these into province and municipality boundary tables."
            ),
        },
    }

    metadata_path = base_dir / "metadata.json"
    write_json(metadata_path, metadata)

    manifest_record = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
        "manifest_record_created_at": utc_now_iso(),
    }

    append_jsonl(Path(manifest_path), manifest_record)

    print(
        f"[OK] downloaded census_boundaries -> {package_path} | "
        f"size_bytes={package_size} | run_id={run_id}"
    )

    return manifest_record


def download_wildfire_history(
    *,
    output_root: str | Path = "lakehouse/bronze",
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> dict[str, Any]:
    config = load_project_config("source_config.yml")
    source = config["sources"]["wildfire_history"]

    download_cfg = source.get("wildfire_download")
    if not download_cfg:
        raise NationalCoreIngestionError(
            "wildfire_history.wildfire_download is missing from source_config.yml"
        )

    run_id = str(uuid.uuid4())
    extract_timestamp = utc_now_iso()
    extract_date = utc_today()

    source_name = "wildfire_history"
    base_dir = Path(output_root) / source_name / f"extract_date={extract_date}" / f"run_id={run_id}"
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / "NFDB_point_raw.zip"

    http = HttpDownloader()
    result = http.get(download_cfg["url"])
    raw_path.write_bytes(result.content)

    raw_content = raw_path.read_bytes()
    raw_checksum = compute_bytes_sha256(raw_content)
    raw_size = raw_path.stat().st_size

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "display_name": source["display_name"],
        "source_group": source["source_group"],
        "provider": source["provider"],
        "source_url": source["source_url"],
        "extract_timestamp": extract_timestamp,
        "extract_date": extract_date,
        "raw_file_path": raw_path.as_posix(),
        "file_name": raw_path.name,
        "file_size_bytes": raw_size,
        "file_checksum": raw_checksum,
        "checksum_algorithm": "sha256",
        "ingestion_method": "direct_zip_download",
        "row_count": None,
        "schema_hash": None,
        "source_period_start": None,
        "source_period_end": None,
        "target_bronze_table": source["target_bronze_table"],
        "target_silver_table": source["target_silver_table"],
        "load_status": "success",
        "extra_metadata": {
            "dataset_name": download_cfg.get("dataset_name"),
            "download_url": download_cfg["url"],
            "download_filename": download_cfg["filename"],
            "download_format": download_cfg.get("format"),
            "download_source_version": download_cfg.get("source_version"),
            "download_status_code": result.status_code,
            "download_content_type": result.content_type,
            "download_size_bytes": result.size_bytes,
            "download_checksum": result.checksum,
            "download_final_url": result.final_url,
            "note": download_cfg.get("note"),
        },
    }

    metadata_path = base_dir / "metadata.json"
    write_json(metadata_path, metadata)

    manifest_record = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
        "manifest_record_created_at": utc_now_iso(),
    }

    append_jsonl(Path(manifest_path), manifest_record)

    print(
        f"[OK] downloaded wildfire_history -> {raw_path} | "
        f"size_bytes={raw_size} | run_id={run_id}"
    )

    return manifest_record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run national core Bronze ingestion.")

    parser.add_argument(
        "--download-census-boundaries",
        action="store_true",
        help="Download Statistics Canada province + CSD boundary files into Bronze.",
    )

    parser.add_argument(
        "--download-wildfire-history",
        action="store_true",
        help="Download Canadian National Fire Database fire point data into Bronze.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.download_census_boundaries:
        download_census_boundaries()
        return

    if args.download_wildfire_history:
        download_wildfire_history()
        return

    raise SystemExit(
        "No action selected. Use --download-census-boundaries or " "--download-wildfire-history."
    )


if __name__ == "__main__":
    main()
