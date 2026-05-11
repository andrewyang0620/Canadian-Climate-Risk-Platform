from __future__ import annotations

import argparse
import gzip
import json
import uuid
import requests
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
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


def download_hydat_archive(
    *,
    output_root: str | Path = "lakehouse/bronze",
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> dict[str, Any]:
    config = load_project_config("source_config.yml")
    source = config["sources"]["hydat_archive"]

    download_cfg = source.get("hydat_download")
    if not download_cfg:
        raise NationalCoreIngestionError(
            "hydat_archive.hydat_download is missing from source_config.yml"
        )

    run_id = str(uuid.uuid4())
    extract_timestamp = utc_now_iso()
    extract_date = utc_today()

    source_name = "hydat_archive"
    base_dir = Path(output_root) / source_name / f"extract_date={extract_date}" / f"run_id={run_id}"
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / "Hydat_sqlite3_raw.zip"

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
            "release_date": download_cfg.get("release_date"),
            "download_url": download_cfg["url"],
            "download_filename": download_cfg["filename"],
            "download_format": download_cfg.get("format"),
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
        f"[OK] downloaded hydat_archive -> {raw_path} | " f"size_bytes={raw_size} | run_id={run_id}"
    )

    return manifest_record


def download_eccc_historical_climate_bc_ab(
    *,
    output_root: str | Path = "lakehouse/bronze",
    manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
) -> dict[str, Any]:
    """Download BC + Alberta daily climate observations from ECCC GeoMet OGC API.

    This writes one gzipped JSONL raw file per year and records the whole
    multi-file extract as one Bronze run.
    """
    config = load_project_config("source_config.yml")
    source = config["sources"]["eccc_historical_climate"]
    api_cfg = source["climate_daily_api"]

    run_id = str(uuid.uuid4())
    extract_timestamp = utc_now_iso()
    extract_date = utc_today()

    source_name = "eccc_historical_climate"
    base_dir = Path(output_root) / source_name / f"extract_date={extract_date}" / f"run_id={run_id}"
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    start_year = int(api_cfg["datetime_start"][:4])
    end_year = int(api_cfg["datetime_end"][:4])

    all_year_outputs: list[dict[str, Any]] = []
    total_record_count = 0
    total_pages = 0
    first_year = None
    last_year = None

    for year in range(start_year, end_year + 1):
        year_start = f"{year}-01-01T00:00:00Z"
        year_end = f"{year}-12-31T23:59:59Z"
        out_path = raw_dir / f"eccc_climate_daily_bc_ab_{year}.jsonl.gz"

        result = _download_eccc_climate_year_to_jsonl_gz(
            items_url=api_cfg["items_url"],
            out_path=out_path,
            bbox=api_cfg["bbox"],
            datetime_range=f"{year_start}/{year_end}",
            limit=int(api_cfg.get("page_limit", 10000)),
            target_provinces=set(api_cfg.get("target_provinces", [])),
        )

        all_year_outputs.append(
            {
                "year": year,
                "file_name": out_path.name,
                "raw_file_path": out_path.as_posix(),
                "file_size_bytes": out_path.stat().st_size,
                "file_checksum": compute_bytes_sha256(out_path.read_bytes()),
                "record_count": result["record_count"],
                "page_count": result["page_count"],
                "first_item_datetime": result.get("first_item_datetime"),
                "last_item_datetime": result.get("last_item_datetime"),
            }
        )

        total_record_count += result["record_count"]
        total_pages += result["page_count"]

        if result["record_count"] > 0:
            first_year = year if first_year is None else min(first_year, year)
            last_year = year if last_year is None else max(last_year, year)

        print(
            f"[OK] ECCC climate {year}: records={result['record_count']} "
            f"pages={result['page_count']} file={out_path}"
        )

    manifest_payload = {
        "files": all_year_outputs,
        "record_count_total": total_record_count,
        "page_count_total": total_pages,
        "datetime_start": api_cfg["datetime_start"],
        "datetime_end": api_cfg["datetime_end"],
        "bbox": api_cfg["bbox"],
        "target_provinces": api_cfg.get("target_provinces", []),
    }

    manifest_raw_path = raw_dir / "eccc_climate_daily_bc_ab_manifest.json"
    write_json(manifest_raw_path, manifest_payload)

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "display_name": source["display_name"],
        "source_group": source["source_group"],
        "provider": source["provider"],
        "source_url": source["source_url"],
        "extract_timestamp": extract_timestamp,
        "extract_date": extract_date,
        "raw_file_path": (
            all_year_outputs[0]["raw_file_path"]
            if all_year_outputs
            else manifest_raw_path.as_posix()
        ),
        "file_name": (
            all_year_outputs[0]["file_name"] if all_year_outputs else manifest_raw_path.name
        ),
        "file_size_bytes": sum(item["file_size_bytes"] for item in all_year_outputs),
        "file_checksum": compute_bytes_sha256(manifest_raw_path.read_bytes()),
        "checksum_algorithm": "sha256",
        "ingestion_method": "geomet_ogc_api_pagination_jsonl_gzip",
        "row_count": total_record_count,
        "schema_hash": None,
        "source_period_start": api_cfg["datetime_start"],
        "source_period_end": api_cfg["datetime_end"],
        "target_bronze_table": source["target_bronze_table"],
        "target_silver_table": source["target_silver_table"],
        "load_status": "success",
        "extra_metadata": {
            "collection": api_cfg["collection"],
            "items_url": api_cfg["items_url"],
            "bbox_name": api_cfg["bbox_name"],
            "bbox": api_cfg["bbox"],
            "target_provinces": api_cfg.get("target_provinces", []),
            "partition_by": "year",
            "year_file_count": len(all_year_outputs),
            "record_count_total": total_record_count,
            "page_count_total": total_pages,
            "raw_manifest_path": manifest_raw_path.as_posix(),
            "year_outputs": all_year_outputs,
            "note": api_cfg.get("note"),
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
        f"[OK] downloaded eccc_historical_climate BC/AB -> "
        f"years={len(all_year_outputs)} records={total_record_count} "
        f"run_id={run_id}"
    )

    return manifest_record


def _request_eccc_json_with_retries(
    *,
    url: str,
    params: dict[str, Any] | None,
    timeout: int = 120,
    max_attempts: int = 6,
) -> dict[str, Any]:
    """Request ECCC GeoMet JSON with retry/backoff for unstable long downloads."""
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": "canadian-climate-risk-platform/0.1"},
            )
            response.raise_for_status()
            return response.json()

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt == max_attempts:
                break

            sleep_seconds = min(60, 2**attempt)
            print(
                "[WARN] ECCC climate request failed; "
                f"attempt={attempt}/{max_attempts}, "
                f"retry_in={sleep_seconds}s, "
                f"error={exc.__class__.__name__}: {exc}"
            )
            time.sleep(sleep_seconds)

    raise NationalCoreIngestionError(
        "ECCC climate API request failed after retries. "
        f"url={url}, params={params}, last_error={last_error}"
    )


def _download_eccc_climate_year_to_jsonl_gz(
    *,
    items_url: str,
    out_path: Path,
    bbox: list[float],
    datetime_range: str,
    limit: int,
    target_provinces: set[str],
) -> dict[str, Any]:
    params = {
        "f": "json",
        "bbox": ",".join(str(value) for value in bbox),
        "datetime": datetime_range,
        "limit": limit,
    }

    next_url: str | None = items_url
    next_params: dict[str, Any] | None = params

    record_count = 0
    page_count = 0
    first_item_datetime = None
    last_item_datetime = None

    with gzip.open(out_path, "wt", encoding="utf-8") as output:
        while next_url:
            payload = _request_eccc_json_with_retries(
                url=next_url,
                params=next_params,
                timeout=120,
                max_attempts=6,
            )

            features = payload.get("features", [])
            if not isinstance(features, list):
                raise NationalCoreIngestionError(
                    f"ECCC climate API returned invalid features payload: {next_url}"
                )

            for feature in features:
                if not isinstance(feature, dict):
                    continue

                if not _feature_matches_target_provinces(feature, target_provinces):
                    continue

                output.write(json.dumps(feature, sort_keys=True) + "\n")
                record_count += 1

                item_dt = _extract_feature_datetime(feature)
                if item_dt:
                    first_item_datetime = (
                        item_dt
                        if first_item_datetime is None
                        else min(first_item_datetime, item_dt)
                    )
                    last_item_datetime = (
                        item_dt if last_item_datetime is None else max(last_item_datetime, item_dt)
                    )

            page_count += 1

            next_link = None
            for link in payload.get("links", []):
                if isinstance(link, dict) and link.get("rel") == "next" and link.get("href"):
                    next_link = link["href"]
                    break

            next_url = next_link
            next_params = None

    return {
        "record_count": record_count,
        "page_count": page_count,
        "first_item_datetime": first_item_datetime,
        "last_item_datetime": last_item_datetime,
    }


def _feature_matches_target_provinces(
    feature: dict[str, Any],
    target_provinces: set[str],
) -> bool:
    if not target_provinces:
        return True

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        return True

    candidates = [
        "PROVINCE_CODE",
        "province_code",
        "PROV_STATE_TERR_CODE",
        "prov_state_terr_code",
        "PROVINCE",
        "province",
    ]

    for key in candidates:
        value = properties.get(key)
        if value is None:
            continue

        normalized = str(value).strip().upper()
        if normalized in target_provinces:
            return True

        if normalized in {"BRITISH COLUMBIA", "B.C.", "B C"} and "BC" in target_provinces:
            return True

        if normalized == "ALBERTA" and "AB" in target_provinces:
            return True

    # If the API payload does not expose a province field, keep the record because
    # the request is already spatially scoped by the BC/AB bounding box.
    return True


def _extract_feature_datetime(feature: dict[str, Any]) -> str | None:
    properties = feature.get("properties")
    if not isinstance(properties, dict):
        return None

    for key in [
        "LOCAL_DATE",
        "local_date",
        "DATE",
        "date",
        "DATETIME",
        "datetime",
    ]:
        value = properties.get(key)
        if value:
            return str(value)

    return None


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

    parser.add_argument(
        "--download-hydat-archive",
        action="store_true",
        help="Download HYDAT SQLite archive into Bronze.",
    )

    parser.add_argument(
        "--download-eccc-historical-climate-bc-ab",
        action="store_true",
        help="Download ECCC climate-daily observations for BC + Alberta into Bronze.",
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

    if args.download_hydat_archive:
        download_hydat_archive()
        return

    if args.download_eccc_historical_climate_bc_ab:
        download_eccc_historical_climate_bc_ab()
        return

    raise SystemExit(
        "No action selected. Use --download-census-boundaries, "
        "--download-wildfire-history, --download-hydat-archive, or "
        "--download-eccc-historical-climate-bc-ab."
    )


if __name__ == "__main__":
    main()
