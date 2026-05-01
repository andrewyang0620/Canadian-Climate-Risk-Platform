from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.ingestion.downloaders.municipal_sources import MunicipalSourceDownloader
from src.ingestion.source_registry import SourceRegistry
from src.utils.time import utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate configured municipal source download availability."
    )

    parser.add_argument(
        "--output",
        default="docs/municipal_source_availability.json",
        help="Output JSON report path.",
    )

    parser.add_argument(
        "--download",
        action="store_true",
        help="Actually download each source. If omitted, only build plans.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    registry = SourceRegistry()
    downloader = MunicipalSourceDownloader(registry=registry)

    report: dict[str, Any] = {
        "validated_at": utc_now_iso(),
        "download_mode": args.download,
        "sources": [],
    }

    for source_name in downloader.list_sources():
        record: dict[str, Any] = {
            "source_name": source_name,
            "status": "unknown",
            "plan": None,
            "download": None,
            "error": None,
        }

        try:
            plan = downloader.build_plan(source_name)
            record["plan"] = {
                "portal_type": plan.portal_type,
                "dataset_id": plan.dataset_id,
                "export_format": plan.export_format,
                "download_url": plan.download_url,
                "target_bronze_table": plan.target_bronze_table,
                "suggested_raw_filename": plan.suggested_raw_filename,
                "implemented": plan.implemented,
            }

            if args.download:
                result = downloader.download_source(source_name)
                record["download"] = {
                    "status_code": result.download.status_code,
                    "content_type": result.download.content_type,
                    "size_bytes": result.download.size_bytes,
                    "final_url": result.download.final_url,
                    "checksum": result.download.checksum,
                }

            record["status"] = "available"

        except Exception as exc:
            record["status"] = "failed"
            record["error"] = {
                "type": exc.__class__.__name__,
                "message": str(exc),
            }

        report["sources"].append(record)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(f"[OK] wrote municipal source availability report -> {output_path}")

    failed = [source for source in report["sources"] if source["status"] == "failed"]
    if failed:
        print(f"[WARN] {len(failed)} municipal source(s) failed availability validation.")
        for item in failed:
            print(
                f"  - {item['source_name']}: {item['error']['type']} | {item['error']['message']}"
            )
    else:
        print("[OK] all municipal sources available.")


if __name__ == "__main__":
    main()
