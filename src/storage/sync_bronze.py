from __future__ import annotations

import argparse
import json

from src.storage import build_storage_backend_from_env
from src.storage.bronze_sync import BronzeSyncer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync local Bronze lakehouse files to the configured storage backend."
    )

    parser.add_argument(
        "--bronze-root",
        default="lakehouse/bronze",
        help="Local Bronze root directory.",
    )

    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Optional source name to sync. Can be repeated.",
    )

    parser.add_argument(
        "--no-manifests",
        action="store_true",
        help="Do not sync Bronze manifest files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned uploads without writing to the backend.",
    )

    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional path to write sync results as JSON.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    backend = build_storage_backend_from_env(zone="bronze")

    syncer = BronzeSyncer(
        bronze_root=args.bronze_root,
        storage_backend=backend,
    )

    results = syncer.sync(
        include_sources=args.source,
        include_manifests=not args.no_manifests,
        dry_run=args.dry_run,
    )

    payload = {
        "dry_run": args.dry_run,
        "bronze_root": args.bronze_root,
        "source_filter": args.source,
        "object_count": len(results),
        "total_size_bytes": sum(item.size_bytes for item in results),
        "objects": [item.__dict__ for item in results],
    }

    print(json.dumps(payload, indent=2))

    if args.output_json:
        from pathlib import Path

        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
