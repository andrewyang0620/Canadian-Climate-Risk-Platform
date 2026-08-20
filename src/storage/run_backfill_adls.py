from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.storage.canonical_backfill import (
    CanonicalBackfillPlan,
    build_canonical_backfill_plan,
)


DEFAULT_MANIFEST = "configs/cloud/adls_backfill_manifest.json"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a canonical ADLS lakehouse backfill plan."
    )

    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST,
        help=f"Backfill manifest path. Default: {DEFAULT_MANIFEST}",
    )

    parser.add_argument(
        "--zone",
        action="append",
        dest="zones",
        help=(
            "Limit the plan to a zone. "
            "May be repeated, e.g. --zone bronze --zone silver."
        ),
    )

    parser.add_argument(
        "--output-json",
        help="Optional path for the generated dry-run plan JSON.",
    )

    return parser


def _plan_to_dict(plan: CanonicalBackfillPlan) -> dict:
    zone_summary = plan.zone_summary()

    return {
        "manifest_version": plan.manifest_version,
        "mode": "dry_run",
        "entry_count": len(plan.entries),
        "object_count": plan.object_count,
        "total_size_bytes": plan.total_size_bytes,
        "zones": {
            zone: {
                "object_count": stats["object_count"],
                "total_size_bytes": stats["total_size_bytes"],
            }
            for zone, stats in sorted(zone_summary.items())
        },
        "entries": [
            {
                "name": entry.name,
                "zone": entry.zone,
                "local_path": entry.local_path.as_posix(),
                "remote_path": entry.remote_path,
            }
            for entry in plan.entries
        ],
        "objects": [
            {
                "entry_name": obj.entry_name,
                "zone": obj.zone,
                "local_path": obj.local_path.as_posix(),
                "remote_path": obj.remote_path,
                "size_bytes": obj.size_bytes,
            }
            for obj in plan.objects
        ],
    }


def _print_summary(plan: CanonicalBackfillPlan) -> None:
    print("CANONICAL ADLS BACKFILL DRY RUN: ")
    print(f"entries : {len(plan.entries)}")
    print(f"objects : {plan.object_count}")
    print("GB      :", round(plan.total_size_bytes / 1024**3, 3))

    print()
    print("BY ZONE: ")

    for zone, stats in sorted(plan.zone_summary().items()):
        print(
            f"{zone:<8} "
            f"objects={stats['object_count']:>4} "
            f"GB={stats['total_size_bytes'] / 1024**3:.3f}"
        )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    plan = build_canonical_backfill_plan(
        args.manifest,
        zones=args.zones,
    )

    _print_summary(plan)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path.write_text(
            json.dumps(
                _plan_to_dict(plan),
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        print()
        print(f"Plan written: {output_path}")


if __name__ == "__main__":
    main()