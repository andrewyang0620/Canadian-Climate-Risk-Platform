from __future__ import annotations

import argparse
import json
import mimetypes
import os
from pathlib import Path

from src.storage.backends import (
    AzureDataLakeStorageBackend,
    build_storage_backend_from_env,
)
from src.storage.canonical_backfill import (
    CanonicalBackfillPlan,
    build_canonical_backfill_plan,
)


DEFAULT_MANIFEST = "configs/cloud/adls_backfill_manifest.json"
AZURE_BACKENDS = {"azure", "adls", "adls2"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan or execute a canonical ADLS lakehouse backfill."
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

    mode = parser.add_mutually_exclusive_group()

    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate the plan without uploading. Default behavior.",
    )

    mode.add_argument(
        "--execute",
        action="store_true",
        help="Upload the planned objects to ADLS Gen2.",
    )

    parser.add_argument(
        "--output-json",
        help="Optional path for the plan or execution report JSON.",
    )

    return parser


def _plan_to_dict(
    plan: CanonicalBackfillPlan,
    *,
    mode: str,
) -> dict:
    zone_summary = plan.zone_summary()

    return {
        "manifest_version": plan.manifest_version,
        "mode": mode,
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


def _print_summary(
    plan: CanonicalBackfillPlan,
    *,
    mode: str,
) -> None:
    title = (
        "CANONICAL ADLS BACKFILL EXECUTE"
        if mode == "execute"
        else "CANONICAL ADLS BACKFILL DRY RUN"
    )

    print(f"===== {title} =====")
    print(f"entries : {len(plan.entries)}")
    print(f"objects : {plan.object_count}")
    print(
        "GB      :",
        round(plan.total_size_bytes / 1024**3, 3),
    )

    print()
    print("===== BY ZONE =====")

    for zone, stats in sorted(
        plan.zone_summary().items()
    ):
        print(
            f"{zone:<8} "
            f"objects={stats['object_count']:>4} "
            f"GB={stats['total_size_bytes'] / 1024**3:.3f}"
        )


def _write_json(
    path: str | Path,
    payload: dict,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )

    print()
    print(f"Report written: {output_path}")


def _require_azure_backend() -> None:
    backend_name = (
        os.getenv("STORAGE_BACKEND", "")
        .strip()
        .lower()
    )

    if backend_name not in AZURE_BACKENDS:
        raise RuntimeError(
            "--execute requires STORAGE_BACKEND to be "
            "azure, adls, or adls2. "
            f"Current value: {backend_name or '<unset>'}"
        )


def _execute_plan(
    plan: CanonicalBackfillPlan,
) -> dict:
    _require_azure_backend()

    backends: dict[str, AzureDataLakeStorageBackend] = {}

    results = []
    uploaded_objects = 0
    uploaded_bytes = 0
    skipped_objects = 0
    skipped_bytes = 0

    for index, obj in enumerate(plan.objects, start=1):
        if obj.zone not in backends:
            backend = build_storage_backend_from_env(
                zone=obj.zone,
            )

            if not isinstance(
                backend,
                AzureDataLakeStorageBackend,
            ):
                raise RuntimeError(
                    f"Zone {obj.zone} did not resolve "
                    "to AzureDataLakeStorageBackend."
                )

            backends[obj.zone] = backend

        backend = backends[obj.zone]

        content_type, _ = mimetypes.guess_type(
            obj.local_path.name
        )

        file_client = (
            backend.file_system_client
            .get_file_client(obj.remote_path)
        )

        remote_size = None

        try:
            if file_client.exists():
                remote_size = (
                    file_client
                    .get_file_properties()
                    .size
                )
        except Exception:
            remote_size = None

        if remote_size == obj.size_bytes:
            print(
                f"[{index:>3}/{plan.object_count}] "
                f"SKIP "
                f"{obj.zone}/{obj.remote_path} "
                f"({obj.size_bytes / 1024**2:.2f} MB)"
            )

            results.append({
                "entry_name": obj.entry_name,
                "zone": obj.zone,
                "local_path": obj.local_path.as_posix(),
                "remote_path": obj.remote_path,
                "size_bytes": obj.size_bytes,
                "status": "skipped_existing",
                "uri": backend.uri(obj.remote_path),
            })

            skipped_objects += 1
            skipped_bytes += obj.size_bytes

            continue

        print(
            f"[{index:>3}/{plan.object_count}] "
            f"UPLOAD "
            f"{obj.zone}/{obj.remote_path} "
            f"({obj.size_bytes / 1024**2:.2f} MB)"
        )

        try:
            uri = backend.upload_file(
                obj.local_path,
                obj.remote_path,
                content_type=content_type,
            )
        except Exception as exc:
            results.append({
                "entry_name": obj.entry_name,
                "zone": obj.zone,
                "local_path": obj.local_path.as_posix(),
                "remote_path": obj.remote_path,
                "size_bytes": obj.size_bytes,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            })

            return {
                **_plan_to_dict(
                    plan,
                    mode="execute",
                ),
                "execution_status": "failed",
                "uploaded_object_count": uploaded_objects,
                "uploaded_size_bytes": uploaded_bytes,
                "skipped_object_count": skipped_objects,
                "skipped_size_bytes": skipped_bytes,
                "results": results,
            }

        uploaded_objects += 1
        uploaded_bytes += obj.size_bytes

        results.append({
            "entry_name": obj.entry_name,
            "zone": obj.zone,
            "local_path": obj.local_path.as_posix(),
            "remote_path": obj.remote_path,
            "size_bytes": obj.size_bytes,
            "status": "uploaded",
            "uri": uri,
        })

    return {
        **_plan_to_dict(
            plan,
            mode="execute",
        ),
        "execution_status": "success",
        "uploaded_object_count": uploaded_objects,
        "uploaded_size_bytes": uploaded_bytes,
        "skipped_object_count": skipped_objects,
        "skipped_size_bytes": skipped_bytes,
        "results": results,
    }


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    plan = build_canonical_backfill_plan(
        args.manifest,
        zones=args.zones,
    )

    mode = "execute" if args.execute else "dry_run"

    _print_summary(
        plan,
        mode=mode,
    )

    if not args.execute:
        if args.output_json:
            _write_json(
                args.output_json,
                _plan_to_dict(
                    plan,
                    mode="dry_run",
                ),
            )
        return

    report = _execute_plan(plan)

    if args.output_json:
        _write_json(
            args.output_json,
            report,
        )

    print()
    print("===== EXECUTION RESULT =====")
    print(
        "status   :",
        report["execution_status"],
    )
    print(
        "uploaded :",
        report["uploaded_object_count"],
    )
    print(
        "skipped  :",
        report["skipped_object_count"],
    )
    print(
        "GB       :",
        round(
            report["uploaded_size_bytes"] / 1024**3,
            3,
        ),
    )

    if report["execution_status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()