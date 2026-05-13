from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from src.audit.bronze_manifest_reader import BronzeManifestReader
from src.utils.config import load_project_config
from src.utils.time import utc_now_iso


class ArchiveInternalProfilerError(Exception):
    """Raised when internal archive profiling fails."""


def normalize_name(value: str) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
    )


class ArchiveInternalProfiler:
    """Inspect internal schemas of complex Bronze archives.

    This profiler is for archive sources where package-level profiling is not enough:
    - census_boundaries: nested shapefile zip packages
    - wildfire_history: shapefile zip package
    - hydat_archive: SQLite archive zip
    """

    def __init__(
        self,
        *,
        manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
        source_config_path: str = "source_config.yml",
        source_config: dict[str, Any] | None = None,
        sample_rows: int = 3,
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.source_config_path = source_config_path
        self.source_config = source_config or load_project_config(source_config_path)
        self.sample_rows = sample_rows

    def run(self, *, include_sources: list[str]) -> dict[str, Any]:
        latest_by_source = BronzeManifestReader(self.manifest_path).latest_successful_by_source()

        results: list[dict[str, Any]] = []

        for source_name in include_sources:
            source_config = self.source_config["sources"].get(source_name)

            if source_config is None:
                results.append(
                    {
                        "source_name": source_name,
                        "status": "source_not_in_config",
                        "message": "Source is not defined in source_config.yml.",
                        "latest_run": None,
                        "internal_profile": None,
                        "contract_checks": None,
                    }
                )
                continue

            latest_run = latest_by_source.get(source_name)

            if latest_run is None:
                results.append(
                    {
                        "source_name": source_name,
                        "status": "missing_bronze_run",
                        "message": "No successful Bronze run found in manifest.",
                        "latest_run": None,
                        "internal_profile": None,
                        "contract_checks": None,
                    }
                )
                continue

            raw_path = Path(latest_run.raw_file_path)

            if not raw_path.exists():
                results.append(
                    {
                        "source_name": source_name,
                        "status": "raw_file_missing",
                        "message": f"Raw file not found: {raw_path}",
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "internal_profile": None,
                        "contract_checks": None,
                    }
                )
                continue

            try:
                profile = profile_archive_internal_schema(
                    source_name=source_name,
                    raw_path=raw_path,
                    sample_rows=self.sample_rows,
                )
                checks = check_archive_contracts(
                    source_name=source_name,
                    source_config=source_config,
                    internal_profile=profile,
                )

                results.append(
                    {
                        "source_name": source_name,
                        "status": "profiled",
                        "message": "Internal archive profile generated successfully.",
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "internal_profile": profile,
                        "contract_checks": checks,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "source_name": source_name,
                        "status": "profile_failed",
                        "message": f"{exc.__class__.__name__}: {exc}",
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "internal_profile": None,
                        "contract_checks": None,
                    }
                )

        return {
            "generated_at": utc_now_iso(),
            "manifest_path": self.manifest_path.as_posix(),
            "source_config_path": self.source_config_path,
            "profile_type": "archive_internal_schema",
            "requested_sources": include_sources,
            "profiled_source_count": sum(item["status"] == "profiled" for item in results),
            "missing_bronze_run_count": sum(
                item["status"] == "missing_bronze_run" for item in results
            ),
            "raw_file_missing_count": sum(item["status"] == "raw_file_missing" for item in results),
            "profile_failed_count": sum(item["status"] == "profile_failed" for item in results),
            "sources": results,
        }

    def write_reports(
        self,
        *,
        include_sources: list[str],
        output_json_path: str | Path = "lakehouse/profiles/archive_internal_profiles.json",
        output_markdown_path: str | Path = "docs/archive_internal_profile_summary.md",
    ) -> dict[str, Any]:
        report = self.run(include_sources=include_sources)

        json_path = Path(output_json_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )

        md_path = Path(output_markdown_path)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(render_markdown_summary(report), encoding="utf-8")

        return report


def profile_archive_internal_schema(
    *,
    source_name: str,
    raw_path: Path,
    sample_rows: int = 3,
) -> dict[str, Any]:
    if source_name in {"census_boundaries", "wildfire_history"}:
        return profile_shapefile_archive(raw_path=raw_path, sample_rows=sample_rows)

    if source_name == "hydat_archive":
        return profile_sqlite_archive(raw_path=raw_path, sample_rows=sample_rows)

    raise ArchiveInternalProfilerError(
        f"Unsupported archive internal profiler source: {source_name}"
    )


def profile_shapefile_archive(
    *,
    raw_path: Path,
    sample_rows: int = 3,
) -> dict[str, Any]:
    """Extract a zip archive and inspect all internal shapefile layers."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        extract_root = temp_root / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        _extract_zip_recursive(raw_path, extract_root)

        shp_files = sorted(extract_root.rglob("*.shp"))

        layers = [
            inspect_shapefile_layer(shp_path=shp_path, sample_rows=sample_rows)
            for shp_path in shp_files
        ]

    return {
        "archive_type": "shapefile_zip",
        "raw_file_path": raw_path.as_posix(),
        "raw_file_size_bytes": raw_path.stat().st_size,
        "layer_count": len(layers),
        "layers": layers,
    }


def inspect_shapefile_layer(
    *,
    shp_path: Path,
    sample_rows: int = 3,
) -> dict[str, Any]:
    try:
        import fiona
    except ImportError as exc:
        raise ArchiveInternalProfilerError("Shapefile internal profiling requires fiona.") from exc

    errors: list[str] = []

    for encoding in [None, "utf-8", "latin1", "cp1252"]:
        try:
            open_kwargs = {}
            if encoding is not None:
                open_kwargs["encoding"] = encoding

            with fiona.open(shp_path, **open_kwargs) as collection:
                schema = collection.schema or {}
                properties = schema.get("properties", {})
                crs = collection.crs_wkt or collection.crs
                geometry_type = schema.get("geometry")
                feature_count = len(collection)
                columns = list(properties.keys())

                samples: list[dict[str, Any]] = []
                sample_error: str | None = None

                try:
                    for idx, feature in enumerate(collection):
                        if idx >= sample_rows:
                            break

                        feature_props = feature.get("properties") or {}
                        samples.append(
                            {
                                key: _truncate_value(value)
                                for key, value in dict(feature_props).items()
                            }
                        )
                except Exception as sample_exc:
                    # Some public shapefiles contain invalid DBF date values
                    # such as year 0. Schema, CRS, geometry type, and columns
                    # are still valid for contract profiling, so do not fail
                    # the entire source because sample row decoding failed.
                    sample_error = f"{sample_exc.__class__.__name__}: {sample_exc}"

            return {
                "layer_path": shp_path.as_posix(),
                "layer_name": shp_path.stem,
                "feature_count": feature_count,
                "geometry_type": geometry_type,
                "crs": str(crs),
                "encoding_used": encoding or "default",
                "column_count": len(columns),
                "columns": columns,
                "property_types": dict(properties),
                "sample_rows": samples,
                "sample_error": sample_error,
            }

        except Exception as exc:
            errors.append(f"encoding={encoding or 'default'}: " f"{exc.__class__.__name__}: {exc}")

    raise ArchiveInternalProfilerError(
        f"Failed to inspect shapefile layer {shp_path}. Errors: {errors}"
    )


def profile_sqlite_archive(
    *,
    raw_path: Path,
    sample_rows: int = 3,
) -> dict[str, Any]:
    """Extract a HYDAT SQLite archive and inspect table schemas."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        extract_root = temp_root / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)

        _extract_zip_recursive(raw_path, extract_root)

        sqlite_files = sorted(
            list(extract_root.rglob("*.sqlite"))
            + list(extract_root.rglob("*.sqlite3"))
            + list(extract_root.rglob("*.db"))
        )

        if not sqlite_files:
            raise ArchiveInternalProfilerError(
                f"No SQLite database found inside archive: {raw_path}"
            )

        sqlite_path = sqlite_files[0]
        profile = inspect_sqlite_database(
            sqlite_path=sqlite_path,
            sample_rows=sample_rows,
        )

    profile["archive_type"] = "sqlite_zip"
    profile["raw_file_path"] = raw_path.as_posix()
    profile["raw_file_size_bytes"] = raw_path.stat().st_size

    return profile


def inspect_sqlite_database(
    *,
    sqlite_path: Path,
    sample_rows: int = 3,
) -> dict[str, Any]:
    conn = sqlite3.connect(sqlite_path)

    try:
        table_names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]

        tables = []
        for table_name in table_names:
            columns = inspect_sqlite_table_columns(conn, table_name)
            row_count = count_sqlite_table_rows(conn, table_name)
            samples = sample_sqlite_table_rows(conn, table_name, sample_rows)

            tables.append(
                {
                    "table_name": table_name,
                    "row_count": row_count,
                    "column_count": len(columns),
                    "columns": columns,
                    "sample_rows": samples,
                }
            )

        return {
            "sqlite_file_path": sqlite_path.as_posix(),
            "sqlite_file_size_bytes": sqlite_path.stat().st_size,
            "table_count": len(table_names),
            "table_names": table_names,
            "tables": tables,
        }
    finally:
        conn.close()


def inspect_sqlite_table_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()

    return [
        {
            "cid": row[0],
            "name": row[1],
            "type": row[2],
            "notnull": bool(row[3]),
            "default_value": row[4],
            "pk": bool(row[5]),
        }
        for row in rows
    ]


def count_sqlite_table_rows(
    conn: sqlite3.Connection,
    table_name: str,
) -> int:
    return int(conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0])


def sample_sqlite_table_rows(
    conn: sqlite3.Connection,
    table_name: str,
    sample_rows: int,
) -> list[dict[str, Any]]:
    if sample_rows <= 0:
        return []

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(f'SELECT * FROM "{table_name}" LIMIT {sample_rows}')
    rows = cursor.fetchall()

    return [{key: _truncate_value(row[key]) for key in row.keys()} for row in rows]


def _extract_zip_recursive(
    archive_path: Path,
    destination: Path,
) -> None:
    with zipfile.ZipFile(archive_path, "r") as archive:
        archive.extractall(destination)

    nested_zips = sorted(destination.rglob("*.zip"))

    for nested_zip in nested_zips:
        nested_destination = nested_zip.parent / f"__extracted_{nested_zip.stem}"
        nested_destination.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(nested_zip, "r") as nested_archive:
            nested_archive.extractall(nested_destination)


def check_archive_contracts(
    *,
    source_name: str,
    source_config: dict[str, Any],
    internal_profile: dict[str, Any],
) -> dict[str, Any]:
    if source_name == "census_boundaries":
        return check_census_boundary_contracts(source_config, internal_profile)

    if source_name == "wildfire_history":
        return check_wildfire_contracts(source_config, internal_profile)

    if source_name == "hydat_archive":
        return check_hydat_contracts(source_config, internal_profile)

    return {}


def check_census_boundary_contracts(
    source_config: dict[str, Any],
    internal_profile: dict[str, Any],
) -> dict[str, Any]:
    layers = internal_profile.get("layers", [])

    province_layer = _find_layer_by_required_columns(
        layers,
        required_all=["PRUID", "PRNAME"],
    )
    csd_layer = _find_layer_by_required_columns(
        layers,
        required_all=["CSDUID", "CSDNAME"],
    )

    return {
        "province_layer_detected": {
            "passed": province_layer is not None,
            "layer_name": province_layer.get("layer_name") if province_layer else None,
            "required_columns": ["PRUID", "PRNAME"],
        },
        "csd_layer_detected": {
            "passed": csd_layer is not None,
            "layer_name": csd_layer.get("layer_name") if csd_layer else None,
            "required_columns": ["CSDUID", "CSDNAME"],
        },
        "required_boundary_outputs": {
            "passed": set(
                source_config.get("boundary_contract", {}).get("required_boundary_outputs", [])
            )
            >= {"silver_boundary_province", "silver_boundary_municipality"},
        },
    }


def check_wildfire_contracts(
    source_config: dict[str, Any],
    internal_profile: dict[str, Any],
) -> dict[str, Any]:
    layers = internal_profile.get("layers", [])
    main_layer = layers[0] if layers else None
    columns = main_layer.get("columns", []) if main_layer else []
    normalized_columns = {normalize_name(column) for column in columns}

    contract = source_config.get("wildfire_event_contract", {})

    return {
        "main_layer_detected": {
            "passed": main_layer is not None,
            "layer_name": main_layer.get("layer_name") if main_layer else None,
        },
        "event_id_field": _candidate_check(
            contract.get("candidate_event_id_fields", []),
            normalized_columns,
        ),
        "year_field": _candidate_check(
            contract.get("candidate_year_fields", []),
            normalized_columns,
        ),
        "date_field": _candidate_check(
            contract.get("candidate_date_fields", []),
            normalized_columns,
        ),
        "size_field": _candidate_check(
            contract.get("candidate_size_fields", []),
            normalized_columns,
        ),
    }


def check_hydat_contracts(
    source_config: dict[str, Any],
    internal_profile: dict[str, Any],
) -> dict[str, Any]:
    table_names = set(internal_profile.get("table_names", []))
    normalized_table_names = {normalize_name(table) for table in table_names}

    contract = source_config.get("measurement_contract", {})
    candidate_tables = contract.get("candidate_tables", [])

    table_checks = {}
    for table in candidate_tables:
        table_checks[table] = {
            "passed": normalize_name(table) in normalized_table_names,
        }

    return {
        "candidate_tables": table_checks,
        "stations_table_detected": {
            "passed": "stations" in normalized_table_names,
        },
        "daily_flow_or_level_detected": {
            "passed": (
                "dly_flows" in normalized_table_names or "dly_levels" in normalized_table_names
            )
        },
    }


def _find_layer_by_required_columns(
    layers: list[dict[str, Any]],
    *,
    required_all: list[str],
) -> dict[str, Any] | None:
    normalized_required = {normalize_name(column) for column in required_all}

    for layer in layers:
        normalized_columns = {normalize_name(column) for column in layer.get("columns", [])}

        if normalized_required <= normalized_columns:
            return layer

    return None


def _find_layer_by_column_candidates(
    layers: list[dict[str, Any]],
    *,
    required_any: list[str],
) -> dict[str, Any] | None:
    normalized_required = {normalize_name(column) for column in required_any}

    for layer in layers:
        normalized_columns = {normalize_name(column) for column in layer.get("columns", [])}

        if normalized_required & normalized_columns:
            return layer

    return None


def _candidate_check(
    candidates: list[str],
    normalized_columns: set[str],
) -> dict[str, Any]:
    found = []
    missing = []

    for candidate in candidates:
        if normalize_name(candidate) in normalized_columns:
            found.append(candidate)
        else:
            missing.append(candidate)

    return {
        "passed": len(found) > 0 if candidates else None,
        "candidate_fields": candidates,
        "found": found,
        "missing": missing,
    }


def render_markdown_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Archive Internal Profile Summary",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "This summary inspects internal schemas inside complex Bronze archives such as nested shapefile packages and SQLite archives.",
        "",
        "## Summary",
        "",
        f"- Profiled sources: `{report['profiled_source_count']}`",
        f"- Missing Bronze runs: `{report['missing_bronze_run_count']}`",
        f"- Raw files missing: `{report['raw_file_missing_count']}`",
        f"- Profile failures: `{report['profile_failed_count']}`",
        "",
        "## Sources",
        "",
        "| Source | Status | Archive Type | Internal Objects | Key Contract Checks |",
        "|---|---:|---:|---:|---|",
    ]

    for item in report["sources"]:
        profile = item.get("internal_profile") or {}
        checks = item.get("contract_checks") or {}

        archive_type = profile.get("archive_type", "")
        internal_objects = _internal_object_summary(profile)
        check_summary = _contract_summary(checks)

        lines.append(
            f"| `{item['source_name']}` | `{item['status']}` | `{archive_type}` | `{internal_objects}` | {check_summary} |"
        )

    lines.extend(
        [
            "",
            "## Detail Notes",
            "",
            "- Census boundaries are checked for province and CSD layers.",
            "- Wildfire history is checked for event ID, year/date, and size candidate fields.",
            "- HYDAT is checked for core hydrometric tables such as STATIONS, DLY_FLOWS, and DLY_LEVELS.",
            "- This file validates that downloaded archives are usable for Silver standardization.",
            "",
        ]
    )

    return "\n".join(lines)


def _internal_object_summary(profile: dict[str, Any]) -> str:
    archive_type = profile.get("archive_type")

    if archive_type == "shapefile_zip":
        return f"layers={profile.get('layer_count', 0)}"

    if archive_type == "sqlite_zip":
        return f"tables={profile.get('table_count', 0)}"

    return ""


def _contract_summary(checks: dict[str, Any]) -> str:
    if not checks:
        return ""

    parts = []

    for key, value in checks.items():
        if isinstance(value, dict) and "passed" in value:
            parts.append(f"`{key}:{value['passed']}`")
        elif isinstance(value, dict):
            nested_pass = _nested_pass_status(value)
            parts.append(f"`{key}:{nested_pass}`")

    return " ".join(parts)


def _nested_pass_status(value: dict[str, Any]) -> bool | None:
    statuses = []

    for nested in value.values():
        if isinstance(nested, dict) and "passed" in nested:
            statuses.append(nested["passed"])

    if not statuses:
        return None

    return all(status is True for status in statuses)


def _latest_run_summary(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "run_id",
        "source_name",
        "extract_timestamp",
        "raw_file_path",
        "metadata_path",
        "file_size_bytes",
        "row_count",
        "target_bronze_table",
        "target_silver_table",
    ]
    return {key: payload.get(key) for key in keys}


def _truncate_value(value: Any, max_length: int = 120) -> Any:
    if value is None:
        return None

    text = str(value)

    if len(text) <= max_length:
        return text

    return text[:max_length] + "..."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile internal schemas inside Bronze archive files."
    )

    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help="Source name to profile. Can be repeated.",
    )

    parser.add_argument(
        "--manifest-path",
        default="lakehouse/bronze/_manifests/bronze_runs.jsonl",
        help="Path to Bronze manifest JSONL.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/profiles/archive_internal_profiles.json",
        help="Output JSON profile path.",
    )

    parser.add_argument(
        "--output-md",
        default="docs/archive_internal_profile_summary.md",
        help="Output Markdown summary path.",
    )

    parser.add_argument(
        "--sample-rows",
        type=int,
        default=3,
        help="Number of sample rows to collect from internal tables/layers.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    profiler = ArchiveInternalProfiler(
        manifest_path=args.manifest_path,
        sample_rows=args.sample_rows,
    )

    report = profiler.write_reports(
        include_sources=args.source,
        output_json_path=args.output_json,
        output_markdown_path=args.output_md,
    )

    print(
        f"[OK] wrote archive internal profiles -> {args.output_json} and {args.output_md} | "
        f"profiled={report['profiled_source_count']} | "
        f"missing={report['missing_bronze_run_count']} | "
        f"failed={report['profile_failed_count']}"
    )


if __name__ == "__main__":
    main()
