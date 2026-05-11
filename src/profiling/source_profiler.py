from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any

from src.audit.bronze_manifest_reader import BronzeManifestReader
from src.utils.config import load_project_config
from src.utils.time import utc_now_iso


class SourceProfilerError(Exception):
    """Raised when source profiling fails."""


class BronzeSourceProfiler:
    """Profile latest Bronze raw files and compare them against source contracts."""

    def __init__(
        self,
        *,
        manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
        source_config_path: str = "source_config.yml",
        source_config: dict[str, Any] | None = None,
        max_sample_rows: int = 5,
        count_rows: bool = False,
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.source_config_path = source_config_path
        self.source_config = source_config or load_project_config(source_config_path)
        self.max_sample_rows = max_sample_rows
        self.count_rows = count_rows

    def run(
        self,
        *,
        source_groups: list[str] | None = None,
        include_sources: list[str] | None = None,
    ) -> dict[str, Any]:
        latest_by_source = BronzeManifestReader(self.manifest_path).latest_successful_by_source()

        sources = self._filtered_sources(
            source_groups=source_groups,
            include_sources=include_sources,
        )

        profiles: list[dict[str, Any]] = []

        for source_name, source_metadata in sources.items():
            latest_run = latest_by_source.get(source_name)

            if latest_run is None:
                profiles.append(
                    {
                        "source_name": source_name,
                        "status": "missing_bronze_run",
                        "message": "No successful Bronze run found in manifest.",
                        "source_group": source_metadata["source_group"],
                        "latest_run": None,
                        "profile": None,
                        "contract_checks": None,
                    }
                )
                continue

            raw_path = Path(latest_run.raw_file_path)

            if not raw_path.exists():
                profiles.append(
                    {
                        "source_name": source_name,
                        "status": "raw_file_missing",
                        "message": f"Raw file not found: {raw_path}",
                        "source_group": source_metadata["source_group"],
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "profile": None,
                        "contract_checks": None,
                    }
                )
                continue

            try:
                file_profile = profile_raw_file(
                    raw_path,
                    max_sample_rows=self.max_sample_rows,
                    count_rows=self.count_rows,
                )
                contract_checks = check_source_contracts(
                    source_metadata=source_metadata,
                    file_profile=file_profile,
                )

                profiles.append(
                    {
                        "source_name": source_name,
                        "status": "profiled",
                        "message": "Profile generated successfully.",
                        "source_group": source_metadata["source_group"],
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "profile": file_profile,
                        "contract_checks": contract_checks,
                    }
                )
            except Exception as exc:
                profiles.append(
                    {
                        "source_name": source_name,
                        "status": "profile_failed",
                        "message": f"{exc.__class__.__name__}: {exc}",
                        "source_group": source_metadata["source_group"],
                        "latest_run": _latest_run_summary(latest_run.raw),
                        "profile": None,
                        "contract_checks": None,
                    }
                )

        return {
            "generated_at": utc_now_iso(),
            "manifest_path": self.manifest_path.as_posix(),
            "source_config_path": self.source_config_path,
            "source_groups": source_groups,
            "include_sources": include_sources,
            "max_sample_rows": self.max_sample_rows,
            "count_rows": self.count_rows,
            "profiled_source_count": sum(item["status"] == "profiled" for item in profiles),
            "missing_bronze_run_count": sum(
                item["status"] == "missing_bronze_run" for item in profiles
            ),
            "raw_file_missing_count": sum(
                item["status"] == "raw_file_missing" for item in profiles
            ),
            "profile_failed_count": sum(item["status"] == "profile_failed" for item in profiles),
            "sources": profiles,
        }

    def write_reports(
        self,
        *,
        output_json_path: str | Path = "lakehouse/profiles/source_schema_profiles.json",
        output_markdown_path: str | Path = "docs/source_schema_profile_summary.md",
        source_groups: list[str] | None = None,
        include_sources: list[str] | None = None,
    ) -> dict[str, Any]:
        report = self.run(
            source_groups=source_groups,
            include_sources=include_sources,
        )

        json_path = Path(output_json_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(report, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        markdown_path = Path(output_markdown_path)
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            render_markdown_summary(report),
            encoding="utf-8",
        )

        return report

    def _filtered_sources(
        self,
        *,
        source_groups: list[str] | None,
        include_sources: list[str] | None,
    ) -> dict[str, dict[str, Any]]:
        sources = self.source_config["sources"]

        selected = sources

        if source_groups:
            allowed_groups = set(source_groups)
            selected = {
                source_name: metadata
                for source_name, metadata in selected.items()
                if metadata["source_group"] in allowed_groups
            }

        if include_sources:
            allowed_sources = set(include_sources)
            selected = {
                source_name: metadata
                for source_name, metadata in selected.items()
                if source_name in allowed_sources
            }

        return selected


def profile_raw_file(
    path: str | Path,
    *,
    max_sample_rows: int = 5,
    count_rows: bool = False,
) -> dict[str, Any]:
    file_path = Path(path)

    if not file_path.exists():
        raise SourceProfilerError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return profile_csv_file(
            file_path,
            max_sample_rows=max_sample_rows,
            count_rows=count_rows,
        )

    if suffix in {".geojson", ".json"}:
        return profile_json_or_geojson_file(
            file_path,
            max_sample_rows=max_sample_rows,
        )

    if suffix in {".xlsx", ".xls"}:
        return profile_excel_file(
            file_path,
            max_sample_rows=max_sample_rows,
            count_rows=count_rows,
        )

    if suffix == ".zip":
        return profile_zip_file(file_path)

    return {
        "file_path": file_path.as_posix(),
        "file_type": "unknown",
        "file_size_bytes": file_path.stat().st_size,
        "columns": [],
        "normalized_columns": [],
        "column_count": 0,
        "row_count_exact": None,
        "sample_rows": [],
        "warning": f"Unsupported file extension for profiling: {suffix}",
    }


def profile_zip_file(path: Path) -> dict[str, Any]:
    """Profile a zip archive, including nested shapefile zip packages."""
    members: list[str] = []
    extension_counts: dict[str, int] = {}
    shapefile_members: list[str] = []
    projection_members: list[str] = []
    shapefile_stems: set[str] = set()
    nested_archive_count = 0
    nested_member_count = 0

    with zipfile.ZipFile(path, "r") as archive:
        members = archive.namelist()

        for member in members:
            suffix = Path(member).suffix.lower()
            _increment_extension_count(extension_counts, suffix)

            if suffix in {".shp", ".dbf", ".shx", ".prj", ".cpg", ".sqlite", ".sqlite3", ".db"}:
                _record_shapefile_member(
                    member_path=member,
                    suffix=suffix,
                    shapefile_members=shapefile_members,
                    projection_members=projection_members,
                    shapefile_stems=shapefile_stems,
                )

            if suffix == ".zip":
                nested_archive_count += 1
                nested_bytes = archive.read(member)

                nested_result = _inspect_nested_zip(
                    parent_member=member,
                    content=nested_bytes,
                )

                nested_member_count += nested_result["member_count"]

                for nested_suffix, count in nested_result["extension_counts"].items():
                    extension_counts[nested_suffix] = extension_counts.get(nested_suffix, 0) + count

                shapefile_members.extend(nested_result["shapefile_members"])
                projection_members.extend(nested_result["projection_members"])
                shapefile_stems.update(nested_result["shapefile_stems"])

    has_sqlite = any(suffix in extension_counts for suffix in [".sqlite", ".sqlite3", ".db"])

    columns = []
    if shapefile_members:
        columns.append("geometry")
    if has_sqlite:
        columns.append("sqlite_database")

    return {
        "file_path": path.as_posix(),
        "file_type": "zip_archive",
        "file_size_bytes": path.stat().st_size,
        "columns": columns,
        "normalized_columns": [_normalize_name(column) for column in columns],
        "column_count": len(columns),
        "row_count_exact": None,
        "archive_member_count": len(members),
        "nested_archive_count": nested_archive_count,
        "nested_member_count": nested_member_count,
        "extension_counts": extension_counts,
        "shapefile_count": len(shapefile_members),
        "shapefile_members": shapefile_members,
        "projection_members": projection_members,
        "shapefile_stems": sorted(shapefile_stems),
        "sample_rows": [
            {
                "archive_member_count": len(members),
                "nested_archive_count": nested_archive_count,
                "nested_member_count": nested_member_count,
                "shapefile_count": len(shapefile_members),
                "sample_members": members[:10],
                "sample_shapefiles": shapefile_members[:10],
            }
        ],
    }


def _inspect_nested_zip(
    *,
    parent_member: str,
    content: bytes,
) -> dict[str, Any]:
    extension_counts: dict[str, int] = {}
    shapefile_members: list[str] = []
    projection_members: list[str] = []
    shapefile_stems: set[str] = set()

    with zipfile.ZipFile(io.BytesIO(content), "r") as nested_archive:
        nested_members = nested_archive.namelist()

        for nested_member in nested_members:
            suffix = Path(nested_member).suffix.lower()
            _increment_extension_count(extension_counts, suffix)

            nested_path = f"{parent_member}::{nested_member}"

            if suffix in {".shp", ".dbf", ".shx", ".prj", ".cpg", ".sqlite", ".sqlite3", ".db"}:
                _record_shapefile_member(
                    member_path=nested_path,
                    suffix=suffix,
                    shapefile_members=shapefile_members,
                    projection_members=projection_members,
                    shapefile_stems=shapefile_stems,
                )

    return {
        "member_count": len(nested_members),
        "extension_counts": extension_counts,
        "shapefile_members": shapefile_members,
        "projection_members": projection_members,
        "shapefile_stems": shapefile_stems,
    }


def _record_shapefile_member(
    *,
    member_path: str,
    suffix: str,
    shapefile_members: list[str],
    projection_members: list[str],
    shapefile_stems: set[str],
) -> None:
    if suffix == ".shp":
        shapefile_members.append(member_path)

    if suffix == ".prj":
        projection_members.append(member_path)

    if suffix in {".shp", ".dbf", ".shx", ".prj", ".cpg"}:
        shapefile_stems.add(str(Path(member_path).with_suffix("")))


def _increment_extension_count(
    extension_counts: dict[str, int],
    suffix: str,
) -> None:
    if suffix:
        extension_counts[suffix] = extension_counts.get(suffix, 0) + 1


def profile_csv_file(
    path: Path,
    *,
    max_sample_rows: int,
    count_rows: bool,
) -> dict[str, Any]:
    for encoding in ["utf-8-sig", "latin-1"]:
        try:
            return _profile_csv_file_with_encoding(
                path,
                max_sample_rows=max_sample_rows,
                count_rows=count_rows,
                encoding=encoding,
            )
        except UnicodeDecodeError:
            continue

    raise SourceProfilerError(f"Unable to decode CSV file: {path}")


def _profile_csv_file_with_encoding(
    path: Path,
    *,
    max_sample_rows: int,
    count_rows: bool,
    encoding: str,
) -> dict[str, Any]:
    sample_rows: list[dict[str, Any]] = []
    row_count = 0
    delimiter = _detect_csv_delimiter(path, encoding)

    with path.open("r", encoding=encoding, newline="") as file:
        reader = csv.reader(file, delimiter=delimiter)
        header = next(reader, [])

        for row in reader:
            row_count += 1

            if len(sample_rows) < max_sample_rows:
                sample_rows.append(_row_to_dict(header, row))

            if not count_rows and len(sample_rows) >= max_sample_rows:
                break

    return {
        "file_path": path.as_posix(),
        "file_type": "csv",
        "encoding": encoding,
        "delimiter": delimiter,
        "file_size_bytes": path.stat().st_size,
        "columns": header,
        "normalized_columns": [_normalize_name(column) for column in header],
        "column_count": len(header),
        "row_count_exact": row_count if count_rows else None,
        "row_count_counted": count_rows,
        "sample_rows": sample_rows,
    }


def _detect_csv_delimiter(path: Path, encoding: str) -> str:
    with path.open("r", encoding=encoding, newline="") as file:
        sample = file.read(65536)

    if not sample:
        return ","

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;	|")
        return dialect.delimiter
    except csv.Error:
        pass

    first_nonempty_line = ""
    for line in sample.splitlines():
        if line.strip():
            first_nonempty_line = line
            break

    if not first_nonempty_line:
        return ","

    candidate_counts = {
        ",": first_nonempty_line.count(","),
        ";": first_nonempty_line.count(";"),
        "	": first_nonempty_line.count("	"),
        "|": first_nonempty_line.count("|"),
    }

    best_delimiter = max(candidate_counts, key=candidate_counts.get)

    if candidate_counts[best_delimiter] == 0:
        return ","

    return best_delimiter


def profile_json_or_geojson_file(
    path: Path,
    *,
    max_sample_rows: int,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(payload, dict) and isinstance(payload.get("features"), list):
        return _profile_geojson_payload(
            path,
            payload,
            max_sample_rows=max_sample_rows,
        )

    if isinstance(payload, list):
        columns = sorted(
            {key for row in payload[: max_sample_rows * 10] if isinstance(row, dict) for key in row}
        )
        return {
            "file_path": path.as_posix(),
            "file_type": "json_records",
            "file_size_bytes": path.stat().st_size,
            "columns": columns,
            "normalized_columns": [_normalize_name(column) for column in columns],
            "column_count": len(columns),
            "row_count_exact": len(payload),
            "sample_rows": [_truncate_row(row) for row in payload[:max_sample_rows]],
        }

    if isinstance(payload, dict):
        columns = sorted(payload.keys())
        return {
            "file_path": path.as_posix(),
            "file_type": "json_object",
            "file_size_bytes": path.stat().st_size,
            "columns": columns,
            "normalized_columns": [_normalize_name(column) for column in columns],
            "column_count": len(columns),
            "row_count_exact": None,
            "sample_rows": [_truncate_row(payload)],
        }

    raise SourceProfilerError(f"Unsupported JSON structure: {path}")


def _profile_geojson_payload(
    path: Path,
    payload: dict[str, Any],
    *,
    max_sample_rows: int,
) -> dict[str, Any]:
    features = payload.get("features", [])

    property_keys: set[str] = set()
    geometry_types: Counter[str] = Counter()
    sample_rows: list[dict[str, Any]] = []

    for feature in features:
        if not isinstance(feature, dict):
            continue

        properties = feature.get("properties", {})
        if isinstance(properties, dict):
            property_keys.update(properties.keys())

        geometry = feature.get("geometry")
        if isinstance(geometry, dict):
            geometry_type = geometry.get("type")
            if geometry_type:
                geometry_types[str(geometry_type)] += 1

        if len(sample_rows) < max_sample_rows:
            sample = dict(properties) if isinstance(properties, dict) else {}
            sample["geometry_type"] = geometry.get("type") if isinstance(geometry, dict) else None
            sample_rows.append(_truncate_row(sample))

    columns = sorted(property_keys)
    all_columns = columns + ["geometry"]

    return {
        "file_path": path.as_posix(),
        "file_type": "geojson",
        "file_size_bytes": path.stat().st_size,
        "columns": all_columns,
        "property_columns": columns,
        "normalized_columns": [_normalize_name(column) for column in all_columns],
        "column_count": len(all_columns),
        "feature_count": len(features),
        "row_count_exact": len(features),
        "geometry_types": dict(geometry_types),
        "sample_rows": sample_rows,
    }


def profile_excel_file(
    path: Path,
    *,
    max_sample_rows: int,
    count_rows: bool,
) -> dict[str, Any]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise SourceProfilerError("Excel profiling requires pandas and openpyxl.") from exc

    excel = pd.ExcelFile(path)
    sheet_name = excel.sheet_names[0]

    if count_rows:
        df = pd.read_excel(path, sheet_name=sheet_name)
        row_count = int(len(df))
        sample_df = df.head(max_sample_rows)
    else:
        sample_df = pd.read_excel(path, sheet_name=sheet_name, nrows=max_sample_rows)
        row_count = None

    columns = [str(column) for column in sample_df.columns]

    return {
        "file_path": path.as_posix(),
        "file_type": "excel",
        "file_size_bytes": path.stat().st_size,
        "sheet_names": excel.sheet_names,
        "profiled_sheet_name": sheet_name,
        "columns": columns,
        "normalized_columns": [_normalize_name(column) for column in columns],
        "column_count": len(columns),
        "row_count_exact": row_count,
        "row_count_counted": count_rows,
        "sample_rows": [
            _truncate_row(row) for row in sample_df.fillna("").to_dict(orient="records")
        ],
    }


def check_source_contracts(
    *,
    source_metadata: dict[str, Any],
    file_profile: dict[str, Any],
) -> dict[str, Any]:
    columns = file_profile.get("columns", [])
    normalized_columns = {_normalize_name(column) for column in columns}

    checks: dict[str, Any] = {}

    checks["required_fields"] = _check_candidate_list(
        candidate_fields=source_metadata.get("required_fields", []),
        normalized_columns=normalized_columns,
        require_all=True,
    )

    if "identity_contract" in source_metadata:
        contract = source_metadata["identity_contract"]
        checks["identity_contract"] = _check_candidate_list(
            candidate_fields=contract.get("candidate_id_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )

    if "join_contract" in source_metadata:
        contract = source_metadata["join_contract"]
        checks["join_contract"] = _check_candidate_list(
            candidate_fields=contract.get("candidate_join_keys", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )

    if "coordinate_contract" in source_metadata:
        contract = source_metadata["coordinate_contract"]
        lat = _check_candidate_list(
            candidate_fields=contract.get("candidate_latitude_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )
        lon = _check_candidate_list(
            candidate_fields=contract.get("candidate_longitude_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )
        geometry = _check_candidate_list(
            candidate_fields=contract.get("candidate_geometry_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )
        checks["coordinate_contract"] = {
            "passed": (lat["passed"] is True and lon["passed"] is True)
            or geometry["passed"] is True,
            "latitude": lat,
            "longitude": lon,
            "geometry": geometry,
        }

    if "location_mapping_contract" in source_metadata:
        contract = source_metadata["location_mapping_contract"]
        checks["location_mapping_contract"] = _check_candidate_list(
            candidate_fields=contract.get("candidate_location_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )

    if "municipality_mapping_contract" in source_metadata:
        contract = source_metadata["municipality_mapping_contract"]
        checks["municipality_mapping_contract"] = _check_candidate_list(
            candidate_fields=contract.get("candidate_geography_fields", []),
            normalized_columns=normalized_columns,
            require_all=False,
        )

    if "climate_measurement_contract" in source_metadata:
        contract = source_metadata["climate_measurement_contract"]
        checks["climate_measurement_contract"] = _check_grouped_candidates(
            grouped_candidates=contract.get("candidate_raw_fields", {}),
            normalized_columns=normalized_columns,
        )

    if "measurement_contract" in source_metadata:
        contract = source_metadata["measurement_contract"]
        grouped_candidates = contract.get("candidate_raw_fields")
        if grouped_candidates:
            checks["measurement_contract"] = _check_grouped_candidates(
                grouped_candidates=grouped_candidates,
                normalized_columns=normalized_columns,
            )
        else:
            checks["measurement_contract"] = {
                "passed": None,
                "message": (
                    "No candidate_raw_fields defined; measurement contract is "
                    "expected to be validated after Silver standardization."
                ),
            }

    return checks


def _check_candidate_list(
    *,
    candidate_fields: list[str],
    normalized_columns: set[str],
    require_all: bool,
) -> dict[str, Any]:
    found = []
    missing = []

    for field in candidate_fields:
        normalized = _normalize_name(field)
        if normalized in normalized_columns:
            found.append(field)
        else:
            missing.append(field)

    if not candidate_fields:
        passed: bool | None = None
    elif require_all:
        passed = len(missing) == 0
    else:
        passed = len(found) > 0

    return {
        "passed": passed,
        "require_all": require_all,
        "candidate_fields": candidate_fields,
        "found": found,
        "missing": missing,
    }


def _check_grouped_candidates(
    *,
    grouped_candidates: dict[str, list[str]],
    normalized_columns: set[str],
) -> dict[str, Any]:
    group_results = {}

    for group_name, candidates in grouped_candidates.items():
        group_results[group_name] = _check_candidate_list(
            candidate_fields=candidates,
            normalized_columns=normalized_columns,
            require_all=False,
        )

    return {
        "passed": any(result["passed"] is True for result in group_results.values()),
        "groups": group_results,
    }


def render_markdown_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Source Schema Profile Summary",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "This summary is generated from local Bronze raw files. It verifies raw schema, candidate join keys, coordinate fields, measurement fields, and downstream source contracts before Silver standardization.",
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
        "| Source | Status | File Type | Rows / Features | Columns | Contract Notes |",
        "|---|---:|---:|---:|---:|---|",
    ]

    for item in report["sources"]:
        profile = item.get("profile") or {}
        checks = item.get("contract_checks") or {}

        source_name = item["source_name"]
        status = item["status"]
        file_type = profile.get("file_type", "")
        row_count = _profile_row_count(profile)
        column_count = profile.get("column_count", "")
        contract_notes = _contract_notes_for_summary(checks)

        lines.append(
            f"| `{source_name}` | `{status}` | `{file_type}` | `{row_count}` | `{column_count}` | {contract_notes} |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `required_fields` are checked against profiled raw columns.",
            "- Candidate contracts are checked by case-insensitive normalized field matching.",
            "- Large CSV row counts are exact only when profiling is run with `--count-rows`.",
            "- This file should be reviewed before implementing Silver standardization logic.",
            "",
        ]
    )

    return "\n".join(lines)


def _profile_row_count(profile: dict[str, Any]) -> Any:
    if "row_count_exact" in profile and profile["row_count_exact"] is not None:
        return profile["row_count_exact"]

    if "feature_count" in profile and profile["feature_count"] is not None:
        return profile["feature_count"]

    return ""


def _contract_notes_for_summary(checks: dict[str, Any]) -> str:
    if not checks:
        return ""

    notes = []

    for name, result in checks.items():
        if isinstance(result, dict) and "passed" in result:
            notes.append(f"`{name}:{result['passed']}`")
        elif isinstance(result, dict):
            notes.append(f"`{name}:nested`")

    return " ".join(notes)


def _row_to_dict(header: list[str], row: list[str]) -> dict[str, Any]:
    output = {}

    for index, column in enumerate(header):
        value = row[index] if index < len(row) else None
        output[column] = _truncate_value(value)

    return output


def _truncate_row(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _truncate_value(value) for key, value in row.items()}


def _truncate_value(value: Any, max_length: int = 120) -> Any:
    if value is None:
        return None

    text = str(value)
    if len(text) <= max_length:
        return text

    return text[:max_length] + "..."


def _normalize_name(value: str) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
    )


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile latest Bronze raw sources.")

    parser.add_argument(
        "--manifest-path",
        default="lakehouse/bronze/_manifests/bronze_runs.jsonl",
        help="Path to Bronze manifest JSONL.",
    )

    parser.add_argument(
        "--source-config-path",
        default="source_config.yml",
        help="Config file name under configs/.",
    )

    parser.add_argument(
        "--output-json",
        default="lakehouse/profiles/source_schema_profiles.json",
        help="Output JSON profile path.",
    )

    parser.add_argument(
        "--output-md",
        default="docs/source_schema_profile_summary.md",
        help="Output Markdown summary path.",
    )

    parser.add_argument(
        "--source-group",
        action="append",
        default=None,
        help="Optional source group filter. Can be repeated.",
    )

    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Optional source name filter. Can be repeated.",
    )

    parser.add_argument(
        "--max-sample-rows",
        type=int,
        default=5,
        help="Maximum number of sample rows to store.",
    )

    parser.add_argument(
        "--count-rows",
        action="store_true",
        help="Count exact CSV/Excel rows. Slower for large files.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    profiler = BronzeSourceProfiler(
        manifest_path=args.manifest_path,
        source_config_path=args.source_config_path,
        max_sample_rows=args.max_sample_rows,
        count_rows=args.count_rows,
    )

    report = profiler.write_reports(
        output_json_path=args.output_json,
        output_markdown_path=args.output_md,
        source_groups=args.source_group,
        include_sources=args.source,
    )

    print(
        f"[OK] wrote source profiles -> {args.output_json} and {args.output_md} | "
        f"profiled={report['profiled_source_count']} | "
        f"missing={report['missing_bronze_run_count']} | "
        f"failed={report['profile_failed_count']}"
    )


if __name__ == "__main__":
    main()
