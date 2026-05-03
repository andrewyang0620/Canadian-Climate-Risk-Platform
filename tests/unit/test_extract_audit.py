import json

from src.audit.extract_audit import ExtractAuditor


def make_source_config():
    return {
        "sources": {
            "source_a": {
                "source_group": "municipal",
                "target_bronze_table": "bronze_source_a",
                "target_silver_table": "silver_source_a",
            },
            "source_b": {
                "source_group": "municipal",
                "target_bronze_table": "bronze_source_b",
                "target_silver_table": "silver_source_b",
            },
            "source_c": {
                "source_group": "national",
                "target_bronze_table": "bronze_source_c",
                "target_silver_table": "silver_source_c",
            },
        }
    }


def make_manifest_record(
    *,
    source_name="source_a",
    run_id="run-1",
    target_bronze_table="bronze_source_a",
    target_silver_table="silver_source_a",
    raw_file_path,
    metadata_path,
    file_size_bytes=100,
    row_count=10,
):
    return {
        "run_id": run_id,
        "source_name": source_name,
        "source_group": "municipal",
        "provider": "Test Provider",
        "extract_timestamp": "2026-04-29T10:00:00+00:00",
        "extract_date": "2026-04-29",
        "raw_file_path": str(raw_file_path),
        "metadata_path": str(metadata_path),
        "file_name": "source.csv",
        "file_size_bytes": file_size_bytes,
        "file_checksum": "abc123",
        "checksum_algorithm": "sha256",
        "ingestion_method": "test",
        "row_count": row_count,
        "target_bronze_table": target_bronze_table,
        "target_silver_table": target_silver_table,
        "load_status": "success",
        "manifest_record_created_at": "2026-04-29T10:01:00+00:00",
    }


def write_jsonl(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def write_metadata(path, *, source_name="source_a", row_count_validation=True):
    extra_metadata = {
        "row_count_validation_supported": True,
        "socrata_expected_row_count": 10,
        "socrata_actual_row_count": 10,
        "socrata_row_count_validation_passed": row_count_validation,
    }

    path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "source_name": source_name,
                "file_size_bytes": 100,
                "file_checksum": "abc123",
                "load_status": "success",
                "row_count": 10,
                "ingestion_method": "test",
                "extra_metadata": extra_metadata,
            }
        ),
        encoding="utf-8",
    )


def test_extract_audit_passes_for_valid_latest_run(tmp_path):
    raw_file = tmp_path / "source.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    raw_file.write_text("id,value\n1,10\n", encoding="utf-8")
    write_metadata(metadata_file)

    write_jsonl(
        manifest_file,
        [
            make_manifest_record(
                raw_file_path=raw_file,
                metadata_path=metadata_file,
            )
        ],
    )

    auditor = ExtractAuditor(
        manifest_path=manifest_file,
        source_config_path="unused.yml",
        source_config=make_source_config(),
    )

    report = auditor.run(source_groups=["municipal"])

    source_a = next(result for result in report.results if result.source_name == "source_a")

    assert source_a.status == "pass"
    assert source_a.checks["has_successful_bronze_run"] is True
    assert source_a.checks["file_size_positive"] is True
    assert source_a.checks["checksum_present"] is True
    assert source_a.checks["raw_file_exists"] is True
    assert source_a.checks["metadata_file_exists"] is True
    assert source_a.checks["table_contract_matches_config"] is True
    assert source_a.checks["row_count_validation_passed_if_available"] is True


def test_extract_audit_marks_missing_source(tmp_path):
    manifest_file = tmp_path / "bronze_runs.jsonl"
    write_jsonl(manifest_file, [])

    auditor = ExtractAuditor(
        manifest_path=manifest_file,
        source_config_path="unused.yml",
        source_config=make_source_config(),
    )

    report = auditor.run(source_groups=["municipal"])

    statuses = {result.source_name: result.status for result in report.results}

    assert statuses["source_a"] == "missing"
    assert statuses["source_b"] == "missing"
    assert report.missing_source_count == 2
    assert report.overall_status == "fail"


def test_extract_audit_fails_when_raw_file_missing(tmp_path):
    raw_file = tmp_path / "missing.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    write_metadata(metadata_file)
    write_jsonl(
        manifest_file,
        [
            make_manifest_record(
                raw_file_path=raw_file,
                metadata_path=metadata_file,
            )
        ],
    )

    auditor = ExtractAuditor(
        manifest_path=manifest_file,
        source_config_path="unused.yml",
        source_config=make_source_config(),
    )

    report = auditor.run(source_groups=["municipal"])
    source_a = next(result for result in report.results if result.source_name == "source_a")

    assert source_a.status == "fail"
    assert source_a.checks["raw_file_exists"] is False


def test_extract_audit_fails_when_table_contract_mismatches(tmp_path):
    raw_file = tmp_path / "source.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    raw_file.write_text("id,value\n1,10\n", encoding="utf-8")
    write_metadata(metadata_file)

    write_jsonl(
        manifest_file,
        [
            make_manifest_record(
                raw_file_path=raw_file,
                metadata_path=metadata_file,
                target_silver_table="wrong_silver_table",
            )
        ],
    )

    auditor = ExtractAuditor(
        manifest_path=manifest_file,
        source_config_path="unused.yml",
        source_config=make_source_config(),
    )

    report = auditor.run(source_groups=["municipal"])
    source_a = next(result for result in report.results if result.source_name == "source_a")

    assert source_a.status == "fail"
    assert source_a.checks["table_contract_matches_config"] is False


def test_extract_audit_can_write_report(tmp_path):
    raw_file = tmp_path / "source.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"
    output_file = tmp_path / "audit" / "extract_audit.json"

    raw_file.write_text("id,value\n1,10\n", encoding="utf-8")
    write_metadata(metadata_file)

    write_jsonl(
        manifest_file,
        [
            make_manifest_record(
                raw_file_path=raw_file,
                metadata_path=metadata_file,
            )
        ],
    )

    auditor = ExtractAuditor(
        manifest_path=manifest_file,
        source_config_path="unused.yml",
        source_config=make_source_config(),
    )

    report = auditor.write_report(
        output_path=output_file,
        source_groups=["municipal"],
    )

    assert output_file.exists()

    payload = json.loads(output_file.read_text(encoding="utf-8"))

    assert payload["overall_status"] == report.overall_status
    assert payload["configured_source_count"] == 2
    assert "results" in payload
