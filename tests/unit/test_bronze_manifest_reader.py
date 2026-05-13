import json

import pytest

from src.audit.audit_models import BronzeManifestError, BronzeRunRecord
from src.audit.bronze_manifest_reader import BronzeManifestReader


def make_manifest_record(
    *,
    run_id="run-1",
    source_name="canadian_disaster_database",
    extract_timestamp="2026-04-29T10:00:00+00:00",
    manifest_record_created_at="2026-04-29T10:01:00+00:00",
    load_status="success",
    file_size_bytes=100,
    row_count=10,
):
    return {
        "run_id": run_id,
        "source_name": source_name,
        "source_group": "national",
        "provider": "Public Safety Canada",
        "extract_timestamp": extract_timestamp,
        "extract_date": "2026-04-29",
        "raw_file_path": (
            f"lakehouse/bronze/{source_name}/extract_date=2026-04-29/"
            f"run_id={run_id}/raw/source.csv"
        ),
        "metadata_path": (
            f"lakehouse/bronze/{source_name}/extract_date=2026-04-29/"
            f"run_id={run_id}/metadata.json"
        ),
        "file_name": "source.csv",
        "file_size_bytes": file_size_bytes,
        "file_checksum": "abc123",
        "checksum_algorithm": "sha256",
        "ingestion_method": "test",
        "row_count": row_count,
        "target_bronze_table": f"bronze_{source_name}",
        "target_silver_table": f"silver_{source_name}",
        "load_status": load_status,
        "manifest_record_created_at": manifest_record_created_at,
    }


def write_jsonl(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def test_bronze_run_record_from_dict_parses_valid_record():
    payload = make_manifest_record()

    record = BronzeRunRecord.from_dict(payload)

    assert record.run_id == "run-1"
    assert record.source_name == "canadian_disaster_database"
    assert record.file_size_bytes == 100
    assert record.row_count == 10
    assert record.is_successful is True
    assert record.raw == payload


def test_bronze_run_record_rejects_missing_required_fields():
    payload = make_manifest_record()
    del payload["run_id"]

    with pytest.raises(BronzeManifestError):
        BronzeRunRecord.from_dict(payload)


def test_bronze_run_record_rejects_invalid_file_size_type():
    payload = make_manifest_record(file_size_bytes="100")

    with pytest.raises(BronzeManifestError):
        BronzeRunRecord.from_dict(payload)


def test_manifest_reader_reads_records(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(run_id="run-1", source_name="source_a"),
        make_manifest_record(run_id="run-2", source_name="source_b"),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    parsed = reader.read_records()

    assert len(parsed) == 2
    assert parsed[0].source_name == "source_a"
    assert parsed[1].source_name == "source_b"


def test_manifest_reader_rejects_missing_manifest(tmp_path):
    reader = BronzeManifestReader(tmp_path / "missing.jsonl")

    with pytest.raises(BronzeManifestError):
        reader.read_records()


def test_manifest_reader_rejects_invalid_json_line(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    manifest_path.write_text("{bad json\n", encoding="utf-8")

    reader = BronzeManifestReader(manifest_path)

    with pytest.raises(BronzeManifestError):
        reader.read_records()


def test_manifest_reader_filters_successful_records(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(run_id="run-1", source_name="source_a", load_status="success"),
        make_manifest_record(run_id="run-2", source_name="source_b", load_status="failed"),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    successful = reader.successful_records()

    assert len(successful) == 1
    assert successful[0].source_name == "source_a"


def test_manifest_reader_groups_records_by_source(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(run_id="run-1", source_name="source_a"),
        make_manifest_record(run_id="run-2", source_name="source_a"),
        make_manifest_record(run_id="run-3", source_name="source_b"),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    grouped = reader.records_by_source()

    assert set(grouped) == {"source_a", "source_b"}
    assert len(grouped["source_a"]) == 2
    assert len(grouped["source_b"]) == 1


def test_manifest_reader_returns_latest_successful_by_source(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(
            run_id="old-success",
            source_name="source_a",
            extract_timestamp="2026-04-29T10:00:00+00:00",
            load_status="success",
        ),
        make_manifest_record(
            run_id="new-success",
            source_name="source_a",
            extract_timestamp="2026-04-29T11:00:00+00:00",
            load_status="success",
        ),
        make_manifest_record(
            run_id="newer-failed",
            source_name="source_a",
            extract_timestamp="2026-04-29T12:00:00+00:00",
            load_status="failed",
        ),
        make_manifest_record(
            run_id="source-b-success",
            source_name="source_b",
            extract_timestamp="2026-04-29T09:00:00+00:00",
            load_status="success",
        ),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    latest = reader.latest_successful_by_source()

    assert latest["source_a"].run_id == "new-success"
    assert latest["source_b"].run_id == "source-b-success"


def test_manifest_reader_latest_successful_for_source(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(run_id="run-1", source_name="source_a"),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    assert reader.latest_successful_for_source("source_a").run_id == "run-1"
    assert reader.latest_successful_for_source("missing_source") is None


def test_manifest_reader_source_name_helpers(tmp_path):
    manifest_path = tmp_path / "bronze_runs.jsonl"
    records = [
        make_manifest_record(source_name="source_a", load_status="success"),
        make_manifest_record(source_name="source_b", load_status="failed"),
    ]
    write_jsonl(manifest_path, records)

    reader = BronzeManifestReader(manifest_path)

    assert reader.source_names_with_any_runs() == {"source_a", "source_b"}
    assert reader.source_names_with_successful_runs() == {"source_a"}
