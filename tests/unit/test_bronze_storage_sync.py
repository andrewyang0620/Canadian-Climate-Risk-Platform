from pathlib import Path

import pytest

from src.storage import LocalStorageBackend
from src.storage.bronze_layout import BronzeLayoutError, BronzeRunLayout, bronze_relative_path
from src.storage.bronze_sync import BronzeSyncer, guess_content_type


def test_bronze_run_layout_builds_relative_paths():
    layout = BronzeRunLayout(
        source_name="eccc_historical_climate",
        extract_date="2026-05-11",
        run_id="run-123",
    )

    assert layout.run_prefix == ("eccc_historical_climate/extract_date=2026-05-11/run_id=run-123")
    assert layout.raw_path("data.jsonl.gz") == (
        "eccc_historical_climate/extract_date=2026-05-11/" "run_id=run-123/raw/data.jsonl.gz"
    )
    assert layout.metadata_path.endswith("/metadata.json")
    assert layout.run_manifest_path == (
        "_manifests/runs/source_name=eccc_historical_climate/"
        "extract_date=2026-05-11/run_id=run-123.json"
    )


def test_bronze_relative_path_rejects_paths_outside_root(tmp_path):
    root = tmp_path / "bronze"
    root.mkdir()

    outside = tmp_path / "outside.txt"
    outside.write_text("bad", encoding="utf-8")

    with pytest.raises(BronzeLayoutError):
        bronze_relative_path(outside, bronze_root=root)


def test_guess_content_type_handles_common_bronze_files():
    assert guess_content_type("metadata.json") == "application/json"
    assert guess_content_type("records.jsonl") == "application/x-ndjson"
    assert guess_content_type("records.jsonl.gz") == "application/gzip"
    assert guess_content_type("data.csv") == "text/csv"
    assert guess_content_type("shape.zip") == "application/zip"


def test_bronze_syncer_plans_source_and_manifests(tmp_path):
    bronze_root = tmp_path / "lakehouse" / "bronze"

    source_file = (
        bronze_root / "test_source" / "extract_date=2026-05-11" / "run_id=abc" / "raw" / "data.json"
    )
    source_file.parent.mkdir(parents=True)
    source_file.write_text("{}", encoding="utf-8")

    manifest_file = bronze_root / "_manifests" / "bronze_runs.jsonl"
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text('{"ok": true}\n', encoding="utf-8")

    backend = LocalStorageBackend(tmp_path / "target" / "bronze")
    syncer = BronzeSyncer(bronze_root=bronze_root, storage_backend=backend)

    plan = syncer.plan(include_sources=["test_source"], include_manifests=True)

    relative_paths = {item.relative_path for item in plan}

    assert "test_source/extract_date=2026-05-11/run_id=abc/raw/data.json" in relative_paths
    assert "_manifests/bronze_runs.jsonl" in relative_paths


def test_bronze_syncer_syncs_files_to_local_backend(tmp_path):
    bronze_root = tmp_path / "lakehouse" / "bronze"

    raw_file = (
        bronze_root / "test_source" / "extract_date=2026-05-11" / "run_id=abc" / "raw" / "data.csv"
    )
    raw_file.parent.mkdir(parents=True)
    raw_file.write_text("a,b\n1,2\n", encoding="utf-8")

    backend = LocalStorageBackend(tmp_path / "target" / "bronze")
    syncer = BronzeSyncer(bronze_root=bronze_root, storage_backend=backend)

    results = syncer.sync(include_sources=["test_source"], include_manifests=False)

    assert len(results) == 1
    assert results[0].target_uri.endswith(
        "target/bronze/test_source/extract_date=2026-05-11/run_id=abc/raw/data.csv"
    )

    copied_file = (
        tmp_path
        / "target"
        / "bronze"
        / "test_source"
        / "extract_date=2026-05-11"
        / "run_id=abc"
        / "raw"
        / "data.csv"
    )

    assert copied_file.exists()
    assert copied_file.read_text(encoding="utf-8") == "a,b\n1,2\n"


def test_bronze_syncer_dry_run_does_not_write(tmp_path):
    bronze_root = tmp_path / "lakehouse" / "bronze"

    raw_file = (
        bronze_root / "test_source" / "extract_date=2026-05-11" / "run_id=abc" / "raw" / "data.csv"
    )
    raw_file.parent.mkdir(parents=True)
    raw_file.write_text("a,b\n1,2\n", encoding="utf-8")

    backend = LocalStorageBackend(tmp_path / "target" / "bronze")
    syncer = BronzeSyncer(bronze_root=bronze_root, storage_backend=backend)

    results = syncer.sync(
        include_sources=["test_source"],
        include_manifests=False,
        dry_run=True,
    )

    assert len(results) == 1
    assert results[0].dry_run is True
    assert not (
        tmp_path
        / "target"
        / "bronze"
        / "test_source"
        / "extract_date=2026-05-11"
        / "run_id=abc"
        / "raw"
        / "data.csv"
    ).exists()
