import json

import pytest

from src.ingestion.bronze_writer import BronzeWriter, BronzeWriterError
from src.ingestion.source_registry import SourceRegistry


def test_bronze_writer_writes_raw_file_and_metadata(tmp_path):
    registry = SourceRegistry()
    source = registry.get_source("canadian_disaster_database")

    writer = BronzeWriter(tmp_path / "bronze")

    result = writer.write_text(
        source=source,
        filename="sample.csv",
        content="event_id,event_date,province\n1,2020-01-01,BC\n",
        row_count=1,
        source_period_start="2020-01-01",
        source_period_end="2020-12-31",
    )

    assert result.raw_file_path.exists()
    assert result.metadata_path.exists()
    assert result.load_status == "success"

    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert metadata["run_id"] == result.run_id
    assert metadata["source_name"] == "canadian_disaster_database"
    assert metadata["source_url"].startswith("http")
    assert metadata["raw_file_path"].endswith("sample.csv")
    assert metadata["file_checksum"] == result.file_checksum
    assert metadata["row_count"] == 1
    assert metadata["load_status"] == "success"
    assert metadata["target_bronze_table"] == "bronze_disaster_events"


def test_bronze_writer_rejects_empty_content(tmp_path):
    registry = SourceRegistry()
    source = registry.get_source("eccc_historical_climate")

    writer = BronzeWriter(tmp_path / "bronze")

    with pytest.raises(BronzeWriterError):
        writer.write_bytes(source=source, filename="empty.csv", content=b"")


def test_bronze_writer_rejects_path_traversal_filename(tmp_path):
    registry = SourceRegistry()
    source = registry.get_source("eccc_historical_climate")

    writer = BronzeWriter(tmp_path / "bronze")

    with pytest.raises(BronzeWriterError):
        writer.write_text(
            source=source,
            filename="../bad.csv",
            content="bad",
        )


def test_bronze_writer_builds_expected_folder_structure(tmp_path):
    writer = BronzeWriter(tmp_path / "bronze")

    paths = writer.build_run_paths(
        source_name="eccc_historical_climate",
        run_id="test-run",
        extract_date="2026-04-29",
    )

    expected_suffix = "bronze/eccc_historical_climate/" "extract_date=2026-04-29/run_id=test-run"

    assert paths.run_dir.as_posix().endswith(expected_suffix)
    assert paths.raw_dir.name == "raw"
    assert paths.metadata_path.name == "metadata.json"
