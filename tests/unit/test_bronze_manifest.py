import json

from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.source_registry import SourceRegistry


def test_bronze_writer_appends_manifest_record(tmp_path):
    registry = SourceRegistry()
    source = registry.get_source("canadian_disaster_database")

    writer = BronzeWriter(tmp_path / "bronze")

    result = writer.write_text(
        source=source,
        filename="sample.csv",
        content="event_id,event_date,province\n1,2020-01-01,BC\n",
        row_count=1,
    )

    assert result.manifest_path.exists()

    lines = result.manifest_path.read_text(encoding="utf-8").strip().splitlines()

    assert len(lines) == 1

    manifest_record = json.loads(lines[0])

    assert manifest_record["run_id"] == result.run_id
    assert manifest_record["source_name"] == "canadian_disaster_database"
    assert manifest_record["source_group"] == "national"
    assert manifest_record["raw_file_path"].endswith("sample.csv")
    assert manifest_record["metadata_path"].endswith("metadata.json")
    assert manifest_record["file_checksum"] == result.file_checksum
    assert manifest_record["row_count"] == 1
    assert manifest_record["target_bronze_table"] == "bronze_disaster_events"
    assert manifest_record["target_silver_table"] == "silver_disaster_event_month"
    assert manifest_record["load_status"] == "success"
    assert manifest_record["manifest_record_created_at"]


def test_bronze_writer_appends_multiple_manifest_records(tmp_path):
    registry = SourceRegistry()
    source = registry.get_source("eccc_historical_climate")

    writer = BronzeWriter(tmp_path / "bronze")

    writer.write_text(
        source=source,
        filename="sample_1.csv",
        content="station_id,date,value\n1,2020-01-01,10\n",
        row_count=1,
    )

    writer.write_text(
        source=source,
        filename="sample_2.csv",
        content="station_id,date,value\n2,2020-01-02,20\n",
        row_count=1,
    )

    lines = writer.manifest_path.read_text(encoding="utf-8").strip().splitlines()

    assert len(lines) == 2

    records = [json.loads(line) for line in lines]

    assert records[0]["file_name"] == "sample_1.csv"
    assert records[1]["file_name"] == "sample_2.csv"
    assert records[0]["run_id"] != records[1]["run_id"]
