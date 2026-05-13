import json

from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.municipal_sources import (
    MunicipalDownloadPlan,
    MunicipalDownloadResult,
)
from src.ingestion.municipal_bronze_ingestion import MunicipalBronzeIngestor
from src.ingestion.source_registry import SourceRegistry


class DummyMunicipalDownloader:
    def __init__(self):
        self.sources = []

    def download_source(self, source_name):
        self.sources.append(source_name)

        plan = MunicipalDownloadPlan(
            source_name=source_name,
            display_name="Calgary Building Permits",
            provider="City of Calgary",
            portal_type="socrata",
            dataset_id="c2es-76ed",
            export_format="csv",
            download_url="https://data.calgary.ca/resource/c2es-76ed.csv",
            target_bronze_table="bronze_calgary_building_permits",
            suggested_raw_filename="calgary_building_permits_raw.csv",
            implemented=True,
            paginated=True,
            page_limit=50000,
        )

        download = HttpDownloadResult(
            url=plan.download_url,
            final_url=plan.download_url,
            status_code=200,
            content_type="text/csv",
            content=b"id,value\n1,10\n",
            size_bytes=14,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )

        return MunicipalDownloadResult(
            plan=plan,
            download=download,
            extra_metadata={
                "row_count_validation_supported": True,
                "socrata_expected_row_count": 1,
                "socrata_actual_row_count": 1,
                "socrata_pages_downloaded": 1,
                "socrata_row_count_validation_passed": True,
            },
        )


def test_municipal_bronze_ingestor_writes_raw_and_metadata(tmp_path):
    registry = SourceRegistry()
    writer = BronzeWriter(tmp_path / "bronze")
    dummy_downloader = DummyMunicipalDownloader()

    ingestor = MunicipalBronzeIngestor(
        registry=registry,
        writer=writer,
        downloader=dummy_downloader,
    )

    result = ingestor.ingest_source("calgary_building_permits")

    assert result.source_name == "calgary_building_permits"
    assert result.portal_type == "socrata"
    assert result.dataset_id == "c2es-76ed"
    assert result.bronze_result.raw_file_path.exists()
    assert result.bronze_result.metadata_path.exists()
    assert result.bronze_result.manifest_path.exists()

    metadata = json.loads(result.bronze_result.metadata_path.read_text(encoding="utf-8"))

    assert metadata["source_name"] == "calgary_building_permits"
    assert metadata["target_bronze_table"] == "bronze_calgary_building_permits"
    assert metadata["ingestion_method"] == "socrata_dataset_export"
    assert metadata["row_count"] == 1
    assert metadata["extra_metadata"]["municipal_portal_type"] == "socrata"
    assert metadata["extra_metadata"]["municipal_dataset_id"] == "c2es-76ed"
    assert metadata["extra_metadata"]["municipal_export_format"] == "csv"
    assert metadata["extra_metadata"]["municipal_paginated"] is True
    assert metadata["extra_metadata"]["municipal_page_limit"] == 50000
    assert metadata["extra_metadata"]["socrata_expected_row_count"] == 1
    assert metadata["extra_metadata"]["socrata_actual_row_count"] == 1
    assert metadata["extra_metadata"]["socrata_pages_downloaded"] == 1
    assert metadata["extra_metadata"]["socrata_row_count_validation_passed"] is True
    assert metadata["extra_metadata"]["download_status_code"] == 200

    manifest_lines = result.bronze_result.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(manifest_lines) == 1

    manifest_record = json.loads(manifest_lines[0])
    assert manifest_record["source_name"] == "calgary_building_permits"
    assert manifest_record["row_count"] == 1
    assert manifest_record["load_status"] == "success"
