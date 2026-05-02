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
            display_name="Vancouver Property Parcel Polygons",
            provider="City of Vancouver",
            portal_type="opendatasoft",
            dataset_id="property-parcel-polygons",
            export_format="geojson",
            download_url=(
                "https://opendata.vancouver.ca/api/explore/v2.1/catalog/"
                "datasets/property-parcel-polygons/exports/geojson"
            ),
            target_bronze_table="bronze_vancouver_property_parcels",
            suggested_raw_filename="vancouver_property_parcels_raw.geojson",
            implemented=True,
            paginated=False,
            page_limit=None,
        )

        download = HttpDownloadResult(
            url=plan.download_url,
            final_url=plan.download_url,
            status_code=200,
            content_type="application/geo+json",
            content=b'{"type":"FeatureCollection","features":[]}',
            size_bytes=42,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )

        return MunicipalDownloadResult(plan=plan, download=download)


def test_municipal_bronze_ingestor_writes_raw_and_metadata(tmp_path):
    registry = SourceRegistry()
    writer = BronzeWriter(tmp_path / "bronze")
    dummy_downloader = DummyMunicipalDownloader()

    ingestor = MunicipalBronzeIngestor(
        registry=registry,
        writer=writer,
        downloader=dummy_downloader,
    )

    result = ingestor.ingest_source("vancouver_property_parcels")

    assert result.source_name == "vancouver_property_parcels"
    assert result.portal_type == "opendatasoft"
    assert result.dataset_id == "property-parcel-polygons"
    assert result.bronze_result.raw_file_path.exists()
    assert result.bronze_result.metadata_path.exists()
    assert result.bronze_result.manifest_path.exists()

    metadata = json.loads(result.bronze_result.metadata_path.read_text(encoding="utf-8"))

    assert metadata["source_name"] == "vancouver_property_parcels"
    assert metadata["target_bronze_table"] == "bronze_vancouver_property_parcels"
    assert metadata["ingestion_method"] == "opendatasoft_dataset_export"
    assert metadata["extra_metadata"]["municipal_portal_type"] == "opendatasoft"
    assert metadata["extra_metadata"]["municipal_dataset_id"] == "property-parcel-polygons"
    assert metadata["extra_metadata"]["municipal_export_format"] == "geojson"
    assert metadata["extra_metadata"]["download_status_code"] == 200
    assert metadata["extra_metadata"]["municipal_paginated"] is False
    assert metadata["extra_metadata"]["municipal_page_limit"] is None

    manifest_lines = result.bronze_result.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(manifest_lines) == 1

    manifest_record = json.loads(manifest_lines[0])
    assert manifest_record["source_name"] == "vancouver_property_parcels"
    assert manifest_record["load_status"] == "success"
