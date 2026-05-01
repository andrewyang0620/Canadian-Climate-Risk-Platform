import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.municipal_sources import (
    MunicipalSourceDownloader,
    MunicipalSourceDownloaderError,
)


class DummyOpenDataSoftDownloader:
    def build_export_plan(self, base_url, dataset_id, export_format):
        from src.ingestion.downloaders.opendatasoft import OpenDataSoftDownloadPlan

        return OpenDataSoftDownloadPlan(
            base_url=base_url,
            dataset_id=dataset_id,
            export_format=export_format,
            download_url=f"{base_url}/exports/{dataset_id}.{export_format}",
            suggested_filename=f"{dataset_id}_raw.{export_format}",
        )

    def download_dataset(self, base_url, dataset_id, export_format):
        plan = self.build_export_plan(base_url, dataset_id, export_format)
        result = HttpDownloadResult(
            url=plan.download_url,
            final_url=plan.download_url,
            status_code=200,
            content_type="application/json",
            content=b"{}",
            size_bytes=2,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )
        return plan, result


class DummySocrataDownloader:
    def build_export_plan(self, domain, dataset_id, export_format):
        from src.ingestion.downloaders.socrata import SocrataDownloadPlan

        return SocrataDownloadPlan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=export_format,
            download_url=f"https://{domain}/resource/{dataset_id}.{export_format}",
            suggested_filename=f"{dataset_id}_raw.{export_format}",
        )

    def download_dataset(self, domain, dataset_id, export_format):
        plan = self.build_export_plan(domain, dataset_id, export_format)
        result = HttpDownloadResult(
            url=plan.download_url,
            final_url=plan.download_url,
            status_code=200,
            content_type="text/csv",
            content=b"id,value\n1,10\n",
            size_bytes=14,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )
        return plan, result


def make_downloader():
    return MunicipalSourceDownloader(
        opendatasoft_downloader=DummyOpenDataSoftDownloader(),
        socrata_downloader=DummySocrataDownloader(),
    )


def test_municipal_source_downloader_lists_sources():
    downloader = make_downloader()

    sources = downloader.list_sources()

    assert "vancouver_property_parcels" in sources
    assert "vancouver_floodplain" in sources
    assert "calgary_property_assessment" in sources
    assert "calgary_building_permits" in sources
    assert "eccc_historical_climate" not in sources


def test_municipal_source_downloader_builds_vancouver_plan():
    downloader = make_downloader()

    plan = downloader.build_plan("vancouver_property_parcels")

    assert plan.source_name == "vancouver_property_parcels"
    assert plan.portal_type == "opendatasoft"
    assert plan.dataset_id == "property-parcel-polygons"
    assert plan.export_format == "geojson"
    assert plan.target_bronze_table == "bronze_vancouver_property_parcels"
    assert plan.implemented is True


def test_municipal_source_downloader_builds_calgary_plan():
    downloader = make_downloader()

    plan = downloader.build_plan("calgary_building_permits")

    assert plan.source_name == "calgary_building_permits"
    assert plan.portal_type == "socrata"
    assert plan.dataset_id == "c2es-76ed"
    assert plan.export_format == "csv"
    assert plan.target_bronze_table == "bronze_calgary_building_permits"
    assert plan.implemented is True


def test_municipal_source_downloader_rejects_national_source():
    downloader = make_downloader()

    with pytest.raises(MunicipalSourceDownloaderError):
        downloader.build_plan("canadian_disaster_database")


def test_municipal_source_downloader_downloads_source():
    downloader = make_downloader()

    result = downloader.download_source("vancouver_floodplain")

    assert result.plan.source_name == "vancouver_floodplain"
    assert result.download.status_code == 200
    assert result.download.content
