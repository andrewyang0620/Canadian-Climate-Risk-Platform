import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.national_sources import (
    NationalSourceDownloader,
    NationalSourceDownloaderError,
)
from src.ingestion.downloaders.open_canada import (
    OpenCanadaDownloadResult,
    OpenCanadaPackage,
    OpenCanadaResource,
)
from src.ingestion.source_registry import SourceRegistry


class DummyHttpDownloader:
    def __init__(self):
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return HttpDownloadResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            content=b"<html>ok</html>",
            size_bytes=len(b"<html>ok</html>"),
            checksum="dummy-checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )


class DummyOpenCanadaDownloader:
    def __init__(self):
        self.calls = []

    def download_resource(self, api_url, dataset_id, preferred_formats):
        self.calls.append(
            {
                "api_url": api_url,
                "dataset_id": dataset_id,
                "preferred_formats": preferred_formats,
            }
        )

        package = OpenCanadaPackage(
            dataset_id=dataset_id,
            title="Canadian Disaster Database",
            resources=[],
            raw={},
        )

        resource = OpenCanadaResource(
            name="CDD CSV",
            url="https://example.com/cdd.csv",
            format="CSV",
            language="eng",
            resource_id="resource-csv",
            raw={},
        )

        download = HttpDownloadResult(
            url="https://example.com/cdd.csv",
            final_url="https://example.com/cdd.csv",
            status_code=200,
            content_type="text/csv",
            content=b"event_id,event_date,province\n1,2020-01-01,BC\n",
            size_bytes=42,
            checksum="dummy-checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )

        return OpenCanadaDownloadResult(
            package=package,
            resource=resource,
            download=download,
        )


def test_national_source_downloader_lists_national_sources():
    downloader = NationalSourceDownloader()

    sources = downloader.list_sources()

    assert "eccc_historical_climate" in sources
    assert "hydat_archive" in sources
    assert "canadian_disaster_database" in sources
    assert "vancouver_property_parcels" not in sources


def test_national_source_downloader_builds_plan():
    downloader = NationalSourceDownloader()

    plan = downloader.build_plan("canadian_disaster_database")

    assert plan.source_name == "canadian_disaster_database"
    assert plan.source_url.startswith("http")
    assert plan.target_bronze_table == "bronze_disaster_events"
    assert plan.suggested_raw_filename.startswith("canadian_disaster_database_raw")
    assert plan.implemented is True


def test_national_source_specific_plan_methods():
    downloader = NationalSourceDownloader()

    assert downloader.plan_eccc_historical_climate().source_name == "eccc_historical_climate"
    assert downloader.plan_eccc_hydrometric_realtime().source_name == "eccc_hydrometric_realtime"
    assert downloader.plan_hydat_archive().source_name == "hydat_archive"
    assert downloader.plan_wildfire_history().source_name == "wildfire_history"
    assert downloader.plan_statcan_building_permits().source_name == "statcan_building_permits"
    assert downloader.plan_census_boundaries().source_name == "census_boundaries"
    assert downloader.plan_canadian_disaster_database().source_name == "canadian_disaster_database"


def test_national_source_downloader_rejects_municipal_source():
    downloader = NationalSourceDownloader()

    with pytest.raises(NationalSourceDownloaderError):
        downloader.build_plan("vancouver_property_parcels")


def test_probe_source_landing_page_uses_http_downloader():
    dummy_http = DummyHttpDownloader()
    downloader = NationalSourceDownloader(http_downloader=dummy_http)

    result = downloader.probe_source_landing_page("canadian_disaster_database")

    registry = SourceRegistry()
    expected_url = registry.get_source("canadian_disaster_database").source_url

    assert result.status_code == 200
    assert dummy_http.urls == [expected_url]


def test_download_canadian_disaster_database_uses_open_canada_metadata():
    dummy_open_canada = DummyOpenCanadaDownloader()
    downloader = NationalSourceDownloader(open_canada_downloader=dummy_open_canada)

    result = downloader.download_canadian_disaster_database()

    assert result.package.title == "Canadian Disaster Database"
    assert result.resource.format == "CSV"
    assert result.download.content.startswith(b"event_id")

    call = dummy_open_canada.calls[0]
    assert call["dataset_id"] == "1c3d15f9-9cfa-4010-8462-0d67e493d9b9"
    assert "CSV" in call["preferred_formats"]
