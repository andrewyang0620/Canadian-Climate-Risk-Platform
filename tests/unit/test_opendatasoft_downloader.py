import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.opendatasoft import (
    OpenDataSoftDownloader,
    OpenDataSoftDownloaderError,
)


class DummyHttpDownloader:
    def __init__(self):
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return HttpDownloadResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="application/json",
            content=b'{"type":"FeatureCollection","features":[]}',
            size_bytes=42,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )


def test_opendatasoft_build_export_plan_geojson():
    downloader = OpenDataSoftDownloader()

    plan = downloader.build_export_plan(
        base_url="https://opendata.vancouver.ca",
        dataset_id="property-parcel-polygons",
        export_format="geojson",
    )

    assert plan.download_url == (
        "https://opendata.vancouver.ca/api/explore/v2.1/catalog/"
        "datasets/property-parcel-polygons/exports/geojson"
    )
    assert plan.suggested_filename == "property-parcel-polygons_raw.geojson"


def test_opendatasoft_download_dataset():
    dummy_http = DummyHttpDownloader()
    downloader = OpenDataSoftDownloader(http_downloader=dummy_http)

    plan, result = downloader.download_dataset(
        base_url="https://opendata.vancouver.ca",
        dataset_id="designated-floodplain",
        export_format="geojson",
    )

    assert plan.dataset_id == "designated-floodplain"
    assert result.status_code == 200
    assert dummy_http.urls == [plan.download_url]


def test_opendatasoft_rejects_unsupported_format():
    downloader = OpenDataSoftDownloader()

    with pytest.raises(OpenDataSoftDownloaderError):
        downloader.build_export_plan(
            base_url="https://opendata.vancouver.ca",
            dataset_id="x",
            export_format="xlsx",
        )
