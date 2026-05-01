import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.socrata import SocrataDownloader, SocrataDownloaderError


class DummyHttpDownloader:
    def __init__(self):
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return HttpDownloadResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="text/csv",
            content=b"id,value\n1,10\n",
            size_bytes=14,
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )


def test_socrata_build_export_plan_csv():
    downloader = SocrataDownloader()

    plan = downloader.build_export_plan(
        domain="data.calgary.ca",
        dataset_id="c2es-76ed",
        export_format="csv",
    )

    assert plan.download_url == "https://data.calgary.ca/resource/c2es-76ed.csv"
    assert plan.suggested_filename == "c2es-76ed_raw.csv"


def test_socrata_build_export_plan_geojson():
    downloader = SocrataDownloader()

    plan = downloader.build_export_plan(
        domain="data.calgary.ca",
        dataset_id="3q69-wm6a",
        export_format="geojson",
    )

    assert plan.download_url == "https://data.calgary.ca/resource/3q69-wm6a.geojson"
    assert plan.suggested_filename == "3q69-wm6a_raw.geojson"


def test_socrata_download_dataset():
    dummy_http = DummyHttpDownloader()
    downloader = SocrataDownloader(http_downloader=dummy_http)

    plan, result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="4bsw-nn7w",
        export_format="csv",
    )

    assert result.status_code == 200
    assert dummy_http.urls == [plan.download_url]


def test_socrata_rejects_invalid_domain():
    downloader = SocrataDownloader()

    with pytest.raises(SocrataDownloaderError):
        downloader.build_export_plan(
            domain="https://data.calgary.ca",
            dataset_id="x",
            export_format="csv",
        )
