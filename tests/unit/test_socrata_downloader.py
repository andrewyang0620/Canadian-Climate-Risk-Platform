import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.socrata import SocrataDownloader, SocrataDownloaderError


class DummyHttpDownloader:
    def __init__(self, responses=None):
        self.urls = []
        self.calls = []
        self.responses = list(responses or [])

    def get(self, url, params=None):
        self.urls.append(url)
        self.calls.append({"url": url, "params": params})

        if self.responses:
            content = self.responses.pop(0)
        else:
            content = b"id,value\n1,10\n"

        return HttpDownloadResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="text/csv",
            content=content,
            size_bytes=len(content),
            checksum="checksum",
            downloaded_at="2026-04-29T00:00:00+00:00",
        )


def test_socrata_build_export_plan_csv_is_paginated():
    downloader = SocrataDownloader(default_page_limit=50000)

    plan = downloader.build_export_plan(
        domain="data.calgary.ca",
        dataset_id="c2es-76ed",
        export_format="csv",
    )

    assert plan.download_url == "https://data.calgary.ca/resource/c2es-76ed.csv"
    assert plan.suggested_filename == "c2es-76ed_raw.csv"
    assert plan.paginated is True
    assert plan.page_limit == 50000


def test_socrata_build_export_plan_geojson_is_not_paginated():
    downloader = SocrataDownloader()

    plan = downloader.build_export_plan(
        domain="data.calgary.ca",
        dataset_id="3q69-wm6a",
        export_format="geojson",
    )

    assert plan.download_url == "https://data.calgary.ca/resource/3q69-wm6a.geojson"
    assert plan.suggested_filename == "3q69-wm6a_raw.geojson"
    assert plan.paginated is False
    assert plan.page_limit is None


def test_socrata_download_dataset_uses_csv_pagination():
    page_1 = b"id,value\n1,10\n2,20\n"
    page_2 = b"id,value\n3,30\n"

    dummy_http = DummyHttpDownloader(responses=[page_1, page_2])
    downloader = SocrataDownloader(
        http_downloader=dummy_http,
        default_page_limit=2,
    )

    plan, result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="4bsw-nn7w",
        export_format="csv",
    )

    assert plan.paginated is True
    assert result.status_code == 200
    assert result.content.decode("utf-8") == "id,value\n1,10\n2,20\n3,30\n"

    assert dummy_http.calls[0]["params"] == {"$limit": 2, "$offset": 0}
    assert dummy_http.calls[1]["params"] == {"$limit": 2, "$offset": 2}


def test_socrata_pagination_removes_duplicate_headers():
    page_1 = b"id,value\n1,10\n"
    page_2 = b"id,value\n"

    dummy_http = DummyHttpDownloader(responses=[page_1, page_2])
    downloader = SocrataDownloader(
        http_downloader=dummy_http,
        default_page_limit=1,
    )

    _, result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="c2es-76ed",
        export_format="csv",
    )

    assert result.content.decode("utf-8") == "id,value\n1,10\n"


def test_socrata_pagination_raises_on_header_change():
    page_1 = b"id,value\n1,10\n"
    page_2 = b"id,changed_value\n2,20\n"

    dummy_http = DummyHttpDownloader(responses=[page_1, page_2])
    downloader = SocrataDownloader(
        http_downloader=dummy_http,
        default_page_limit=1,
    )

    with pytest.raises(SocrataDownloaderError):
        downloader.download_dataset(
            domain="data.calgary.ca",
            dataset_id="c2es-76ed",
            export_format="csv",
        )


def test_socrata_rejects_invalid_domain():
    downloader = SocrataDownloader()

    with pytest.raises(SocrataDownloaderError):
        downloader.build_export_plan(
            domain="https://data.calgary.ca",
            dataset_id="x",
            export_format="csv",
        )
