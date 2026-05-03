import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.socrata import SocrataDownloader, SocrataDownloaderError


class DummyHttpDownloader:
    def __init__(self, responses=None, fail_on_page_once=False):
        self.urls = []
        self.calls = []
        self.responses = list(responses or [])
        self.fail_on_page_once = fail_on_page_once
        self.failed_once = False

    def get(self, url, params=None):
        self.urls.append(url)
        self.calls.append({"url": url, "params": params})

        if self.fail_on_page_once and params and "$limit" in params and not self.failed_once:
            self.failed_once = True
            raise RuntimeError("dummy page failure")

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


def test_socrata_download_dataset_uses_csv_pagination_and_row_count_validation():
    count_response = b'[{"count":"3"}]'
    page_1 = b'id,value\n1,10\n2,"hello\nworld"\n'
    page_2 = b"id,value\n3,30\n"

    dummy_http = DummyHttpDownloader(responses=[count_response, page_1, page_2])
    downloader = SocrataDownloader(http_downloader=dummy_http, default_page_limit=2)

    result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="4bsw-nn7w",
        export_format="csv",
    )

    assert result.expected_row_count == 3
    assert result.actual_row_count == 3
    assert result.pages_downloaded == 2
    assert result.row_count_validation_supported is True
    assert result.row_count_validation_passed is True
    assert result.page_limits_used == (2, 2)

    assert dummy_http.calls[0]["url"] == "https://data.calgary.ca/resource/4bsw-nn7w.json"
    assert dummy_http.calls[0]["params"] == {"$select": "count(*)"}
    assert dummy_http.calls[1]["params"] == {"$limit": 2, "$offset": 0}
    assert dummy_http.calls[2]["params"] == {"$limit": 2, "$offset": 2}


def test_socrata_count_failure_does_not_block_paginated_download():
    page_1 = b"id,value\n1,10\n"
    page_2 = b"id,value\n"

    class CountFailHttp(DummyHttpDownloader):
        def get(self, url, params=None):
            if params == {"$select": "count(*)"}:
                raise RuntimeError("count failed")
            return super().get(url, params=params)

    dummy_http = CountFailHttp(responses=[page_1, page_2])
    downloader = SocrataDownloader(http_downloader=dummy_http, default_page_limit=1)

    result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="6933-unw5",
        export_format="csv",
    )

    assert result.expected_row_count is None
    assert result.actual_row_count == 1
    assert result.row_count_validation_supported is False
    assert result.row_count_validation_passed is None
    assert result.row_count_validation_error


def test_socrata_raises_on_row_count_mismatch():
    count_response = b'[{"count":"3"}]'
    page_1 = b"id,value\n1,10\n"
    page_2 = b"id,value\n"

    dummy_http = DummyHttpDownloader(responses=[count_response, page_1, page_2])
    downloader = SocrataDownloader(http_downloader=dummy_http, default_page_limit=1)

    with pytest.raises(SocrataDownloaderError):
        downloader.download_dataset(
            domain="data.calgary.ca",
            dataset_id="c2es-76ed",
            export_format="csv",
        )


def test_socrata_reduces_page_limit_after_page_failure():
    count_response = b'[{"count":"1"}]'
    page_1 = b"id,value\n1,10\n"

    dummy_http = DummyHttpDownloader(
        responses=[count_response, page_1],
        fail_on_page_once=True,
    )
    downloader = SocrataDownloader(
        http_downloader=dummy_http,
        default_page_limit=4,
        min_page_limit=1,
    )

    result = downloader.download_dataset(
        domain="data.calgary.ca",
        dataset_id="c2es-76ed",
        export_format="csv",
    )

    assert result.actual_row_count == 1
    assert result.page_limits_used == (2,)


def test_socrata_rejects_invalid_domain():
    downloader = SocrataDownloader()

    with pytest.raises(SocrataDownloaderError):
        downloader.build_export_plan(
            domain="https://data.calgary.ca",
            dataset_id="x",
            export_format="csv",
        )
