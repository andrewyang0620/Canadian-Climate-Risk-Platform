import json

import pytest

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.open_canada import (
    OpenCanadaDownloader,
    OpenCanadaDownloaderError,
)


class DummyHttpDownloader:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None):
        self.calls.append({"url": url, "params": params})

        if not self.responses:
            raise AssertionError("No dummy responses left.")

        return self.responses.pop(0)


def make_http_result(url, content, content_type="application/json"):
    return HttpDownloadResult(
        url=url,
        final_url=url,
        status_code=200,
        content_type=content_type,
        content=content,
        size_bytes=len(content),
        checksum="checksum",
        downloaded_at="2026-04-29T00:00:00+00:00",
    )


def test_open_canada_fetch_package_and_select_resource():
    package_payload = {
        "success": True,
        "result": {
            "title": "Canadian Disaster Database",
            "resources": [
                {
                    "id": "resource-html",
                    "name": "HTML landing page",
                    "url": "https://example.com/cdd.html",
                    "format": "HTML",
                    "language": ["eng"],
                },
                {
                    "id": "resource-csv",
                    "name": "CDD CSV",
                    "url": "https://example.com/cdd.csv",
                    "format": "CSV",
                    "language": ["eng"],
                },
            ],
        },
    }

    http = DummyHttpDownloader(
        [
            make_http_result(
                "https://open.canada.ca/data/api/action/package_show",
                json.dumps(package_payload).encode("utf-8"),
            )
        ]
    )

    downloader = OpenCanadaDownloader(http_downloader=http)
    package = downloader.fetch_package(
        api_url="https://open.canada.ca/data/api/action/package_show",
        dataset_id="dataset-123",
    )

    selected = downloader.select_resource(package, ["CSV"])

    assert package.dataset_id == "dataset-123"
    assert package.title == "Canadian Disaster Database"
    assert selected.resource_id == "resource-csv"
    assert selected.url == "https://example.com/cdd.csv"
    assert selected.format == "CSV"


def test_open_canada_download_resource():
    package_payload = {
        "success": True,
        "result": {
            "title": "Canadian Disaster Database",
            "resources": [
                {
                    "id": "resource-csv",
                    "name": "CDD CSV",
                    "url": "https://example.com/cdd.csv",
                    "format": "CSV",
                    "language": ["eng"],
                }
            ],
        },
    }

    http = DummyHttpDownloader(
        [
            make_http_result(
                "https://open.canada.ca/data/api/action/package_show",
                json.dumps(package_payload).encode("utf-8"),
            ),
            make_http_result(
                "https://example.com/cdd.csv",
                b"event_id,event_date,province\n1,2020-01-01,BC\n",
                content_type="text/csv",
            ),
        ]
    )

    downloader = OpenCanadaDownloader(http_downloader=http)
    result = downloader.download_resource(
        api_url="https://open.canada.ca/data/api/action/package_show",
        dataset_id="dataset-123",
        preferred_formats=["CSV"],
    )

    assert result.package.title == "Canadian Disaster Database"
    assert result.resource.format == "CSV"
    assert result.download.content.startswith(b"event_id")
    assert http.calls[0]["params"] == {"id": "dataset-123"}
    assert http.calls[1]["url"] == "https://example.com/cdd.csv"


def test_open_canada_raises_when_no_preferred_format():
    package_payload = {
        "success": True,
        "result": {
            "title": "Canadian Disaster Database",
            "resources": [
                {
                    "id": "resource-html",
                    "name": "CDD HTML",
                    "url": "https://example.com/cdd.html",
                    "format": "HTML",
                }
            ],
        },
    }

    http = DummyHttpDownloader(
        [
            make_http_result(
                "https://open.canada.ca/data/api/action/package_show",
                json.dumps(package_payload).encode("utf-8"),
            )
        ]
    )

    downloader = OpenCanadaDownloader(http_downloader=http)
    package = downloader.fetch_package(
        api_url="https://open.canada.ca/data/api/action/package_show",
        dataset_id="dataset-123",
    )

    with pytest.raises(OpenCanadaDownloaderError):
        downloader.select_resource(package, ["CSV"])


def test_open_canada_raises_on_unsuccessful_payload():
    payload = {"success": False, "result": {}}

    http = DummyHttpDownloader(
        [
            make_http_result(
                "https://open.canada.ca/data/api/action/package_show",
                json.dumps(payload).encode("utf-8"),
            )
        ]
    )

    downloader = OpenCanadaDownloader(http_downloader=http)

    with pytest.raises(OpenCanadaDownloaderError):
        downloader.fetch_package(
            api_url="https://open.canada.ca/data/api/action/package_show",
            dataset_id="dataset-123",
        )
