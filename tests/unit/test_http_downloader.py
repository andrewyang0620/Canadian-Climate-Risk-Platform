import pytest

from src.ingestion.downloaders.http_downloader import (
    HttpDownloader,
    HttpDownloaderError,
)


class DummyResponse:
    def __init__(
        self,
        url="https://example.com/final.csv",
        status_code=200,
        content=b"id,value\n1,10\n",
        headers=None,
    ):
        self.url = url
        self.status_code = status_code
        self.content = content
        self.headers = headers or {"Content-Type": "text/csv"}


class DummySession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, allow_redirects=True):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
                "allow_redirects": allow_redirects,
            }
        )
        return self.response


def test_http_downloader_get_success():
    response = DummyResponse()
    session = DummySession(response)
    downloader = HttpDownloader(session=session)

    result = downloader.get("https://example.com/data.csv", timeout_seconds=10)

    assert result.status_code == 200
    assert result.final_url == "https://example.com/final.csv"
    assert result.content_type == "text/csv"
    assert result.size_bytes == len(b"id,value\n1,10\n")
    assert result.checksum
    assert result.text() == "id,value\n1,10\n"

    assert session.calls[0]["url"] == "https://example.com/data.csv"
    assert session.calls[0]["timeout"] == 10
    assert session.calls[0]["headers"]["User-Agent"].startswith("Canadian-Climate-Risk-Platform")


def test_http_downloader_rejects_invalid_url():
    downloader = HttpDownloader(session=DummySession(DummyResponse()))

    with pytest.raises(HttpDownloaderError):
        downloader.get("ftp://example.com/data.csv")


def test_http_downloader_raises_on_http_error():
    response = DummyResponse(status_code=404, content=b"not found")
    downloader = HttpDownloader(session=DummySession(response))

    with pytest.raises(HttpDownloaderError):
        downloader.get("https://example.com/missing.csv")


def test_http_downloader_raises_on_empty_content():
    response = DummyResponse(content=b"")
    downloader = HttpDownloader(session=DummySession(response))

    with pytest.raises(HttpDownloaderError):
        downloader.get("https://example.com/empty.csv")
