from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from src.utils.checksum import compute_bytes_checksum
from src.utils.time import utc_now_iso


class HttpDownloaderError(Exception):
    """Raised when an HTTP download fails."""


@dataclass(frozen=True)
class HttpDownloadResult:
    """Result returned by an HTTP download."""

    url: str
    final_url: str
    status_code: int
    content_type: str | None
    content: bytes
    size_bytes: int
    checksum: str
    downloaded_at: str

    def text(self, encoding: str = "utf-8") -> str:
        """Decode response bytes as text."""
        return self.content.decode(encoding)


class HttpDownloader:
    """Small HTTP downloader wrapper used by ingestion clients."""

    def __init__(
        self,
        session: requests.Session | None = None,
        user_agent: str = "Canadian-Climate-Risk-Platform/0.1",
    ) -> None:
        self.session = session or requests.Session()
        self.user_agent = user_agent

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_seconds: int = 60,
    ) -> HttpDownloadResult:
        """Download bytes from an HTTP endpoint."""
        self._validate_url(url)

        request_headers = {
            "User-Agent": self.user_agent,
        }

        if headers:
            request_headers.update(headers)

        try:
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=timeout_seconds,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            raise HttpDownloaderError(f"HTTP request failed for {url}: {exc}") from exc

        if response.status_code >= 400:
            raise HttpDownloaderError(
                f"HTTP request failed for {url}: status_code={response.status_code}"
            )

        content = response.content

        if not content:
            raise HttpDownloaderError(f"HTTP response is empty for {url}")

        content_type = response.headers.get("Content-Type")

        return HttpDownloadResult(
            url=url,
            final_url=str(response.url),
            status_code=response.status_code,
            content_type=content_type,
            content=content,
            size_bytes=len(content),
            checksum=compute_bytes_checksum(content),
            downloaded_at=utc_now_iso(),
        )

    @staticmethod
    def _validate_url(url: str) -> None:
        if not isinstance(url, str) or not url.strip():
            raise HttpDownloaderError("URL must be a non-empty string.")

        if not url.startswith(("http://", "https://")):
            raise HttpDownloaderError(f"Only HTTP/HTTPS URLs are supported: {url}")
