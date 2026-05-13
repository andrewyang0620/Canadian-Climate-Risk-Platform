from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from src.ingestion.downloaders.http_downloader import HttpDownloader, HttpDownloadResult


class OpenCanadaDownloaderError(Exception):
    """Raised when Open Canada metadata or resource download fails."""


@dataclass(frozen=True)
class OpenCanadaResource:
    """One downloadable resource from an Open Canada package."""

    name: str
    url: str
    format: str
    language: str | None
    resource_id: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class OpenCanadaPackage:
    """Open Canada package metadata."""

    dataset_id: str
    title: str
    resources: list[OpenCanadaResource]
    raw: dict[str, Any]


@dataclass(frozen=True)
class OpenCanadaDownloadResult:
    """Downloaded Open Canada resource plus selected metadata."""

    package: OpenCanadaPackage
    resource: OpenCanadaResource
    download: HttpDownloadResult


class OpenCanadaDownloader:
    """Downloader for Open Canada CKAN-style package metadata and resources."""

    def __init__(self, http_downloader: HttpDownloader | None = None) -> None:
        self.http_downloader = http_downloader or HttpDownloader()

    def fetch_package(
        self,
        *,
        api_url: str,
        dataset_id: str,
    ) -> OpenCanadaPackage:
        """Fetch Open Canada package metadata using package_show."""
        result = self.http_downloader.get(api_url, params={"id": dataset_id})

        try:
            payload = json.loads(result.content.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise OpenCanadaDownloaderError(
                f"Open Canada package response is not valid JSON for dataset_id={dataset_id}"
            ) from exc

        if not payload.get("success"):
            raise OpenCanadaDownloaderError(
                f"Open Canada package_show did not return success for dataset_id={dataset_id}"
            )

        package_raw = payload.get("result")
        if not isinstance(package_raw, dict):
            raise OpenCanadaDownloaderError(
                f"Open Canada package_show missing result object for dataset_id={dataset_id}"
            )

        resources_raw = package_raw.get("resources", [])
        if not isinstance(resources_raw, list) or not resources_raw:
            raise OpenCanadaDownloaderError(
                f"Open Canada package has no resources for dataset_id={dataset_id}"
            )

        resources: list[OpenCanadaResource] = []

        for item in resources_raw:
            if not isinstance(item, dict):
                continue

            url = item.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                continue

            resources.append(
                OpenCanadaResource(
                    name=str(item.get("name") or item.get("name_translated") or "unnamed_resource"),
                    url=url,
                    format=str(item.get("format") or "").upper(),
                    language=_extract_language(item),
                    resource_id=item.get("id"),
                    raw=item,
                )
            )

        if not resources:
            raise OpenCanadaDownloaderError(
                f"Open Canada package has no downloadable HTTP resources for dataset_id={dataset_id}"
            )

        return OpenCanadaPackage(
            dataset_id=dataset_id,
            title=str(
                package_raw.get("title") or package_raw.get("title_translated") or dataset_id
            ),
            resources=resources,
            raw=package_raw,
        )

    def select_resource(
        self,
        package: OpenCanadaPackage,
        preferred_formats: list[str],
    ) -> OpenCanadaResource:
        """Select the best resource from a package by preferred format order."""
        normalized_preferences = [fmt.upper() for fmt in preferred_formats]

        for preferred_format in normalized_preferences:
            for resource in package.resources:
                if preferred_format in resource.format:
                    return resource

        available = ", ".join(
            sorted({resource.format or "UNKNOWN" for resource in package.resources})
        )

        raise OpenCanadaDownloaderError(
            f"No resource matched preferred formats {normalized_preferences}. "
            f"Available formats: {available}"
        )

    def download_resource(
        self,
        *,
        api_url: str,
        dataset_id: str,
        preferred_formats: list[str],
    ) -> OpenCanadaDownloadResult:
        """Fetch package metadata, select a resource, and download its raw bytes."""
        package = self.fetch_package(api_url=api_url, dataset_id=dataset_id)
        resource = self.select_resource(package, preferred_formats)
        download = self.http_downloader.get(resource.url)

        return OpenCanadaDownloadResult(
            package=package,
            resource=resource,
            download=download,
        )


def _extract_language(item: dict[str, Any]) -> str | None:
    language = item.get("language")

    if isinstance(language, str):
        return language

    if isinstance(language, list) and language:
        return str(language[0])

    return None
