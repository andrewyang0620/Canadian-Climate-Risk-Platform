from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.downloaders.http_downloader import HttpDownloader, HttpDownloadResult


class OpenDataSoftDownloaderError(Exception):
    """Raised when an OpenDataSoft download cannot be planned or executed."""


@dataclass(frozen=True)
class OpenDataSoftDownloadPlan:
    """Download plan for one OpenDataSoft dataset export."""

    base_url: str
    dataset_id: str
    export_format: str
    download_url: str
    suggested_filename: str


class OpenDataSoftDownloader:
    """Downloader for OpenDataSoft dataset exports."""

    SUPPORTED_FORMATS = {"csv", "json", "geojson"}

    def __init__(self, http_downloader: HttpDownloader | None = None) -> None:
        self.http_downloader = http_downloader or HttpDownloader()

    def build_export_plan(
        self,
        *,
        base_url: str,
        dataset_id: str,
        export_format: str,
    ) -> OpenDataSoftDownloadPlan:
        """Build a dataset export URL."""
        normalized_format = export_format.lower().strip()

        if normalized_format not in self.SUPPORTED_FORMATS:
            raise OpenDataSoftDownloaderError(
                f"Unsupported OpenDataSoft export format: {export_format}"
            )

        if not base_url.startswith(("http://", "https://")):
            raise OpenDataSoftDownloaderError(f"Invalid OpenDataSoft base_url: {base_url}")

        if not dataset_id:
            raise OpenDataSoftDownloaderError("dataset_id must be non-empty.")

        clean_base = base_url.rstrip("/")
        download_url = f"{clean_base}/api/explore/v2.1/catalog/datasets/{dataset_id}/exports/{normalized_format}"

        return OpenDataSoftDownloadPlan(
            base_url=clean_base,
            dataset_id=dataset_id,
            export_format=normalized_format,
            download_url=download_url,
            suggested_filename=f"{dataset_id}_raw.{_extension_for_format(normalized_format)}",
        )

    def download_dataset(
        self,
        *,
        base_url: str,
        dataset_id: str,
        export_format: str,
    ) -> tuple[OpenDataSoftDownloadPlan, HttpDownloadResult]:
        """Download a dataset export."""
        plan = self.build_export_plan(
            base_url=base_url,
            dataset_id=dataset_id,
            export_format=export_format,
        )

        result = self.http_downloader.get(plan.download_url)

        return plan, result


def _extension_for_format(export_format: str) -> str:
    if export_format == "geojson":
        return "geojson"

    if export_format == "json":
        return "json"

    return "csv"
