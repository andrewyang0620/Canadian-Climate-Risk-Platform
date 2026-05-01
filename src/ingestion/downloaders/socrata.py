from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.downloaders.http_downloader import HttpDownloader, HttpDownloadResult


class SocrataDownloaderError(Exception):
    """Raised when a Socrata download cannot be planned or executed."""


@dataclass(frozen=True)
class SocrataDownloadPlan:
    """Download plan for one Socrata dataset export."""

    domain: str
    dataset_id: str
    export_format: str
    download_url: str
    suggested_filename: str


class SocrataDownloader:
    """Downloader for Socrata dataset exports."""

    SUPPORTED_FORMATS = {"csv", "json", "geojson"}

    def __init__(self, http_downloader: HttpDownloader | None = None) -> None:
        self.http_downloader = http_downloader or HttpDownloader()

    def build_export_plan(
        self,
        *,
        domain: str,
        dataset_id: str,
        export_format: str,
    ) -> SocrataDownloadPlan:
        """Build a Socrata API export URL."""
        normalized_format = export_format.lower().strip()

        if normalized_format not in self.SUPPORTED_FORMATS:
            raise SocrataDownloaderError(f"Unsupported Socrata export format: {export_format}")

        if not domain or "/" in domain:
            raise SocrataDownloaderError(f"Invalid Socrata domain: {domain}")

        if not dataset_id:
            raise SocrataDownloaderError("dataset_id must be non-empty.")

        if normalized_format == "geojson":
            download_url = f"https://{domain}/resource/{dataset_id}.geojson"
            extension = "geojson"
        elif normalized_format == "json":
            download_url = f"https://{domain}/resource/{dataset_id}.json"
            extension = "json"
        else:
            download_url = f"https://{domain}/resource/{dataset_id}.csv"
            extension = "csv"

        return SocrataDownloadPlan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=normalized_format,
            download_url=download_url,
            suggested_filename=f"{dataset_id}_raw.{extension}",
        )

    def download_dataset(
        self,
        *,
        domain: str,
        dataset_id: str,
        export_format: str,
    ) -> tuple[SocrataDownloadPlan, HttpDownloadResult]:
        """Download a Socrata dataset export."""
        plan = self.build_export_plan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=export_format,
        )

        result = self.http_downloader.get(plan.download_url)

        return plan, result
