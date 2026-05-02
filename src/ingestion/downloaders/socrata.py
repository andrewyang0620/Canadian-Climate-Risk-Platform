from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.downloaders.http_downloader import HttpDownloader, HttpDownloadResult
from src.utils.checksum import compute_bytes_checksum
from src.utils.time import utc_now_iso


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
    paginated: bool = False
    page_limit: int | None = None


class SocrataDownloader:
    """Downloader for Socrata dataset exports.

    CSV exports are downloaded with pagination by default to avoid Socrata's
    default row-limit behavior. JSON and GeoJSON exports are currently downloaded
    as single exports.
    """

    SUPPORTED_FORMATS = {"csv", "json", "geojson"}

    def __init__(
        self,
        http_downloader: HttpDownloader | None = None,
        default_page_limit: int = 50000,
        max_pages: int = 1000,
    ) -> None:
        self.http_downloader = http_downloader or HttpDownloader()
        self.default_page_limit = default_page_limit
        self.max_pages = max_pages

    def build_export_plan(
        self,
        *,
        domain: str,
        dataset_id: str,
        export_format: str,
        paginated: bool | None = None,
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
            final_paginated = False
        elif normalized_format == "json":
            download_url = f"https://{domain}/resource/{dataset_id}.json"
            extension = "json"
            final_paginated = False
        else:
            download_url = f"https://{domain}/resource/{dataset_id}.csv"
            extension = "csv"
            final_paginated = True if paginated is None else paginated

        return SocrataDownloadPlan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=normalized_format,
            download_url=download_url,
            suggested_filename=f"{dataset_id}_raw.{extension}",
            paginated=final_paginated,
            page_limit=self.default_page_limit if final_paginated else None,
        )

    def download_dataset(
        self,
        *,
        domain: str,
        dataset_id: str,
        export_format: str,
    ) -> tuple[SocrataDownloadPlan, HttpDownloadResult]:
        """Download a Socrata dataset export.

        CSV sources are downloaded with pagination. Other formats use a single request.
        """
        plan = self.build_export_plan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=export_format,
        )

        if plan.export_format == "csv" and plan.paginated:
            result = self._download_csv_paginated(plan)
            return plan, result

        result = self.http_downloader.get(plan.download_url)
        return plan, result

    def _download_csv_paginated(self, plan: SocrataDownloadPlan) -> HttpDownloadResult:
        """Download a full CSV export using $limit / $offset pagination."""
        if plan.page_limit is None:
            raise SocrataDownloaderError("Paginated CSV plan must define page_limit.")

        combined_lines: list[str] = []
        total_data_rows = 0
        header: str | None = None
        last_result: HttpDownloadResult | None = None

        for page_number in range(self.max_pages):
            offset = page_number * plan.page_limit

            result = self.http_downloader.get(
                plan.download_url,
                params={
                    "$limit": plan.page_limit,
                    "$offset": offset,
                },
            )
            last_result = result

            text = result.content.decode("utf-8-sig")
            lines = text.splitlines()

            if not lines:
                break

            page_header = lines[0]
            page_rows = lines[1:]

            if header is None:
                header = page_header
                combined_lines.append(page_header)
            elif page_header != header:
                raise SocrataDownloaderError(
                    "Socrata CSV pagination header changed between pages. "
                    f"dataset_id={plan.dataset_id}, page={page_number}"
                )

            if page_rows:
                combined_lines.extend(page_rows)
                total_data_rows += len(page_rows)

            if len(page_rows) < plan.page_limit:
                break
        else:
            raise SocrataDownloaderError(
                f"Socrata pagination exceeded max_pages={self.max_pages} "
                f"for dataset_id={plan.dataset_id}"
            )

        if header is None:
            raise SocrataDownloaderError(f"No CSV header returned for dataset_id={plan.dataset_id}")

        combined_text = "\n".join(combined_lines) + "\n"
        combined_content = combined_text.encode("utf-8")

        content_type = last_result.content_type if last_result else "text/csv"

        return HttpDownloadResult(
            url=plan.download_url,
            final_url=plan.download_url,
            status_code=200,
            content_type=content_type,
            content=combined_content,
            size_bytes=len(combined_content),
            checksum=compute_bytes_checksum(combined_content),
            downloaded_at=utc_now_iso(),
        )
