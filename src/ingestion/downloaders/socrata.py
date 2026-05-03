from __future__ import annotations

import csv
import io
import json
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


@dataclass(frozen=True)
class SocrataDownloadResult:
    """Socrata download result with pagination and row-count validation metadata."""

    plan: SocrataDownloadPlan
    download: HttpDownloadResult
    expected_row_count: int | None
    actual_row_count: int | None
    pages_downloaded: int | None
    row_count_validation_supported: bool
    row_count_validation_passed: bool | None
    row_count_validation_error: str | None
    page_limits_used: tuple[int, ...] | None = None


@dataclass(frozen=True)
class SocrataPageDownloadResult:
    """Internal result for one downloaded Socrata page."""

    download: HttpDownloadResult
    page_limit_used: int


class SocrataDownloader:
    """Downloader for Socrata dataset exports.

    CSV exports are downloaded with pagination to avoid Socrata's default
    row-limit behavior. If count(*) is available, downloaded CSV row count is
    reconciled against the expected count.
    """

    SUPPORTED_FORMATS = {"csv", "json", "geojson"}

    def __init__(
        self,
        http_downloader: HttpDownloader | None = None,
        default_page_limit: int = 50000,
        min_page_limit: int = 1000,
        max_pages: int = 2000,
    ) -> None:
        self.http_downloader = http_downloader or HttpDownloader()
        self.default_page_limit = default_page_limit
        self.min_page_limit = min_page_limit
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
    ) -> SocrataDownloadResult:
        """Download a Socrata dataset export."""
        plan = self.build_export_plan(
            domain=domain,
            dataset_id=dataset_id,
            export_format=export_format,
        )

        if plan.export_format == "csv" and plan.paginated:
            expected_row_count, count_error = self._try_fetch_row_count(plan)
            download, actual_row_count, pages_downloaded, page_limits_used = (
                self._download_csv_paginated(plan)
            )

            validation_supported = expected_row_count is not None
            validation_passed: bool | None = None

            if validation_supported:
                validation_passed = expected_row_count == actual_row_count

                if not validation_passed:
                    raise SocrataDownloaderError(
                        "Socrata row-count validation failed. "
                        f"dataset_id={plan.dataset_id}, "
                        f"expected={expected_row_count}, actual={actual_row_count}"
                    )

            return SocrataDownloadResult(
                plan=plan,
                download=download,
                expected_row_count=expected_row_count,
                actual_row_count=actual_row_count,
                pages_downloaded=pages_downloaded,
                row_count_validation_supported=validation_supported,
                row_count_validation_passed=validation_passed,
                row_count_validation_error=count_error,
                page_limits_used=tuple(page_limits_used),
            )

        download = self.http_downloader.get(plan.download_url)

        return SocrataDownloadResult(
            plan=plan,
            download=download,
            expected_row_count=None,
            actual_row_count=None,
            pages_downloaded=None,
            row_count_validation_supported=False,
            row_count_validation_passed=None,
            row_count_validation_error="row_count_validation_not_supported_for_non_csv_export",
            page_limits_used=None,
        )

    def _try_fetch_row_count(self, plan: SocrataDownloadPlan) -> tuple[int | None, str | None]:
        """Try to fetch expected row count using Socrata count(*)."""
        errors: list[str] = []

        try:
            return self._fetch_row_count_json(plan), None
        except Exception as exc:
            errors.append(f"json_count_failed={exc.__class__.__name__}: {exc}")

        try:
            return self._fetch_row_count_csv(plan), None
        except Exception as exc:
            errors.append(f"csv_count_failed={exc.__class__.__name__}: {exc}")

        return None, " | ".join(errors)

    def _fetch_row_count_json(self, plan: SocrataDownloadPlan) -> int:
        """Fetch expected row count using JSON count(*)."""
        count_url = f"https://{plan.domain}/resource/{plan.dataset_id}.json"

        result = self.http_downloader.get(
            count_url,
            params={"$select": "count(*)"},
        )

        try:
            payload = json.loads(result.content.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise SocrataDownloaderError(
                f"Socrata JSON count response is not valid JSON for dataset_id={plan.dataset_id}"
            ) from exc

        if not isinstance(payload, list) or not payload:
            raise SocrataDownloaderError(
                f"Socrata JSON count response has unexpected shape for dataset_id={plan.dataset_id}"
            )

        first_row = payload[0]
        if not isinstance(first_row, dict):
            raise SocrataDownloaderError(
                f"Socrata JSON count response row is not an object for dataset_id={plan.dataset_id}"
            )

        count_value = _extract_count_value(first_row)

        try:
            return int(count_value)
        except (TypeError, ValueError) as exc:
            raise SocrataDownloaderError(
                f"Socrata JSON count value is not an integer for dataset_id={plan.dataset_id}: "
                f"{count_value}"
            ) from exc

    def _fetch_row_count_csv(self, plan: SocrataDownloadPlan) -> int:
        """Fetch expected row count using CSV count(*)."""
        count_url = f"https://{plan.domain}/resource/{plan.dataset_id}.csv"

        result = self.http_downloader.get(
            count_url,
            params={"$select": "count(*)"},
        )

        text = result.content.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))

        if len(rows) < 2 or not rows[1]:
            raise SocrataDownloaderError(
                f"Socrata CSV count response has unexpected shape for dataset_id={plan.dataset_id}"
            )

        try:
            return int(rows[1][0])
        except (TypeError, ValueError) as exc:
            raise SocrataDownloaderError(
                f"Socrata CSV count value is not an integer for dataset_id={plan.dataset_id}: "
                f"{rows[1][0]}"
            ) from exc

    def _download_csv_paginated(
        self,
        plan: SocrataDownloadPlan,
    ) -> tuple[HttpDownloadResult, int, int, list[int]]:
        """Download a full CSV export using adaptive $limit / $offset pagination."""
        if plan.page_limit is None:
            raise SocrataDownloaderError("Paginated CSV plan must define page_limit.")

        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")

        total_data_rows = 0
        pages_downloaded = 0
        header: list[str] | None = None
        last_result: HttpDownloadResult | None = None
        page_limits_used: list[int] = []

        offset = 0
        current_limit = plan.page_limit

        while pages_downloaded < self.max_pages:
            page_result = self._download_csv_page_with_adaptive_limit(
                plan=plan,
                offset=offset,
                starting_limit=current_limit,
            )

            result = page_result.download
            used_limit = page_result.page_limit_used

            last_result = result
            pages_downloaded += 1
            current_limit = used_limit
            page_limits_used.append(used_limit)

            text = result.content.decode("utf-8-sig")
            rows = list(csv.reader(io.StringIO(text)))

            if not rows:
                break

            page_header = rows[0]
            page_rows = rows[1:]

            if header is None:
                header = page_header
                writer.writerow(header)
            elif page_header != header:
                raise SocrataDownloaderError(
                    "Socrata CSV pagination header changed between pages. "
                    f"dataset_id={plan.dataset_id}, page={pages_downloaded}"
                )

            if page_rows:
                writer.writerows(page_rows)
                total_data_rows += len(page_rows)

            if len(page_rows) < used_limit:
                break

            offset += len(page_rows)

        else:
            raise SocrataDownloaderError(
                f"Socrata pagination exceeded max_pages={self.max_pages} "
                f"for dataset_id={plan.dataset_id}"
            )

        if header is None:
            raise SocrataDownloaderError(f"No CSV header returned for dataset_id={plan.dataset_id}")

        combined_content = output.getvalue().encode("utf-8")
        content_type = last_result.content_type if last_result else "text/csv"

        return (
            HttpDownloadResult(
                url=plan.download_url,
                final_url=plan.download_url,
                status_code=200,
                content_type=content_type,
                content=combined_content,
                size_bytes=len(combined_content),
                checksum=compute_bytes_checksum(combined_content),
                downloaded_at=utc_now_iso(),
            ),
            total_data_rows,
            pages_downloaded,
            page_limits_used,
        )

    def _download_csv_page_with_adaptive_limit(
        self,
        *,
        plan: SocrataDownloadPlan,
        offset: int,
        starting_limit: int,
    ) -> SocrataPageDownloadResult:
        """Download one CSV page, reducing limit if Socrata returns server errors."""
        if starting_limit <= 0:
            raise SocrataDownloaderError(f"starting_limit must be positive: {starting_limit}")

        current_limit = starting_limit
        effective_min_limit = min(self.min_page_limit, starting_limit)
        errors: list[str] = []

        while current_limit >= effective_min_limit:
            try:
                result = self.http_downloader.get(
                    plan.download_url,
                    params={
                        "$limit": current_limit,
                        "$offset": offset,
                    },
                )

                return SocrataPageDownloadResult(
                    download=result,
                    page_limit_used=current_limit,
                )

            except Exception as exc:
                errors.append(
                    f"limit={current_limit}, offset={offset}, "
                    f"error={exc.__class__.__name__}: {exc}"
                )

                if current_limit == effective_min_limit:
                    break

                current_limit = max(effective_min_limit, current_limit // 2)

        raise SocrataDownloaderError(
            "Socrata CSV page download failed after adaptive limit reduction. "
            f"dataset_id={plan.dataset_id}, offset={offset}, errors={errors}"
        )


def _extract_count_value(row: dict[str, object]) -> object:
    count_value = row.get("count")

    if count_value is not None:
        return count_value

    for key, value in row.items():
        if "count" in key.lower():
            return value

    raise SocrataDownloaderError("Socrata count response missing count field.")
