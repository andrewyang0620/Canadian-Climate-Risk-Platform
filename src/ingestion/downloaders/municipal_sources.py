from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.opendatasoft import (
    OpenDataSoftDownloadPlan,
    OpenDataSoftDownloader,
)
from src.ingestion.downloaders.socrata import SocrataDownloadPlan, SocrataDownloader
from src.ingestion.source_registry import SourceDefinition, SourceRegistry


class MunicipalSourceDownloaderError(Exception):
    """Raised when a municipal source cannot be planned or downloaded."""


@dataclass(frozen=True)
class MunicipalDownloadPlan:
    """Download plan for one municipal source."""

    source_name: str
    display_name: str
    provider: str
    portal_type: str
    dataset_id: str
    export_format: str
    download_url: str
    target_bronze_table: str
    suggested_raw_filename: str
    implemented: bool
    paginated: bool = False
    page_limit: int | None = None


@dataclass(frozen=True)
class MunicipalDownloadResult:
    """Downloaded municipal raw dataset with plan metadata."""

    plan: MunicipalDownloadPlan
    download: HttpDownloadResult
    extra_metadata: dict[str, Any] = field(default_factory=dict)


class MunicipalSourceDownloader:
    """Downloader for configured Vancouver and Calgary municipal open data sources."""

    def __init__(
        self,
        registry: SourceRegistry | None = None,
        opendatasoft_downloader: OpenDataSoftDownloader | None = None,
        socrata_downloader: SocrataDownloader | None = None,
    ) -> None:
        self.registry = registry or SourceRegistry()
        self.opendatasoft_downloader = opendatasoft_downloader or OpenDataSoftDownloader()
        self.socrata_downloader = socrata_downloader or SocrataDownloader()

    def list_sources(self) -> list[str]:
        """List configured municipal source names."""
        return [
            source.name
            for source in self.registry.sources.values()
            if source.source_group == "municipal"
        ]

    def build_plan(self, source_name: str) -> MunicipalDownloadPlan:
        """Build a municipal download plan."""
        source = self.registry.get_source(source_name)
        self._validate_municipal_source(source)

        portal_type = str(source.raw.get("portal_type", "")).lower()

        if portal_type == "opendatasoft":
            ods_plan = self._build_opendatasoft_plan(source)
            return MunicipalDownloadPlan(
                source_name=source.name,
                display_name=source.display_name,
                provider=source.provider,
                portal_type=portal_type,
                dataset_id=ods_plan.dataset_id,
                export_format=ods_plan.export_format,
                download_url=ods_plan.download_url,
                target_bronze_table=source.target_bronze_table,
                suggested_raw_filename=(
                    f"{source.name}_raw.{_extension_from_filename(ods_plan.suggested_filename)}"
                ),
                implemented=True,
                paginated=False,
                page_limit=None,
            )

        if portal_type == "socrata":
            socrata_plan = self._build_socrata_plan(source)
            return MunicipalDownloadPlan(
                source_name=source.name,
                display_name=source.display_name,
                provider=source.provider,
                portal_type=portal_type,
                dataset_id=socrata_plan.dataset_id,
                export_format=socrata_plan.export_format,
                download_url=socrata_plan.download_url,
                target_bronze_table=source.target_bronze_table,
                suggested_raw_filename=(
                    f"{source.name}_raw.{_extension_from_filename(socrata_plan.suggested_filename)}"
                ),
                implemented=True,
                paginated=socrata_plan.paginated,
                page_limit=socrata_plan.page_limit,
            )

        raise MunicipalSourceDownloaderError(
            f"Source '{source.name}' has unsupported portal_type: {portal_type}"
        )

    def build_all_plans(self) -> list[MunicipalDownloadPlan]:
        """Build plans for all municipal sources."""
        return [self.build_plan(source_name) for source_name in self.list_sources()]

    def download_source(self, source_name: str) -> MunicipalDownloadResult:
        """Download one configured municipal source."""
        source = self.registry.get_source(source_name)
        self._validate_municipal_source(source)

        portal_type = str(source.raw.get("portal_type", "")).lower()

        if portal_type == "opendatasoft":
            _, download = self.opendatasoft_downloader.download_dataset(
                base_url=str(source.raw["opendatasoft_base_url"]),
                dataset_id=str(source.raw["dataset_id"]),
                export_format=str(source.raw["preferred_export_format"]),
            )
            plan = self.build_plan(source_name)
            return MunicipalDownloadResult(
                plan=plan,
                download=download,
                extra_metadata={
                    "row_count_validation_supported": False,
                    "row_count_validation_reason": "opendatasoft_export_count_not_implemented",
                },
            )

        if portal_type == "socrata":
            socrata_result = self.socrata_downloader.download_dataset(
                domain=str(source.raw["socrata_domain"]),
                dataset_id=str(source.raw["dataset_id"]),
                export_format=str(source.raw["preferred_export_format"]),
            )
            plan = self.build_plan(source_name)
            return MunicipalDownloadResult(
                plan=plan,
                download=socrata_result.download,
                extra_metadata={
                    "row_count_validation_supported": (
                        socrata_result.row_count_validation_supported
                    ),
                    "socrata_expected_row_count": socrata_result.expected_row_count,
                    "socrata_actual_row_count": socrata_result.actual_row_count,
                    "socrata_pages_downloaded": socrata_result.pages_downloaded,
                    "socrata_row_count_validation_passed": (
                        socrata_result.row_count_validation_passed
                    ),
                    "socrata_row_count_validation_error": (
                        socrata_result.row_count_validation_error
                    ),
                },
            )

        raise MunicipalSourceDownloaderError(
            f"Source '{source.name}' has unsupported portal_type: {portal_type}"
        )

    def _build_opendatasoft_plan(self, source: SourceDefinition) -> OpenDataSoftDownloadPlan:
        required = ["opendatasoft_base_url", "dataset_id", "preferred_export_format"]
        _require_raw_fields(source, required)

        return self.opendatasoft_downloader.build_export_plan(
            base_url=str(source.raw["opendatasoft_base_url"]),
            dataset_id=str(source.raw["dataset_id"]),
            export_format=str(source.raw["preferred_export_format"]),
        )

    def _build_socrata_plan(self, source: SourceDefinition) -> SocrataDownloadPlan:
        required = ["socrata_domain", "dataset_id", "preferred_export_format"]
        _require_raw_fields(source, required)

        return self.socrata_downloader.build_export_plan(
            domain=str(source.raw["socrata_domain"]),
            dataset_id=str(source.raw["dataset_id"]),
            export_format=str(source.raw["preferred_export_format"]),
        )

    @staticmethod
    def _validate_municipal_source(source: SourceDefinition) -> None:
        if source.source_group != "municipal":
            raise MunicipalSourceDownloaderError(
                f"Source '{source.name}' is not municipal. source_group={source.source_group}"
            )


def _require_raw_fields(source: SourceDefinition, fields: list[str]) -> None:
    missing = [field for field in fields if field not in source.raw]

    if missing:
        raise MunicipalSourceDownloaderError(
            f"Source '{source.name}' missing required raw config fields: {missing}"
        )


def _extension_from_filename(filename: str) -> str:
    if "." not in filename:
        return "bin"

    return filename.rsplit(".", 1)[-1]
