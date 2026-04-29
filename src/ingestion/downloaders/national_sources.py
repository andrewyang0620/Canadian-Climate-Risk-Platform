from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.downloaders.http_downloader import (
    HttpDownloadResult,
    HttpDownloader,
)
from src.ingestion.source_registry import SourceDefinition, SourceRegistry


class NationalSourceDownloaderError(Exception):
    """Raised when a national source downloader cannot be planned or executed."""


@dataclass(frozen=True)
class NationalDownloadPlan:
    """Download plan for a national or provincial public source."""

    source_name: str
    display_name: str
    source_url: str
    access_method: str
    expected_frequency: str
    file_format: str
    spatial_grain: str
    temporal_grain: str
    target_bronze_table: str
    suggested_raw_filename: str
    implemented: bool
    notes: str


class NationalSourceDownloader:
    """Download planner and probe utility for national/provincial sources.

    This class does not yet implement full dataset-specific extraction logic.
    It provides a tested foundation for source-specific clients that will be
    added incrementally.
    """

    NATIONAL_SOURCE_GROUPS = {"national", "provincial"}

    def __init__(
        self,
        registry: SourceRegistry | None = None,
        http_downloader: HttpDownloader | None = None,
    ) -> None:
        self.registry = registry or SourceRegistry()
        self.http_downloader = http_downloader or HttpDownloader()

    def list_sources(self) -> list[str]:
        """List configured national/provincial source names."""
        return [source.name for source in self._national_source_definitions()]

    def build_plan(self, source_name: str) -> NationalDownloadPlan:
        """Build a download plan for one configured source."""
        source = self.registry.get_source(source_name)
        self._validate_national_source(source)

        return NationalDownloadPlan(
            source_name=source.name,
            display_name=source.display_name,
            source_url=source.source_url,
            access_method=source.access_method,
            expected_frequency=source.expected_frequency,
            file_format=source.file_format,
            spatial_grain=source.spatial_grain,
            temporal_grain=source.temporal_grain,
            target_bronze_table=source.target_bronze_table,
            suggested_raw_filename=self._suggested_raw_filename(source),
            implemented=False,
            notes=(
                "Dataset-specific extraction is not implemented yet. "
                "This plan records the configured source metadata and "
                "the intended Bronze raw filename."
            ),
        )

    def build_all_plans(self) -> list[NationalDownloadPlan]:
        """Build download plans for all national/provincial sources."""
        return [self.build_plan(source.name) for source in self._national_source_definitions()]

    def probe_source_landing_page(self, source_name: str) -> HttpDownloadResult:
        """Probe a source landing page to confirm basic HTTP accessibility.

        This is not a dataset extraction method. It should be used only as a
        lightweight availability check.
        """
        source = self.registry.get_source(source_name)
        self._validate_national_source(source)

        return self.http_downloader.get(source.source_url)

    def plan_eccc_historical_climate(self) -> NationalDownloadPlan:
        return self.build_plan("eccc_historical_climate")

    def plan_eccc_hydrometric_realtime(self) -> NationalDownloadPlan:
        return self.build_plan("eccc_hydrometric_realtime")

    def plan_hydat_archive(self) -> NationalDownloadPlan:
        return self.build_plan("hydat_archive")

    def plan_wildfire_history(self) -> NationalDownloadPlan:
        return self.build_plan("wildfire_history")

    def plan_statcan_building_permits(self) -> NationalDownloadPlan:
        return self.build_plan("statcan_building_permits")

    def plan_census_boundaries(self) -> NationalDownloadPlan:
        return self.build_plan("census_boundaries")

    def plan_canadian_disaster_database(self) -> NationalDownloadPlan:
        return self.build_plan("canadian_disaster_database")

    def _national_source_definitions(self) -> list[SourceDefinition]:
        return [
            source
            for source in self.registry.sources.values()
            if source.source_group in self.NATIONAL_SOURCE_GROUPS
        ]

    def _validate_national_source(self, source: SourceDefinition) -> None:
        if source.source_group not in self.NATIONAL_SOURCE_GROUPS:
            raise NationalSourceDownloaderError(
                f"Source '{source.name}' is not a national/provincial source. "
                f"source_group={source.source_group}"
            )

    @staticmethod
    def _suggested_raw_filename(source: SourceDefinition) -> str:
        extension = _extension_from_file_format(source.file_format)
        return f"{source.name}_raw{extension}"


def _extension_from_file_format(file_format: str) -> str:
    normalized = file_format.lower()

    if "csv" in normalized:
        return ".csv"

    if "json" in normalized or "geojson" in normalized:
        return ".geojson"

    if "sqlite" in normalized:
        return ".sqlite"

    if "xlsx" in normalized:
        return ".xlsx"

    if "shapefile" in normalized:
        return ".zip"

    return ".bin"
