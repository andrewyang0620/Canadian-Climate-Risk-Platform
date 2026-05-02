from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.bronze_writer import BronzeWriteResult, BronzeWriter
from src.ingestion.downloaders.municipal_sources import (
    MunicipalDownloadResult,
    MunicipalSourceDownloader,
)
from src.ingestion.source_registry import SourceRegistry


@dataclass(frozen=True)
class MunicipalBronzeIngestionResult:
    """Result for one municipal Bronze ingestion run."""

    source_name: str
    portal_type: str
    dataset_id: str
    download_url: str
    bronze_result: BronzeWriteResult


class MunicipalBronzeIngestor:
    """Ingest configured municipal open data sources into Bronze."""

    def __init__(
        self,
        registry: SourceRegistry | None = None,
        writer: BronzeWriter | None = None,
        downloader: MunicipalSourceDownloader | None = None,
    ) -> None:
        self.registry = registry or SourceRegistry()
        self.writer = writer or BronzeWriter()
        self.downloader = downloader or MunicipalSourceDownloader(registry=self.registry)

    def ingest_source(self, source_name: str) -> MunicipalBronzeIngestionResult:
        """Download one municipal source and write it to Bronze."""
        source = self.registry.get_source(source_name)
        download_result = self.downloader.download_source(source_name)

        bronze_result = self._write_download_to_bronze(
            source_name=source_name,
            download_result=download_result,
        )

        return MunicipalBronzeIngestionResult(
            source_name=source_name,
            portal_type=download_result.plan.portal_type,
            dataset_id=download_result.plan.dataset_id,
            download_url=download_result.plan.download_url,
            bronze_result=bronze_result,
        )

    def _write_download_to_bronze(
        self,
        *,
        source_name: str,
        download_result: MunicipalDownloadResult,
    ) -> BronzeWriteResult:
        source = self.registry.get_source(source_name)
        plan = download_result.plan
        download = download_result.download

        return self.writer.write_bytes(
            source=source,
            filename=plan.suggested_raw_filename,
            content=download.content,
            row_count=None,
            ingestion_method=f"{plan.portal_type}_dataset_export",
            extra_metadata={
                "municipal_portal_type": plan.portal_type,
                "municipal_dataset_id": plan.dataset_id,
                "municipal_export_format": plan.export_format,
                "municipal_download_url": plan.download_url,
                "municipal_paginated": plan.paginated,
                "municipal_page_limit": plan.page_limit,
                "download_final_url": download.final_url,
                "download_status_code": download.status_code,
                "download_content_type": download.content_type,
                "download_size_bytes": download.size_bytes,
                "source_note": (
                    "Raw municipal open data export downloaded and preserved in Bronze."
                ),
            },
        )
