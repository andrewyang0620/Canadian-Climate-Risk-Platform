from __future__ import annotations

import argparse

from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.downloaders.national_sources import NationalSourceDownloader
from src.ingestion.source_registry import SourceRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze ingestion framework utilities.")

    parser.add_argument(
        "--group",
        default="national",
        choices=["national", "municipal", "provincial"],
        help="Source group to include.",
    )

    parser.add_argument(
        "--source",
        default=None,
        help="Optional single source name.",
    )

    parser.add_argument(
        "--bronze-base-path",
        default="lakehouse/bronze",
        help="Base path for local Bronze lakehouse output.",
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Write small smoke-test raw files instead of downloading real sources.",
    )

    parser.add_argument(
        "--list-plans",
        action="store_true",
        help="Print national/provincial download plans.",
    )

    parser.add_argument(
        "--probe-source",
        action="store_true",
        help="Probe a configured national/provincial source landing page.",
    )

    parser.add_argument(
        "--download-cdd",
        action="store_true",
        help="Download Canadian Disaster Database raw file into Bronze.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    registry = SourceRegistry()
    writer = BronzeWriter(args.bronze_base_path)
    national_downloader = NationalSourceDownloader(registry=registry)

    if args.list_plans:
        plans = (
            [national_downloader.build_plan(args.source)]
            if args.source
            else national_downloader.build_all_plans()
        )

        for plan in plans:
            print(
                f"[PLAN] {plan.source_name} | "
                f"method={plan.access_method} | "
                f"format={plan.file_format} | "
                f"bronze={plan.target_bronze_table} | "
                f"filename={plan.suggested_raw_filename} | "
                f"implemented={plan.implemented}"
            )

        return

    if args.probe_source:
        if not args.source:
            raise ValueError("--probe-source requires --source")

        result = national_downloader.probe_source_landing_page(args.source)
        print(
            f"[PROBE OK] {args.source} | "
            f"status={result.status_code} | "
            f"bytes={result.size_bytes} | "
            f"content_type={result.content_type}"
        )
        return

    if args.download_cdd:
        source = registry.get_source("canadian_disaster_database")
        download_result = national_downloader.download_canadian_disaster_database()

        selected_resource = download_result.resource
        raw_filename = _filename_for_cdd_resource(selected_resource.url, selected_resource.format)

        result = writer.write_bytes(
            source=source,
            filename=raw_filename,
            content=download_result.download.content,
            row_count=None,
            ingestion_method="open_canada_package_resource",
            extra_metadata={
                "open_data_dataset_id": download_result.package.dataset_id,
                "open_data_package_title": download_result.package.title,
                "open_data_resource_id": selected_resource.resource_id,
                "open_data_resource_name": selected_resource.name,
                "open_data_resource_format": selected_resource.format,
                "open_data_resource_language": selected_resource.language,
                "open_data_resource_url": selected_resource.url,
                "download_final_url": download_result.download.final_url,
                "download_status_code": download_result.download.status_code,
                "download_content_type": download_result.download.content_type,
                "download_size_bytes": download_result.download.size_bytes,
                "source_note": (
                    "Raw Canadian Disaster Database resource downloaded from "
                    "Open Canada package metadata."
                ),
            },
        )

        print(
            f"[CDD OK] {result.source_name} -> "
            f"{result.raw_file_path} | metadata={result.metadata_path}"
        )
        return

    if args.source:
        sources = [registry.get_source(args.source)]
    else:
        sources = registry.filter_by_group(args.group)

    if not sources:
        raise ValueError(f"No sources found for group: {args.group}")

    if not args.smoke_test:
        source_names = ", ".join(source.name for source in sources)
        raise NotImplementedError(
            "Real source downloaders are not implemented yet. "
            "Use --smoke-test to validate Bronze writing, or --download-cdd for the first "
            "implemented real source. "
            f"Selected sources: {source_names}"
        )

    for source in sources:
        content = (
            "bronze ingestion smoke test\n"
            f"source_name={source.name}\n"
            f"source_url={source.source_url}\n"
            f"target_bronze_table={source.target_bronze_table}\n"
        )

        result = writer.write_text(
            source=source,
            filename="_SMOKE_TEST.txt",
            content=content,
            row_count=1,
            extra_metadata={
                "smoke_test": True,
                "note": "This is not real source data.",
            },
        )

        print(
            f"[OK] {result.source_name} -> "
            f"{result.raw_file_path} | metadata={result.metadata_path}"
        )


def _filename_for_cdd_resource(resource_url: str, resource_format: str) -> str:
    normalized_format = resource_format.lower()

    if "csv" in normalized_format:
        return "canadian_disaster_database_raw.csv"

    if "xlsx" in normalized_format:
        return "canadian_disaster_database_raw.xlsx"

    if "xls" in normalized_format:
        return "canadian_disaster_database_raw.xls"

    if resource_url.lower().endswith(".csv"):
        return "canadian_disaster_database_raw.csv"

    if resource_url.lower().endswith(".xlsx"):
        return "canadian_disaster_database_raw.xlsx"

    if resource_url.lower().endswith(".xls"):
        return "canadian_disaster_database_raw.xls"

    return "canadian_disaster_database_raw.bin"


if __name__ == "__main__":
    main()
