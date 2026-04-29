from __future__ import annotations

import argparse

from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.source_registry import SourceRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze ingestion framework smoke tests.")

    parser.add_argument(
        "--group",
        default="national",
        choices=["national", "municipal", "provincial"],
        help="Source group to include.",
    )

    parser.add_argument(
        "--source",
        default=None,
        help="Optional single source name to ingest.",
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    registry = SourceRegistry()
    writer = BronzeWriter(args.bronze_base_path)

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
            "Use --smoke-test to validate Bronze writing. "
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


if __name__ == "__main__":
    main()
