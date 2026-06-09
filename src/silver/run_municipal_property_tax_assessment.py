from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from uuid import uuid4

from src.silver.common import latest_successful_bronze_raw_path
from src.silver.municipal_property_tax_assessment import (
    build_vancouver_property_tax_assessment_silver,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver municipal property tax assessment outputs."
    )

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
        help="Silver output root.",
    )

    parser.add_argument(
        "--extract-date",
        default=date.today().isoformat(),
        help="Silver extract date partition.",
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="CSV chunk size.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    raw_path = latest_successful_bronze_raw_path(source_name="vancouver_property_tax")

    dataframe = build_vancouver_property_tax_assessment_silver(
        raw_path,
        chunksize=args.chunksize,
    )

    run_id = str(uuid4())
    output_dir = (
        Path(args.silver_root)
        / "silver_property_tax_assessment"
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "silver_property_tax_assessment.parquet"
    dataframe.to_parquet(output_path, index=False)

    metadata_dir = (
        Path(args.silver_root)
        / "_metadata"
        / "municipal_property_tax_assessment"
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "table_name": "silver_property_tax_assessment",
        "run_id": run_id,
        "extract_date": args.extract_date,
        "row_count": int(len(dataframe)),
        "cities": sorted(dataframe["city"].dropna().unique().tolist()),
        "source_names": sorted(dataframe["source_name"].dropna().unique().tolist()),
        "report_year_min": int(dataframe["report_year"].dropna().min()),
        "report_year_max": int(dataframe["report_year"].dropna().max()),
        "output_path": output_path.as_posix(),
        "raw_path": Path(raw_path).as_posix(),
    }

    (metadata_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(
        "[OK] wrote municipal property tax assessment Silver outputs | "
        f"rows={len(dataframe)} "
        f"cities={metadata['cities']} "
        f"report_years={metadata['report_year_min']}-{metadata['report_year_max']} "
        f"run_id={run_id}"
    )


if __name__ == "__main__":
    main()
