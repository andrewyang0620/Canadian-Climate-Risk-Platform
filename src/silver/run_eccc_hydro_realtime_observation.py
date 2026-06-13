from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from uuid import uuid4

from src.silver.eccc_hydro_realtime_observation import (
    build_eccc_hydro_realtime_observation_silver,
    latest_eccc_hydrometric_realtime_raw_path,
)
from src.utils.config import load_project_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build ECCC hydrometric realtime Silver observation table."
    )

    parser.add_argument(
        "--bronze-root",
        default="lakehouse/bronze",
        help="Bronze input root.",
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_project_config("source_config.yml")
    source = config["sources"]["eccc_hydrometric_realtime"]
    table_name = source.get(
        "target_silver_table",
        "silver_hydro_realtime_observation",
    )

    raw_path = latest_eccc_hydrometric_realtime_raw_path(
        bronze_root=args.bronze_root,
    )

    dataframe = build_eccc_hydro_realtime_observation_silver(raw_path)

    run_id = str(uuid4())
    output_dir = (
        Path(args.silver_root)
        / table_name
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{table_name}.parquet"
    dataframe.to_parquet(output_path, index=False)

    metadata_dir = (
        Path(args.silver_root)
        / "_metadata"
        / "eccc_hydro_realtime_observation"
        / f"extract_date={args.extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "table_name": table_name,
        "run_id": run_id,
        "extract_date": args.extract_date,
        "row_count": int(len(dataframe)),
        "source_names": sorted(dataframe["source_name"].dropna().unique().tolist()),
        "province_codes": sorted(dataframe["province_code"].dropna().unique().tolist()),
        "station_count": int(dataframe["station_id"].nunique()),
        "observed_at_min": str(dataframe["observed_at_utc"].min()),
        "observed_at_max": str(dataframe["observed_at_utc"].max()),
        "negative_discharge_cleaned_count": int(dataframe["negative_discharge_flag"].sum()),
        "output_path": output_path.as_posix(),
        "raw_path": Path(raw_path).as_posix(),
    }

    (metadata_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(
        "[OK] wrote ECCC hydro realtime Silver outputs | "
        f"table={table_name} "
        f"rows={len(dataframe)} "
        f"stations={metadata['station_count']} "
        f"observed_at={metadata['observed_at_min']} to {metadata['observed_at_max']} "
        f"run_id={run_id}"
    )


if __name__ == "__main__":
    main()
