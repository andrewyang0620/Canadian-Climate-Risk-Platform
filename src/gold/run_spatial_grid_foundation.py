from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.gold.spatial_grid import (
    ANALYSIS_CRS_EPSG,
    DEFAULT_GRID_VERSION,
    GridSpec,
    GoldSpatialGridError,
    generate_boundary_grid,
    select_municipality_boundary,
)


GOLD_GRID_TABLE_NAME = "gold_grid_cell"

EXPECTED_GRID_SYSTEMS = {
    "bc_10km",
    "ab_10km",
    "vancouver_1km",
    "calgary_1km",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold province and city analytical grids.")

    parser.add_argument(
        "--silver-root",
        default="lakehouse/silver",
    )

    parser.add_argument(
        "--gold-root",
        default="lakehouse/gold",
    )

    parser.add_argument(
        "--extract-date",
        default=None,
        help="Gold extract date. Defaults to current UTC date.",
    )

    return parser.parse_args()


def latest_table_parquet(
    *,
    silver_root: str | Path,
    table_name: str,
) -> Path:
    table_root = Path(silver_root) / table_name

    candidates = list(table_root.glob("extract_date=*/run_id=*/*.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No Silver Parquet found for {table_name}: {table_root}")

    return max(
        candidates,
        key=lambda path: path.stat().st_mtime,
    )


def select_province_boundary(
    province_dataframe: pd.DataFrame,
    *,
    province_key: str,
) -> pd.Series:
    required_columns = {
        "province_key",
        "province_code",
        "province_name",
        "boundary_year",
        "geometry_wkt",
        "crs",
    }

    missing_columns = required_columns - set(province_dataframe.columns)

    if missing_columns:
        raise GoldSpatialGridError(
            "Province boundary table is missing columns: " f"{sorted(missing_columns)}"
        )

    matches = province_dataframe[province_dataframe["province_key"] == province_key]

    if len(matches) != 1:
        raise GoldSpatialGridError(
            f"Expected exactly one province boundary for " f"{province_key}; found {len(matches)}."
        )

    return matches.iloc[0]


def build_gold_grid_cell(
    *,
    province_dataframe: pd.DataFrame,
    municipality_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Build all v1 province and city analytical grid systems."""
    alberta = select_province_boundary(
        province_dataframe,
        province_key="AB",
    )
    british_columbia = select_province_boundary(
        province_dataframe,
        province_key="BC",
    )

    calgary = select_municipality_boundary(
        municipality_dataframe,
        municipality_name="Calgary",
        province_key="AB",
    )
    vancouver = select_municipality_boundary(
        municipality_dataframe,
        municipality_name="Vancouver",
        province_key="BC",
    )

    grid_frames: list[pd.DataFrame] = []

    grid_frames.append(
        _build_province_grid(
            boundary=british_columbia,
            spec=GridSpec(
                grid_system="bc_10km",
                grid_level="province",
                cell_size_m=10_000,
                province_key="BC",
                province_code=str(british_columbia["province_code"]),
                province_name=str(british_columbia["province_name"]),
            ),
        )
    )

    grid_frames.append(
        _build_province_grid(
            boundary=alberta,
            spec=GridSpec(
                grid_system="ab_10km",
                grid_level="province",
                cell_size_m=10_000,
                province_key="AB",
                province_code=str(alberta["province_code"]),
                province_name=str(alberta["province_name"]),
            ),
        )
    )

    grid_frames.append(
        _build_city_grid(
            boundary=vancouver,
            spec=GridSpec(
                grid_system="vancouver_1km",
                grid_level="city",
                cell_size_m=1_000,
                province_key="BC",
                province_code=str(vancouver["province_code"]),
                province_name=str(vancouver["province_name"]),
                city_name="Vancouver",
            ),
        )
    )

    grid_frames.append(
        _build_city_grid(
            boundary=calgary,
            spec=GridSpec(
                grid_system="calgary_1km",
                grid_level="city",
                cell_size_m=1_000,
                province_key="AB",
                province_code=str(calgary["province_code"]),
                province_name=str(calgary["province_name"]),
                city_name="Calgary",
            ),
        )
    )

    dataframe = pd.concat(
        grid_frames,
        ignore_index=True,
    )

    dataframe = dataframe.sort_values(
        [
            "grid_system",
            "grid_y_index",
            "grid_x_index",
        ]
    ).reset_index(drop=True)

    _validate_combined_grid(dataframe)

    return dataframe


def _build_province_grid(
    *,
    boundary: pd.Series,
    spec: GridSpec,
) -> pd.DataFrame:
    started_at = time.perf_counter()

    print("[INFO] building Gold spatial grid | " f"grid_system={spec.grid_system}")

    dataframe = generate_boundary_grid(
        boundary_geometry=boundary["geometry_wkt"],
        boundary_key=str(boundary["province_key"]),
        boundary_year=int(boundary["boundary_year"]),
        spec=spec,
        crs_value=str(boundary["crs"]),
    )

    dataframe["source_boundary_type"] = "province"
    dataframe["municipality_key"] = None

    print(
        "[INFO] completed Gold spatial grid | "
        f"grid_system={spec.grid_system} "
        f"rows={len(dataframe)} "
        f"elapsed_seconds="
        f"{time.perf_counter() - started_at:.3f}"
    )

    return dataframe


def _build_city_grid(
    *,
    boundary: pd.Series,
    spec: GridSpec,
) -> pd.DataFrame:
    started_at = time.perf_counter()

    print("[INFO] building Gold spatial grid | " f"grid_system={spec.grid_system}")

    dataframe = generate_boundary_grid(
        boundary_geometry=boundary["geometry_wkt"],
        boundary_key=str(boundary["municipality_key"]),
        boundary_year=int(boundary["boundary_year"]),
        spec=spec,
        crs_value=str(boundary["crs"]),
    )

    dataframe["source_boundary_type"] = "municipality"
    dataframe["municipality_key"] = str(boundary["municipality_key"])

    print(
        "[INFO] completed Gold spatial grid | "
        f"grid_system={spec.grid_system} "
        f"rows={len(dataframe)} "
        f"elapsed_seconds="
        f"{time.perf_counter() - started_at:.3f}"
    )

    return dataframe


def _validate_combined_grid(
    dataframe: pd.DataFrame,
) -> None:
    actual_grid_systems = set(dataframe["grid_system"].dropna().unique())

    if actual_grid_systems != EXPECTED_GRID_SYSTEMS:
        raise GoldSpatialGridError(
            "Unexpected grid systems. "
            f"actual={sorted(actual_grid_systems)} "
            f"expected={sorted(EXPECTED_GRID_SYSTEMS)}"
        )

    if dataframe["grid_cell_key"].isna().any():
        raise GoldSpatialGridError("Gold grid contains null grid-cell keys.")

    if dataframe["grid_cell_key"].duplicated().any():
        raise GoldSpatialGridError("Gold grid contains duplicate grid-cell keys.")

    if (
        dataframe["analysis_area_sq_km"].isna().any()
        or (dataframe["analysis_area_sq_km"] <= 0).any()
    ):
        raise GoldSpatialGridError("Gold grid contains invalid analysis areas.")

    if (
        not dataframe["boundary_coverage_ratio"]
        .between(
            0,
            1,
            inclusive="both",
        )
        .all()
    ):
        raise GoldSpatialGridError("Gold grid contains invalid coverage ratios.")

    if set(dataframe["crs_epsg"].unique()) != {ANALYSIS_CRS_EPSG}:
        raise GoldSpatialGridError("Gold grid contains unexpected CRS values.")


def build_grid_metadata(
    *,
    dataframe: pd.DataFrame,
    run_id: str,
    extract_date: str,
    output_path: Path,
    province_input_path: Path,
    municipality_input_path: Path,
) -> dict[str, Any]:
    grid_system_counts = {
        str(key): int(value)
        for key, value in dataframe["grid_system"].value_counts().to_dict().items()
    }

    grid_system_area_sq_km = {
        str(key): round(float(value), 6)
        for key, value in dataframe.groupby("grid_system")["analysis_area_sq_km"]
        .sum()
        .to_dict()
        .items()
    }

    edge_cell_counts = {
        str(key): int(value)
        for key, value in dataframe.groupby("grid_system")["is_boundary_edge_cell"]
        .sum()
        .to_dict()
        .items()
    }

    return {
        "table_name": GOLD_GRID_TABLE_NAME,
        "run_id": run_id,
        "extract_date": extract_date,
        "load_status": "success",
        "grid_version": DEFAULT_GRID_VERSION,
        "analysis_crs_epsg": ANALYSIS_CRS_EPSG,
        "row_count": int(len(dataframe)),
        "grid_systems": sorted(dataframe["grid_system"].unique().tolist()),
        "grid_system_counts": grid_system_counts,
        "grid_system_area_sq_km": (grid_system_area_sq_km),
        "edge_cell_counts": edge_cell_counts,
        "repaired_source_boundary_cell_counts": {
            str(key): int(value)
            for key, value in dataframe.groupby("grid_system")["source_boundary_geometry_repaired"]
            .sum()
            .to_dict()
            .items()
        },
        "minimum_boundary_coverage_ratio": float(dataframe["boundary_coverage_ratio"].min()),
        "maximum_boundary_coverage_ratio": float(dataframe["boundary_coverage_ratio"].max()),
        "output_path": output_path.as_posix(),
        "input_paths": {
            "silver_boundary_province": (province_input_path.as_posix()),
            "silver_boundary_municipality": (municipality_input_path.as_posix()),
        },
    }


def run_spatial_grid_foundation(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    extract_date: str | None = None,
) -> dict[str, Any]:
    final_extract_date = extract_date or datetime.now(timezone.utc).date().isoformat()
    run_id = str(uuid4())

    province_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_boundary_province",
    )
    municipality_path = latest_table_parquet(
        silver_root=silver_root,
        table_name="silver_boundary_municipality",
    )

    province_dataframe = pd.read_parquet(province_path)
    municipality_dataframe = pd.read_parquet(municipality_path)

    dataframe = build_gold_grid_cell(
        province_dataframe=province_dataframe,
        municipality_dataframe=municipality_dataframe,
    )

    output_dir = (
        Path(gold_root)
        / GOLD_GRID_TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{GOLD_GRID_TABLE_NAME}.parquet"

    dataframe.to_parquet(
        output_path,
        index=False,
    )

    metadata_dir = (
        Path(gold_root)
        / "_metadata"
        / GOLD_GRID_TABLE_NAME
        / f"extract_date={final_extract_date}"
        / f"run_id={run_id}"
    )
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = metadata_dir / "metadata.json"

    metadata = build_grid_metadata(
        dataframe=dataframe,
        run_id=run_id,
        extract_date=final_extract_date,
        output_path=output_path,
        province_input_path=province_path,
        municipality_input_path=municipality_path,
    )

    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    result = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
    }

    print(
        "[OK] wrote Gold spatial grid foundation | "
        f"rows={len(dataframe)} "
        f"systems={metadata['grid_system_counts']} "
        f"run_id={run_id}"
    )

    return result


def main() -> None:
    args = parse_args()

    result = run_spatial_grid_foundation(
        silver_root=args.silver_root,
        gold_root=args.gold_root,
        extract_date=args.extract_date,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
