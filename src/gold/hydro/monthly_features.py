from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.gold.common.io import (
    latest_partitioned_table_parquet_files,
    latest_table_parquet,
)
from src.gold.spatial.grid import ANALYSIS_CRS_EPSG

HYDRO_ANALYSIS_START_YEAR = 2016
HYDRO_ANALYSIS_END_YEAR = 2025

HYDRO_ANALYSIS_START_MONTH = "2016-01"
HYDRO_ANALYSIS_END_MONTH = "2025-12"

EXPECTED_HYDRO_MEASUREMENT_TYPES = {"flow", "level"}

DISPLAY_CRS_EPSG = 4326
EXPECTED_HYDRO_GRID_SYSTEMS = {"ab_10km", "bc_10km"}

MIN_STATION_MAPPING_COVERAGE_RATIO = 0.95
MAX_REASONABLE_STATION_GRID_DISTANCE_KM = 50.0


class GoldHydroFeatureError(Exception):
    """Raised when Gold hydro feature generation fails."""


def read_silver_hydro_station(
    *,
    silver_root: str | Path = "lakehouse/silver",
) -> pd.DataFrame:
    station_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_hydro_station",
    )

    return pd.read_parquet(station_path)


def read_silver_hydro_daily(
    *,
    silver_root: str | Path = "lakehouse/silver",
    start_year: int = HYDRO_ANALYSIS_START_YEAR,
    end_year: int = HYDRO_ANALYSIS_END_YEAR,
) -> pd.DataFrame:
    paths = latest_partitioned_table_parquet_files(
        table_root=Path(silver_root) / "silver_hydro_daily",
        partition_pattern="observation_year=*/*.parquet",
    )

    filtered_paths = [
        path
        for path in paths
        if _path_partition_year_in_range(
            path=path,
            start_year=start_year,
            end_year=end_year,
        )
    ]

    if not filtered_paths:
        raise GoldHydroFeatureError(
            "No silver_hydro_daily files found for requested year range "
            f"{start_year}–{end_year}."
        )

    frames = [pd.read_parquet(path) for path in filtered_paths]

    return pd.concat(frames, ignore_index=True)


def build_gold_hydro_station_month_feature(
    *,
    hydro_daily: pd.DataFrame,
    hydro_station: pd.DataFrame,
    start_month: str = HYDRO_ANALYSIS_START_MONTH,
    end_month: str = HYDRO_ANALYSIS_END_MONTH,
) -> pd.DataFrame:
    _require_columns(
        hydro_daily,
        {
            "station_id",
            "observation_date",
            "measurement_type",
            "measurement_value",
            "measurement_symbol",
            "province",
            "latitude",
            "longitude",
        },
        "silver_hydro_daily",
    )

    _require_columns(
        hydro_station,
        {
            "station_id",
            "station_name",
            "drainage_area_gross",
            "drainage_area_effect",
            "rhbn",
            "real_time",
        },
        "silver_hydro_station",
    )

    station_duplicate_count = int(hydro_station["station_id"].astype(str).duplicated().sum())

    if station_duplicate_count > 0:
        raise GoldHydroFeatureError(
            "silver_hydro_station contains duplicate station_id values: "
            f"{station_duplicate_count}."
        )

    daily = hydro_daily.copy()
    daily["station_id"] = daily["station_id"].astype(str)
    daily["province_key"] = daily["province"].astype(str).str.upper()
    daily["measurement_type"] = daily["measurement_type"].astype(str).str.lower()

    unexpected_measurement_types = (
        set(daily["measurement_type"].dropna().unique()) - EXPECTED_HYDRO_MEASUREMENT_TYPES
    )

    if unexpected_measurement_types:
        raise GoldHydroFeatureError(
            "silver_hydro_daily contains unexpected measurement_type values: "
            f"{sorted(unexpected_measurement_types)}."
        )

    daily["observation_date"] = pd.to_datetime(
        daily["observation_date"],
        errors="coerce",
    )

    invalid_date_count = int(daily["observation_date"].isna().sum())

    if invalid_date_count > 0:
        raise GoldHydroFeatureError(
            "silver_hydro_daily contains invalid observation_date values: " f"{invalid_date_count}."
        )

    daily["reference_month"] = daily["observation_date"].dt.to_period("M").astype(str)

    daily = daily[
        daily["reference_month"].between(
            start_month,
            end_month,
            inclusive="both",
        )
    ].copy()

    if daily.empty:
        raise GoldHydroFeatureError(
            "No hydro daily observations remain after applying "
            f"month window {start_month}–{end_month}."
        )

    station_metadata = hydro_station[
        [
            "station_id",
            "station_name",
            "drainage_area_gross",
            "drainage_area_effect",
            "rhbn",
            "real_time",
        ]
    ].copy()
    station_metadata["station_id"] = station_metadata["station_id"].astype(str)

    daily = daily.merge(
        station_metadata,
        on="station_id",
        how="left",
        validate="many_to_one",
    )

    missing_station_metadata_count = int(daily["station_name"].isna().sum())

    if missing_station_metadata_count > 0:
        raise GoldHydroFeatureError(
            "Hydro daily observations include station_id values missing "
            "from silver_hydro_station: "
            f"{missing_station_metadata_count} rows."
        )

    daily["has_measurement"] = daily["measurement_value"].notna()
    daily["has_measurement_symbol"] = daily["measurement_symbol"].notna()
    daily["estimated_symbol"] = daily["measurement_symbol"].astype(str).eq("E")
    daily["approved_symbol"] = daily["measurement_symbol"].astype(str).eq("A")
    daily["flow_zero_day"] = daily["measurement_type"].eq("flow") & daily[
        "measurement_value"
    ].fillna(-1).eq(0)
    daily["negative_value"] = daily["measurement_value"] < 0

    grouped = (
        daily.groupby(
            [
                "province_key",
                "station_id",
                "measurement_type",
                "reference_month",
            ],
            dropna=False,
        )
        .agg(
            station_name=("station_name", "first"),
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            drainage_area_gross=("drainage_area_gross", "first"),
            drainage_area_effect=("drainage_area_effect", "first"),
            rhbn=("rhbn", "first"),
            real_time=("real_time", "first"),
            daily_record_count=("observation_date", "count"),
            observation_day_count=("observation_date", "nunique"),
            measurement_observation_count=("has_measurement", "sum"),
            mean_measurement_value=("measurement_value", "mean"),
            min_measurement_value=("measurement_value", "min"),
            max_measurement_value=("measurement_value", "max"),
            median_measurement_value=("measurement_value", "median"),
            p95_measurement_value=("measurement_value", _p95),
            measurement_symbol_count=("has_measurement_symbol", "sum"),
            estimated_symbol_count=("estimated_symbol", "sum"),
            approved_symbol_count=("approved_symbol", "sum"),
            flow_zero_day_count=("flow_zero_day", "sum"),
            negative_value_count=("negative_value", "sum"),
        )
        .reset_index()
    )

    grouped["days_in_month"] = pd.PeriodIndex(
        grouped["reference_month"],
        freq="M",
    ).days_in_month

    grouped["measurement_completeness_ratio"] = (
        grouped["observation_day_count"] / grouped["days_in_month"]
    ).clip(0, 1)

    grouped["hydro_station_month_key"] = (
        grouped["province_key"].astype(str)
        + "__"
        + grouped["station_id"].astype(str)
        + "__"
        + grouped["measurement_type"].astype(str)
        + "__"
        + grouped["reference_month"].astype(str)
    )

    integer_columns = [
        "daily_record_count",
        "observation_day_count",
        "measurement_observation_count",
        "measurement_symbol_count",
        "estimated_symbol_count",
        "approved_symbol_count",
        "flow_zero_day_count",
        "negative_value_count",
        "days_in_month",
    ]

    for column in integer_columns:
        grouped[column] = grouped[column].astype("int64")

    return grouped.sort_values(
        [
            "reference_month",
            "province_key",
            "station_id",
            "measurement_type",
        ]
    ).reset_index(drop=True)


def _p95(series: pd.Series) -> float:
    return float(series.quantile(0.95))


def _path_partition_year_in_range(
    *,
    path: Path,
    start_year: int,
    end_year: int,
) -> bool:
    year = _partition_value(path, "observation_year")

    if year is None:
        return False

    return start_year <= int(year) <= end_year


def _partition_value(path: Path, partition_name: str) -> str | None:
    prefix = f"{partition_name}="

    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)

    return None


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldHydroFeatureError(f"{table_name} is missing columns: {sorted(missing_columns)}")


def summarize_hydro_station_month(
    station_month: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "row_count": int(len(station_month)),
        "station_count": int(station_month["station_id"].nunique()),
        "month_count": int(station_month["reference_month"].nunique()),
        "measurement_types": sorted(station_month["measurement_type"].dropna().unique().tolist()),
        "minimum_month": str(station_month["reference_month"].min()),
        "maximum_month": str(station_month["reference_month"].max()),
    }


def build_gold_grid_month_hydro_feature(
    *,
    station_month: pd.DataFrame,
    grid: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    _require_columns(
        station_month,
        {
            "province_key",
            "station_id",
            "measurement_type",
            "reference_month",
            "latitude",
            "longitude",
            "daily_record_count",
            "observation_day_count",
            "measurement_observation_count",
            "measurement_completeness_ratio",
            "mean_measurement_value",
            "min_measurement_value",
            "max_measurement_value",
            "median_measurement_value",
            "p95_measurement_value",
            "flow_zero_day_count",
            "negative_value_count",
        },
        "gold_hydro_station_month_feature",
    )

    _require_columns(
        grid,
        {
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "analysis_geometry_wkt",
            "crs_epsg",
        },
        "gold_grid_cell",
    )

    grid_for_mapping = grid[grid["grid_system"].isin(EXPECTED_HYDRO_GRID_SYSTEMS)].copy()

    if grid_for_mapping.empty:
        raise GoldHydroFeatureError("gold_grid_cell does not contain AB/BC 10km grid systems.")

    mapping = _map_hydro_stations_to_grid_cells(
        station_month=station_month,
        grid=grid_for_mapping,
    )

    station_keys = station_month[["station_id", "province_key"]].drop_duplicates()

    mapped_station_keys = (
        mapping[["station_id", "province_key"]].drop_duplicates()
        if not mapping.empty
        else station_keys.iloc[0:0]
    )

    mapping_coverage_ratio = (
        len(mapped_station_keys) / len(station_keys) if len(station_keys) > 0 else 0.0
    )

    if mapping_coverage_ratio < MIN_STATION_MAPPING_COVERAGE_RATIO:
        raise GoldHydroFeatureError(
            "Hydro station mapping coverage below threshold: "
            f"{mapping_coverage_ratio:.4f} < "
            f"{MIN_STATION_MAPPING_COVERAGE_RATIO:.4f}."
        )

    maximum_station_grid_distance_km = float(mapping["station_grid_distance_km"].max())

    if maximum_station_grid_distance_km > MAX_REASONABLE_STATION_GRID_DISTANCE_KM:
        raise GoldHydroFeatureError(
            "Hydro station mapping produced an unreasonable "
            "station-to-grid distance: "
            f"{maximum_station_grid_distance_km:.3f} km > "
            f"{MAX_REASONABLE_STATION_GRID_DISTANCE_KM:.3f} km."
        )

    mapped = station_month.merge(
        mapping,
        on=["station_id", "province_key"],
        how="left",
        validate="many_to_one",
    )

    mapped_rows = mapped[mapped["grid_cell_key"].notna()].copy()

    if mapped_rows.empty:
        raise GoldHydroFeatureError("No hydro station-month rows were mapped to Gold grid cells.")

    grouped = (
        mapped_rows.groupby(
            [
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
                "measurement_type",
                "reference_month",
            ],
            dropna=False,
        )
        .agg(
            station_count=("station_id", "nunique"),
            daily_record_count=("daily_record_count", "sum"),
            observation_day_count=("observation_day_count", "sum"),
            measurement_observation_count=(
                "measurement_observation_count",
                "sum",
            ),
            mean_measurement_value=("mean_measurement_value", "mean"),
            min_measurement_value=("min_measurement_value", "min"),
            max_measurement_value=("max_measurement_value", "max"),
            median_measurement_value=(
                "median_measurement_value",
                "mean",
            ),
            p95_measurement_value=("p95_measurement_value", "mean"),
            mean_measurement_completeness_ratio=(
                "measurement_completeness_ratio",
                "mean",
            ),
            flow_zero_day_count=("flow_zero_day_count", "sum"),
            negative_value_count=("negative_value_count", "sum"),
            nearest_station_distance_km=(
                "station_grid_distance_km",
                "min",
            ),
            mean_station_distance_km=(
                "station_grid_distance_km",
                "mean",
            ),
        )
        .reset_index()
    )

    grouped["hydro_feature_quality_flag"] = np.select(
        [
            grouped["mean_measurement_completeness_ratio"] >= 0.9,
            grouped["mean_measurement_completeness_ratio"] >= 0.7,
            grouped["mean_measurement_completeness_ratio"] >= 0.4,
        ],
        [
            "high",
            "medium",
            "low",
        ],
        default="very_low",
    )

    grouped["grid_month_hydro_feature_key"] = (
        grouped["grid_cell_key"].astype(str)
        + "__"
        + grouped["measurement_type"].astype(str)
        + "__"
        + grouped["reference_month"].astype(str)
    )

    grouped = grouped.sort_values(
        [
            "reference_month",
            "grid_system",
            "grid_cell_key",
            "measurement_type",
        ]
    ).reset_index(drop=True)

    summary = {
        "station_month_row_count": int(len(station_month)),
        "grid_month_row_count": int(len(grouped)),
        "station_count": int(station_month["station_id"].nunique()),
        "mapped_station_count": int(mapping["station_id"].nunique()),
        "unmapped_station_count": int(
            station_month["station_id"].nunique() - mapping["station_id"].nunique()
        ),
        "month_count": int(grouped["reference_month"].nunique()),
        "measurement_types": sorted(grouped["measurement_type"].dropna().unique().tolist()),
    }

    return grouped, summary


def _map_hydro_stations_to_grid_cells(
    *,
    station_month: pd.DataFrame,
    grid: pd.DataFrame,
) -> pd.DataFrame:
    crs_values = {int(value) for value in grid["crs_epsg"].dropna().unique()}

    if crs_values != {ANALYSIS_CRS_EPSG}:
        raise GoldHydroFeatureError(
            "Gold grid must use EPSG:3347 for hydro station mapping; "
            f"found {sorted(crs_values)}."
        )

    station_points = (
        station_month.assign(
            station_id=station_month["station_id"].astype(str),
            province_key=station_month["province_key"].astype(str),
        )
        .groupby(["station_id", "province_key"], as_index=False)
        .agg(
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
        )
    )

    transformer = Transformer.from_crs(
        DISPLAY_CRS_EPSG,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )

    rows: list[dict[str, Any]] = []

    for province_key, grid_group in grid.groupby("province_key"):
        province_stations = station_points[station_points["province_key"] == province_key].copy()

        if province_stations.empty:
            continue

        geometries = shapely.from_wkt(grid_group["analysis_geometry_wkt"].astype(str).to_numpy())
        tree = STRtree(geometries)

        grid_lookup = grid_group.reset_index(drop=True)

        for station in province_stations.itertuples(index=False):
            x, y = transformer.transform(
                float(station.longitude),
                float(station.latitude),
            )
            point = Point(x, y)

            candidate_indices = tree.query(point, predicate="intersects")

            if len(candidate_indices) == 0:
                nearest_index = int(tree.nearest(point))
                grid_row = grid_lookup.iloc[nearest_index]
                distance_km = point.distance(geometries[nearest_index]) / 1_000
                mapping_method = "nearest_grid_cell"
            else:
                candidate_rows = grid_lookup.iloc[
                    [int(index) for index in candidate_indices]
                ].sort_values("grid_cell_key")
                grid_row = candidate_rows.iloc[0]
                distance_km = 0.0
                mapping_method = "point_within_grid_cell"

            rows.append(
                {
                    "station_id": str(station.station_id),
                    "province_key": str(province_key),
                    "grid_cell_key": grid_row["grid_cell_key"],
                    "grid_system": grid_row["grid_system"],
                    "grid_level": grid_row["grid_level"],
                    "grid_version": grid_row["grid_version"],
                    "station_projected_x": x,
                    "station_projected_y": y,
                    "station_grid_distance_km": distance_km,
                    "station_grid_mapping_method": mapping_method,
                }
            )

    mapping_columns = [
        "station_id",
        "province_key",
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "station_projected_x",
        "station_projected_y",
        "station_grid_distance_km",
        "station_grid_mapping_method",
    ]

    return pd.DataFrame(rows, columns=mapping_columns)
