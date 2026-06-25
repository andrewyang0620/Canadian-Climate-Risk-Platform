from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.gold.common.io import latest_partitioned_table_parquet_files
from src.gold.spatial.grid import ANALYSIS_CRS_EPSG


DISPLAY_CRS_EPSG = 4326

HEAVY_PRECIP_THRESHOLD_MM = 10.0
EXTREME_HEAT_THRESHOLD_C = 30.0
EXTREME_COLD_THRESHOLD_C = -20.0

MIN_STATION_MAPPING_COVERAGE_RATIO = 0.95
MAX_REASONABLE_STATION_GRID_DISTANCE_KM = 50.0


class GoldClimateFeatureError(Exception):
    """Raised when Gold climate feature generation fails."""


@dataclass(frozen=True)
class ClimateFeatureSummary:
    station_month_row_count: int
    grid_month_row_count: int
    station_count: int
    mapped_station_count: int
    unmapped_station_count: int
    month_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "station_month_row_count": self.station_month_row_count,
            "grid_month_row_count": self.grid_month_row_count,
            "station_count": self.station_count,
            "mapped_station_count": self.mapped_station_count,
            "unmapped_station_count": self.unmapped_station_count,
            "month_count": self.month_count,
        }


def read_silver_climate_daily(
    *,
    silver_climate_root: str | Path,
) -> pd.DataFrame:
    paths = latest_partitioned_table_parquet_files(
        table_root=silver_climate_root,
        partition_pattern="observation_year=*/*.parquet",
    )

    frames = [pd.read_parquet(path) for path in paths]

    return pd.concat(frames, ignore_index=True)


def build_gold_climate_station_month_feature(
    climate_daily: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "station_id",
        "station_name",
        "province",
        "observation_date",
        "latitude",
        "longitude",
        "mean_temp_c",
        "min_temp_c",
        "max_temp_c",
        "total_precip_mm",
        "total_rain_mm",
        "total_snow",
    }
    _require_columns(
        climate_daily,
        required_columns,
        "silver_climate_daily",
    )

    df = climate_daily.copy()
    df["observation_date"] = pd.to_datetime(
        df["observation_date"],
        errors="coerce",
    )

    if df["observation_date"].isna().any():
        raise GoldClimateFeatureError(
            "silver_climate_daily contains invalid observation_date values."
        )

    df["reference_month"] = df["observation_date"].dt.to_period("M").astype(str)
    df["province_key"] = df["province"].astype(str).str.upper()

    df["has_temperature_observation"] = (
        df[["mean_temp_c", "min_temp_c", "max_temp_c"]].notna().any(axis=1)
    )
    df["has_precipitation_observation"] = df["total_precip_mm"].notna()
    df["precipitation_day"] = df["total_precip_mm"].fillna(0) > 0
    df["heavy_precipitation_day"] = df["total_precip_mm"].fillna(0) >= HEAVY_PRECIP_THRESHOLD_MM
    df["extreme_heat_day"] = df["max_temp_c"].fillna(-999) >= EXTREME_HEAT_THRESHOLD_C
    df["extreme_cold_day"] = df["min_temp_c"].fillna(999) <= EXTREME_COLD_THRESHOLD_C
    df["freeze_thaw_day"] = (df["min_temp_c"] <= 0) & (df["max_temp_c"] > 0)

    grouped = (
        df.groupby(
            [
                "station_id",
                "province_key",
                "reference_month",
            ],
            dropna=False,
        )
        .agg(
            station_name=("station_name", "first"),
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            daily_record_count=("observation_date", "count"),
            temperature_observation_count=(
                "has_temperature_observation",
                "sum",
            ),
            precipitation_observation_count=(
                "has_precipitation_observation",
                "sum",
            ),
            mean_temp_c=("mean_temp_c", "mean"),
            min_temp_c=("min_temp_c", "mean"),
            max_temp_c=("max_temp_c", "mean"),
            observed_min_temp_c=("min_temp_c", "min"),
            observed_max_temp_c=("max_temp_c", "max"),
            total_precip_mm=("total_precip_mm", _sum_with_min_count),
            total_rain_mm=("total_rain_mm", _sum_with_min_count),
            total_snow=("total_snow", _sum_with_min_count),
            precipitation_days=("precipitation_day", "sum"),
            heavy_precipitation_days=(
                "heavy_precipitation_day",
                "sum",
            ),
            extreme_heat_days=("extreme_heat_day", "sum"),
            extreme_cold_days=("extreme_cold_day", "sum"),
            freeze_thaw_days=("freeze_thaw_day", "sum"),
        )
        .reset_index()
    )

    grouped["days_in_month"] = pd.PeriodIndex(
        grouped["reference_month"],
        freq="M",
    ).days_in_month

    grouped["temperature_completeness_ratio"] = (
        grouped["temperature_observation_count"] / grouped["days_in_month"]
    ).clip(0, 1)

    grouped["precipitation_completeness_ratio"] = (
        grouped["precipitation_observation_count"] / grouped["days_in_month"]
    ).clip(0, 1)

    grouped["climate_station_month_key"] = (
        grouped["province_key"].astype(str)
        + "__"
        + grouped["station_id"].astype(str)
        + "__"
        + grouped["reference_month"].astype(str)
    )

    return grouped.sort_values(["reference_month", "province_key", "station_id"]).reset_index(
        drop=True
    )


def build_gold_grid_month_climate_feature(
    *,
    station_month: pd.DataFrame,
    grid: pd.DataFrame,
) -> tuple[pd.DataFrame, ClimateFeatureSummary]:
    _require_columns(
        station_month,
        {
            "station_id",
            "station_name",
            "province_key",
            "reference_month",
            "latitude",
            "longitude",
            "daily_record_count",
            "temperature_observation_count",
            "precipitation_observation_count",
            "mean_temp_c",
            "min_temp_c",
            "max_temp_c",
            "observed_min_temp_c",
            "observed_max_temp_c",
            "total_precip_mm",
            "total_rain_mm",
            "total_snow",
            "precipitation_days",
            "heavy_precipitation_days",
            "extreme_heat_days",
            "extreme_cold_days",
            "freeze_thaw_days",
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
        },
        "gold_climate_station_month_feature",
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

    grid_for_mapping = grid[grid["grid_system"].isin(["ab_10km", "bc_10km"])].copy()

    mapping = _map_stations_to_grid_cells(
        station_month=station_month,
        grid=grid_for_mapping,
    )

    station_keys = station_month[["station_id", "province_key"]].drop_duplicates()

    if station_keys.empty:
        raise GoldClimateFeatureError(
            "No station-month records are available for climate grid mapping."
        )

    mapped_station_keys = (
        mapping[["station_id", "province_key"]].drop_duplicates()
        if not mapping.empty
        else station_keys.iloc[0:0]
    )

    mapping_coverage_ratio = len(mapped_station_keys) / len(station_keys)

    if mapping_coverage_ratio < MIN_STATION_MAPPING_COVERAGE_RATIO:
        raise GoldClimateFeatureError(
            "Climate station mapping coverage below threshold: "
            f"{mapping_coverage_ratio:.4f} < "
            f"{MIN_STATION_MAPPING_COVERAGE_RATIO:.4f}."
        )

    maximum_station_grid_distance_km = float(mapping["station_grid_distance_km"].max())

    if maximum_station_grid_distance_km > MAX_REASONABLE_STATION_GRID_DISTANCE_KM:
        raise GoldClimateFeatureError(
            "Climate station mapping produced an unreasonable "
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

    mapped["is_station_mapped_to_grid"] = mapped["grid_cell_key"].notna()

    mapped_rows = mapped[mapped["is_station_mapped_to_grid"]].copy()

    if mapped_rows.empty:
        raise GoldClimateFeatureError("No climate stations were mapped to Gold grid cells.")

    grouped = (
        mapped_rows.groupby(
            [
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
                "reference_month",
            ],
            dropna=False,
        )
        .agg(
            station_count=("station_id", "nunique"),
            daily_record_count=("daily_record_count", "sum"),
            temperature_observation_count=(
                "temperature_observation_count",
                "sum",
            ),
            precipitation_observation_count=(
                "precipitation_observation_count",
                "sum",
            ),
            mean_temp_c=("mean_temp_c", "mean"),
            min_temp_c=("min_temp_c", "mean"),
            max_temp_c=("max_temp_c", "mean"),
            observed_min_temp_c=("observed_min_temp_c", "min"),
            observed_max_temp_c=("observed_max_temp_c", "max"),
            total_precip_mm=("total_precip_mm", "mean"),
            total_rain_mm=("total_rain_mm", "mean"),
            total_snow=("total_snow", "mean"),
            precipitation_days=("precipitation_days", "mean"),
            heavy_precipitation_days=(
                "heavy_precipitation_days",
                "mean",
            ),
            extreme_heat_days=("extreme_heat_days", "mean"),
            extreme_cold_days=("extreme_cold_days", "mean"),
            freeze_thaw_days=("freeze_thaw_days", "mean"),
            nearest_station_distance_km=(
                "station_grid_distance_km",
                "min",
            ),
            mean_station_distance_km=(
                "station_grid_distance_km",
                "mean",
            ),
            temperature_completeness_ratio=(
                "temperature_completeness_ratio",
                "mean",
            ),
            precipitation_completeness_ratio=(
                "precipitation_completeness_ratio",
                "mean",
            ),
        )
        .reset_index()
    )

    grouped["climate_data_completeness_score"] = (
        grouped[
            [
                "temperature_completeness_ratio",
                "precipitation_completeness_ratio",
            ]
        ]
        .mean(axis=1)
        .clip(0, 1)
    )

    grouped["climate_feature_quality_flag"] = np.select(
        [
            grouped["climate_data_completeness_score"] >= 0.9,
            grouped["climate_data_completeness_score"] >= 0.7,
            grouped["climate_data_completeness_score"] >= 0.4,
        ],
        [
            "high",
            "medium",
            "low",
        ],
        default="very_low",
    )

    grouped["grid_month_climate_feature_key"] = (
        grouped["grid_cell_key"].astype(str) + "__" + grouped["reference_month"].astype(str)
    )

    grouped = grouped.sort_values(["reference_month", "grid_system", "grid_cell_key"]).reset_index(
        drop=True
    )

    summary = ClimateFeatureSummary(
        station_month_row_count=int(len(station_month)),
        grid_month_row_count=int(len(grouped)),
        station_count=int(station_month["station_id"].nunique()),
        mapped_station_count=int(mapping["station_id"].nunique()),
        unmapped_station_count=int(
            station_month["station_id"].nunique() - mapping["station_id"].nunique()
        ),
        month_count=int(grouped["reference_month"].nunique()),
    )

    return grouped, summary


def _map_stations_to_grid_cells(
    *,
    station_month: pd.DataFrame,
    grid: pd.DataFrame,
) -> pd.DataFrame:
    crs_values = {int(value) for value in grid["crs_epsg"].dropna().unique()}

    if crs_values != {ANALYSIS_CRS_EPSG}:
        raise GoldClimateFeatureError(
            "Gold grid must use EPSG:3347 for climate station mapping; "
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


def _sum_with_min_count(series: pd.Series) -> float:
    return float(series.sum(min_count=1))


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldClimateFeatureError(f"{table_name} is missing columns: {sorted(missing_columns)}")
