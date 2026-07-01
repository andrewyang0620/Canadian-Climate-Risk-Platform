from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.gold.common.io import latest_partitioned_table_parquet_files
from src.gold.spatial.grid import ANALYSIS_CRS_EPSG


DISPLAY_CRS_EPSG = 4326

TARGET_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
IDW_RADIUS_KM = 150.0
IDW_POWER = 2.0
IDW_MIN_STATION_COUNT = 1

HEAVY_PRECIP_THRESHOLD_MM = 10.0
EXTREME_HEAT_THRESHOLD_C = 30.0
EXTREME_COLD_THRESHOLD_C = -20.0


IDENTITY_COLUMNS = [
    "grid_cell_key",
    "grid_system",
    "grid_level",
    "grid_version",
    "province_key",
    "reference_month",
]

CLIMATE_VALUE_COLUMNS = [
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
]


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
    grid_cell_count: int
    direct_station_in_cell_grid_month_count: int
    direct_station_average_in_cell_grid_month_count: int
    idw_interpolated_grid_month_count: int
    no_station_within_radius_grid_month_count: int
    climate_value_coverage_rate: float
    idw_radius_km: float
    idw_min_station_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "station_month_row_count": self.station_month_row_count,
            "grid_month_row_count": self.grid_month_row_count,
            "station_count": self.station_count,
            "mapped_station_count": self.mapped_station_count,
            "unmapped_station_count": self.unmapped_station_count,
            "month_count": self.month_count,
            "grid_cell_count": self.grid_cell_count,
            "direct_station_in_cell_grid_month_count": (
                self.direct_station_in_cell_grid_month_count
            ),
            "direct_station_average_in_cell_grid_month_count": (
                self.direct_station_average_in_cell_grid_month_count
            ),
            "idw_interpolated_grid_month_count": self.idw_interpolated_grid_month_count,
            "no_station_within_radius_grid_month_count": (
                self.no_station_within_radius_grid_month_count
            ),
            "climate_value_coverage_rate": self.climate_value_coverage_rate,
            "idw_radius_km": self.idw_radius_km,
            "idw_min_station_count": self.idw_min_station_count,
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
            *CLIMATE_VALUE_COLUMNS,
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
            "centroid_x",
            "centroid_y",
            "analysis_geometry_wkt",
            "crs_epsg",
        },
        "gold_grid_cell",
    )

    grid_for_mapping = grid[grid["grid_system"].isin(TARGET_GRID_SYSTEMS)].copy()
    grid_for_mapping = grid_for_mapping[grid_for_mapping["province_key"].isin(["AB", "BC"])].copy()

    if grid_for_mapping.empty:
        raise GoldClimateFeatureError("No AB/BC 10km grid cells are available.")

    crs_values = {int(value) for value in grid_for_mapping["crs_epsg"].dropna().unique()}

    if crs_values != {ANALYSIS_CRS_EPSG}:
        raise GoldClimateFeatureError(
            "Gold grid must use EPSG:3347 for climate interpolation; "
            f"found {sorted(crs_values)}."
        )

    station_month = station_month.copy()
    station_month["station_id"] = station_month["station_id"].astype(str)
    station_month["province_key"] = station_month["province_key"].astype(str)
    station_month["station_key"] = (
        station_month["province_key"] + "__" + station_month["station_id"]
    )

    station_locations = _build_station_locations(station_month)
    station_grid_mapping = _map_stations_to_direct_grid_cells(
        station_locations=station_locations,
        grid=grid_for_mapping,
    )

    station_month = station_month.merge(
        station_grid_mapping[
            [
                "station_key",
                "direct_grid_cell_key",
                "station_projected_x",
                "station_projected_y",
            ]
        ],
        on="station_key",
        how="left",
        validate="many_to_one",
    )

    months = sorted(station_month["reference_month"].dropna().astype(str).unique())

    if not months:
        raise GoldClimateFeatureError(
            "No reference months are available for climate grid features."
        )

    static_candidates = _build_static_idw_candidates(
        grid=grid_for_mapping,
        station_locations=station_locations,
    )

    monthly_frames: list[pd.DataFrame] = []

    for reference_month in months:
        month_station = station_month[
            station_month["reference_month"].astype(str) == reference_month
        ].copy()

        for province_key, province_grid in grid_for_mapping.groupby("province_key"):
            province_station = month_station[month_station["province_key"] == province_key].copy()

            monthly_frames.append(
                _build_province_month_grid_features(
                    reference_month=reference_month,
                    province_key=str(province_key),
                    province_grid=province_grid.copy(),
                    province_station=province_station,
                    static_candidates=static_candidates[str(province_key)],
                )
            )

    result = pd.concat(monthly_frames, ignore_index=True)

    result["climate_data_completeness_score"] = result[
        [
            "temperature_completeness_ratio",
            "precipitation_completeness_ratio",
        ]
    ].mean(axis=1)

    result.loc[
        result["climate_mapping_method"] == "no_station_within_radius",
        "climate_data_completeness_score",
    ] = np.nan

    result["climate_feature_quality_flag"] = _build_quality_flag(result)

    result["grid_month_climate_feature_key"] = (
        result["grid_cell_key"].astype(str) + "__" + result["reference_month"].astype(str)
    )

    result = result[
        [
            "grid_month_climate_feature_key",
            *IDENTITY_COLUMNS,
            "climate_mapping_method",
            "climate_station_count",
            "climate_nearest_station_distance_km",
            "climate_mean_station_distance_km",
            "climate_max_station_distance_km",
            "climate_idw_confidence_score",
            *CLIMATE_VALUE_COLUMNS,
            "climate_data_completeness_score",
            "climate_feature_quality_flag",
        ]
    ]

    result = result.sort_values(["reference_month", "grid_system", "grid_cell_key"]).reset_index(
        drop=True
    )

    method_counts = result["climate_mapping_method"].value_counts().to_dict()
    no_station_count = int(method_counts.get("no_station_within_radius", 0))

    summary = ClimateFeatureSummary(
        station_month_row_count=int(len(station_month)),
        grid_month_row_count=int(len(result)),
        station_count=int(station_month["station_key"].nunique()),
        mapped_station_count=int(station_grid_mapping["direct_grid_cell_key"].notna().sum()),
        unmapped_station_count=int(station_grid_mapping["direct_grid_cell_key"].isna().sum()),
        month_count=int(result["reference_month"].nunique()),
        grid_cell_count=int(result["grid_cell_key"].nunique()),
        direct_station_in_cell_grid_month_count=int(method_counts.get("direct_station_in_cell", 0)),
        direct_station_average_in_cell_grid_month_count=int(
            method_counts.get("direct_station_average_in_cell", 0)
        ),
        idw_interpolated_grid_month_count=int(method_counts.get("idw_interpolated", 0)),
        no_station_within_radius_grid_month_count=no_station_count,
        climate_value_coverage_rate=float(1 - (no_station_count / len(result))),
        idw_radius_km=IDW_RADIUS_KM,
        idw_min_station_count=IDW_MIN_STATION_COUNT,
    )

    return result, summary


def _build_station_locations(station_month: pd.DataFrame) -> pd.DataFrame:
    station_locations = (
        station_month.groupby(["station_key", "station_id", "province_key"], as_index=False)
        .agg(
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
        )
        .dropna(subset=["latitude", "longitude"])
    )

    transformer = Transformer.from_crs(
        DISPLAY_CRS_EPSG,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )

    x_values, y_values = transformer.transform(
        station_locations["longitude"].astype(float).tolist(),
        station_locations["latitude"].astype(float).tolist(),
    )

    station_locations["station_projected_x"] = x_values
    station_locations["station_projected_y"] = y_values

    return station_locations


def _map_stations_to_direct_grid_cells(
    *,
    station_locations: pd.DataFrame,
    grid: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for province_key, grid_group in grid.groupby("province_key"):
        province_stations = station_locations[
            station_locations["province_key"] == province_key
        ].copy()

        if province_stations.empty:
            continue

        geometries = shapely.from_wkt(grid_group["analysis_geometry_wkt"].astype(str).to_numpy())
        tree = STRtree(geometries)
        grid_lookup = grid_group.reset_index(drop=True)

        for station in province_stations.itertuples(index=False):
            point = Point(
                float(station.station_projected_x),
                float(station.station_projected_y),
            )

            candidate_indices = tree.query(point, predicate="intersects")

            direct_grid_cell_key = None

            if len(candidate_indices) > 0:
                candidate_rows = grid_lookup.iloc[
                    [int(index) for index in candidate_indices]
                ].sort_values("grid_cell_key")
                direct_grid_cell_key = str(candidate_rows.iloc[0]["grid_cell_key"])

            rows.append(
                {
                    "station_key": str(station.station_key),
                    "station_id": str(station.station_id),
                    "province_key": str(province_key),
                    "direct_grid_cell_key": direct_grid_cell_key,
                    "station_projected_x": float(station.station_projected_x),
                    "station_projected_y": float(station.station_projected_y),
                }
            )

    return pd.DataFrame(
        rows,
        columns=[
            "station_key",
            "station_id",
            "province_key",
            "direct_grid_cell_key",
            "station_projected_x",
            "station_projected_y",
        ],
    )


def _build_static_idw_candidates(
    *,
    grid: pd.DataFrame,
    station_locations: pd.DataFrame,
) -> dict[str, dict[str, list[tuple[str, float]]]]:
    candidates_by_province: dict[str, dict[str, list[tuple[str, float]]]] = {}

    for province_key, grid_group in grid.groupby("province_key"):
        province_key = str(province_key)
        province_stations = station_locations[
            station_locations["province_key"] == province_key
        ].copy()

        province_candidates: dict[str, list[tuple[str, float]]] = {}

        if province_stations.empty:
            candidates_by_province[province_key] = province_candidates
            continue

        station_points = (
            province_stations[["station_projected_x", "station_projected_y"]]
            .astype(float)
            .to_numpy()
        )
        station_keys = province_stations["station_key"].astype(str).tolist()

        grid_points = grid_group[["centroid_x", "centroid_y"]].astype(float).to_numpy()
        grid_keys = grid_group["grid_cell_key"].astype(str).tolist()

        for grid_key, grid_point in zip(grid_keys, grid_points, strict=True):
            diff = station_points - grid_point
            distances_km = np.sqrt(np.sum(diff * diff, axis=1)) / 1_000
            nearby_mask = distances_km <= IDW_RADIUS_KM

            candidate_pairs = [
                (station_key, float(distance))
                for station_key, distance, is_nearby in zip(
                    station_keys,
                    distances_km,
                    nearby_mask,
                    strict=True,
                )
                if is_nearby
            ]

            candidate_pairs.sort(key=lambda item: (item[1], item[0]))
            province_candidates[grid_key] = candidate_pairs

        candidates_by_province[province_key] = province_candidates

    return candidates_by_province


def _build_province_month_grid_features(
    *,
    reference_month: str,
    province_key: str,
    province_grid: pd.DataFrame,
    province_station: pd.DataFrame,
    static_candidates: dict[str, list[tuple[str, float]]],
) -> pd.DataFrame:
    base = province_grid[
        [
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
        ]
    ].copy()
    base["reference_month"] = reference_month

    direct_features = _aggregate_direct_features(province_station)

    feature_lookup = {
        str(row["grid_cell_key"]): row for row in direct_features.to_dict(orient="records")
    }

    station_by_key = {str(row.station_key): row for row in province_station.itertuples(index=False)}

    rows: list[dict[str, Any]] = []

    for grid_row in base.itertuples(index=False):
        grid_cell_key = str(grid_row.grid_cell_key)

        if grid_cell_key in feature_lookup:
            feature = feature_lookup[grid_cell_key]
        else:
            candidate_pairs = [
                (station_key, distance_km)
                for station_key, distance_km in static_candidates.get(grid_cell_key, [])
                if station_key in station_by_key
            ]

            if len(candidate_pairs) >= IDW_MIN_STATION_COUNT:
                feature = _build_idw_feature(
                    grid_cell_key=grid_cell_key,
                    candidate_pairs=candidate_pairs,
                    station_by_key=station_by_key,
                )
            else:
                feature = _build_no_station_feature(grid_cell_key=grid_cell_key)

        row = {
            "grid_cell_key": grid_cell_key,
            "grid_system": grid_row.grid_system,
            "grid_level": grid_row.grid_level,
            "grid_version": grid_row.grid_version,
            "province_key": grid_row.province_key,
            "reference_month": reference_month,
            **{key: value for key, value in feature.items() if key != "grid_cell_key"},
        }
        rows.append(row)

    return pd.DataFrame(rows)


def _aggregate_direct_features(province_station: pd.DataFrame) -> pd.DataFrame:
    direct_rows = province_station[province_station["direct_grid_cell_key"].notna()].copy()

    if direct_rows.empty:
        return pd.DataFrame(columns=["grid_cell_key"])

    direct_rows["grid_cell_key"] = direct_rows["direct_grid_cell_key"].astype(str)

    grouped = (
        direct_rows.groupby("grid_cell_key", dropna=False)
        .agg(
            climate_station_count=("station_id", "nunique"),
            daily_record_count=("daily_record_count", "sum"),
            temperature_observation_count=("temperature_observation_count", "sum"),
            precipitation_observation_count=("precipitation_observation_count", "sum"),
            mean_temp_c=("mean_temp_c", "mean"),
            min_temp_c=("min_temp_c", "mean"),
            max_temp_c=("max_temp_c", "mean"),
            observed_min_temp_c=("observed_min_temp_c", "min"),
            observed_max_temp_c=("observed_max_temp_c", "max"),
            total_precip_mm=("total_precip_mm", "mean"),
            total_rain_mm=("total_rain_mm", "mean"),
            total_snow=("total_snow", "mean"),
            precipitation_days=("precipitation_days", "mean"),
            heavy_precipitation_days=("heavy_precipitation_days", "mean"),
            extreme_heat_days=("extreme_heat_days", "mean"),
            extreme_cold_days=("extreme_cold_days", "mean"),
            freeze_thaw_days=("freeze_thaw_days", "mean"),
            temperature_completeness_ratio=("temperature_completeness_ratio", "mean"),
            precipitation_completeness_ratio=("precipitation_completeness_ratio", "mean"),
        )
        .reset_index()
    )

    grouped["climate_mapping_method"] = np.where(
        grouped["climate_station_count"] == 1,
        "direct_station_in_cell",
        "direct_station_average_in_cell",
    )
    grouped["climate_nearest_station_distance_km"] = 0.0
    grouped["climate_mean_station_distance_km"] = 0.0
    grouped["climate_max_station_distance_km"] = 0.0
    grouped["climate_idw_confidence_score"] = 1.0

    return grouped


def _build_idw_feature(
    *,
    grid_cell_key: str,
    candidate_pairs: list[tuple[str, float]],
    station_by_key: dict[str, Any],
) -> dict[str, Any]:
    distances = np.array([distance for _, distance in candidate_pairs], dtype=float)
    weights = 1 / np.power(np.maximum(distances, 0.001), IDW_POWER)
    weights = weights / weights.sum()

    station_rows = [station_by_key[station_key] for station_key, _ in candidate_pairs]

    feature: dict[str, Any] = {
        "grid_cell_key": grid_cell_key,
        "climate_mapping_method": "idw_interpolated",
        "climate_station_count": len(candidate_pairs),
        "climate_nearest_station_distance_km": float(distances.min()),
        "climate_mean_station_distance_km": float(distances.mean()),
        "climate_max_station_distance_km": float(distances.max()),
        "climate_idw_confidence_score": _build_idw_confidence_score(
            station_count=len(candidate_pairs),
            nearest_distance_km=float(distances.min()),
            mean_distance_km=float(distances.mean()),
        ),
    }

    for column in CLIMATE_VALUE_COLUMNS:
        values = np.array(
            [
                (
                    np.nan
                    if pd.isna(getattr(station_row, column))
                    else float(getattr(station_row, column))
                )
                for station_row in station_rows
            ],
            dtype=float,
        )

        valid_mask = ~np.isnan(values)

        if not valid_mask.any():
            feature[column] = np.nan
            continue

        valid_weights = weights[valid_mask]
        valid_weights = valid_weights / valid_weights.sum()
        feature[column] = float(np.sum(values[valid_mask] * valid_weights))

    return feature


def _build_no_station_feature(*, grid_cell_key: str) -> dict[str, Any]:
    feature: dict[str, Any] = {
        "grid_cell_key": grid_cell_key,
        "climate_mapping_method": "no_station_within_radius",
        "climate_station_count": 0,
        "climate_nearest_station_distance_km": np.nan,
        "climate_mean_station_distance_km": np.nan,
        "climate_max_station_distance_km": np.nan,
        "climate_idw_confidence_score": 0.0,
    }

    for column in CLIMATE_VALUE_COLUMNS:
        feature[column] = np.nan

    return feature


def _build_idw_confidence_score(
    *,
    station_count: int,
    nearest_distance_km: float,
    mean_distance_km: float,
) -> float:
    station_score = min(station_count / 5, 1.0)
    nearest_distance_score = max(0.0, 1.0 - (nearest_distance_km / IDW_RADIUS_KM))
    mean_distance_score = max(0.0, 1.0 - (mean_distance_km / IDW_RADIUS_KM))

    return float(
        np.clip(
            (0.4 * station_score) + (0.3 * nearest_distance_score) + (0.3 * mean_distance_score),
            0,
            1,
        )
    )


def _build_quality_flag(result: pd.DataFrame) -> pd.Series:
    score = result["climate_data_completeness_score"].fillna(0)
    confidence = result["climate_idw_confidence_score"].fillna(0)
    combined_score = (0.7 * score) + (0.3 * confidence)

    quality = pd.Series(pd.NA, index=result.index, dtype="object")

    direct_mask = result["climate_mapping_method"].isin(
        [
            "direct_station_in_cell",
            "direct_station_average_in_cell",
        ]
    )
    idw_mask = result["climate_mapping_method"] == "idw_interpolated"

    quality.loc[direct_mask] = "direct"
    quality.loc[idw_mask & (combined_score >= 0.9)] = "high"
    quality.loc[idw_mask & (combined_score >= 0.7) & (combined_score < 0.9)] = "medium"
    quality.loc[idw_mask & (combined_score >= 0.4) & (combined_score < 0.7)] = "low"
    quality.loc[idw_mask & (combined_score < 0.4)] = "very_low"

    return quality


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
