from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from shapely.geometry import Point
from shapely.ops import transform as shapely_transform
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

HYDRO_SPATIAL_METHOD_BASIN = "basin_polygon_intersection"
HYDRO_SPATIAL_METHOD_POINT = "station_point_in_cell"
HYDRO_SPATIAL_METHOD_NONE = "no_hydro_coverage"

GRID_MONTH_IDENTITY_COLUMNS = [
    "grid_cell_key",
    "grid_system",
    "grid_level",
    "grid_version",
    "province_key",
    "reference_month",
]


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


def read_silver_hydro_basin_polygon(
    *,
    silver_root: str | Path = "lakehouse/silver",
) -> pd.DataFrame:
    basin_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_hydro_basin_polygon",
    )

    return pd.read_parquet(basin_path)


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
    basin_polygon: pd.DataFrame,
    start_month: str = HYDRO_ANALYSIS_START_MONTH,
    end_month: str = HYDRO_ANALYSIS_END_MONTH,
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

    _require_columns(
        basin_polygon,
        {
            "station_id",
            "geometry_wkt",
        },
        "silver_hydro_basin_polygon",
    )

    grid_for_mapping = grid[grid["grid_system"].isin(EXPECTED_HYDRO_GRID_SYSTEMS)].copy()

    if grid_for_mapping.empty:
        raise GoldHydroFeatureError("gold_grid_cell does not contain AB/BC 10km grid systems.")

    crs_values = {int(value) for value in grid_for_mapping["crs_epsg"].dropna().unique()}

    if crs_values != {ANALYSIS_CRS_EPSG}:
        raise GoldHydroFeatureError(
            "Gold grid must use EPSG:3347 for hydro basin mapping; " f"found {sorted(crs_values)}."
        )

    station_month = station_month.copy()
    station_month["station_id"] = station_month["station_id"].astype(str)
    station_month["station_id_norm"] = station_month["station_id"].map(_normalize_station_id)
    station_month["province_key"] = station_month["province_key"].astype(str).str.upper()
    station_month["measurement_type"] = station_month["measurement_type"].astype(str).str.lower()

    unexpected_measurement_types = (
        set(station_month["measurement_type"].dropna().unique()) - EXPECTED_HYDRO_MEASUREMENT_TYPES
    )

    if unexpected_measurement_types:
        raise GoldHydroFeatureError(
            "gold_hydro_station_month_feature contains unexpected measurement_type values: "
            f"{sorted(unexpected_measurement_types)}."
        )

    observed_station_ids = set(station_month["station_id_norm"].dropna().unique())

    basin_polygon = basin_polygon.copy()
    basin_polygon["station_id_norm"] = basin_polygon["station_id"].map(_normalize_station_id)
    basin_station_ids = set(basin_polygon["station_id_norm"].dropna().unique())
    basin_matched_station_ids = observed_station_ids & basin_station_ids
    basin_unmatched_station_ids = observed_station_ids - basin_station_ids

    basin_mapping = _build_basin_grid_mapping(
        basin_polygon=basin_polygon,
        grid=grid_for_mapping,
        observed_station_ids=basin_matched_station_ids,
    )

    basin_covered_grid_keys = (
        set(basin_mapping["grid_cell_key"].astype(str).unique())
        if not basin_mapping.empty
        else set()
    )

    point_mapping = _build_station_point_in_cell_mapping(
        station_month=station_month,
        grid=grid_for_mapping,
        station_ids=basin_unmatched_station_ids,
        excluded_grid_cell_keys=basin_covered_grid_keys,
    )

    spatial_mapping_frames = [frame for frame in [basin_mapping, point_mapping] if not frame.empty]

    if spatial_mapping_frames:
        spatial_mapping = pd.concat(
            spatial_mapping_frames,
            ignore_index=True,
        )
    else:
        spatial_mapping = pd.DataFrame(columns=basin_mapping.columns)

    spatial_summary = _build_spatial_grid_summary(
        spatial_mapping=spatial_mapping,
    )

    hydro_features = _build_grid_month_hydro_values(
        station_month=station_month,
        spatial_mapping=spatial_mapping,
    )

    skeleton = _build_grid_month_skeleton(
        grid=grid_for_mapping,
        start_month=start_month,
        end_month=end_month,
    )

    result = skeleton.merge(
        spatial_summary,
        on=[
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
        ],
        how="left",
        validate="many_to_one",
    )

    result["hydro_spatial_assignment_method"] = result["hydro_spatial_assignment_method"].fillna(
        HYDRO_SPATIAL_METHOD_NONE
    )

    result = result.merge(
        hydro_features,
        on=[
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
        ],
        how="left",
        validate="one_to_one",
    )

    _fill_count_columns(result)
    result["hydro_data_completeness_score"] = _build_hydro_data_completeness_score(result)
    result["hydro_feature_quality_flag"] = _build_hydro_quality_flag(result)

    result["grid_month_hydro_feature_key"] = (
        result["grid_cell_key"].astype(str) + "__" + result["reference_month"].astype(str)
    )

    ordered_columns = [
        "grid_month_hydro_feature_key",
        *GRID_MONTH_IDENTITY_COLUMNS,
        "hydro_spatial_assignment_method",
        "hydro_station_count",
        "hydro_basin_station_count",
        "hydro_point_station_count",
        "hydro_basin_intersection_area_sq_km",
        "hydro_basin_grid_coverage_ratio",
        "flow_station_count",
        "flow_daily_record_count",
        "flow_observation_day_count",
        "flow_measurement_observation_count",
        "flow_mean_measurement_value",
        "flow_min_measurement_value",
        "flow_max_measurement_value",
        "flow_median_measurement_value",
        "flow_p95_measurement_value",
        "flow_measurement_completeness_ratio",
        "flow_zero_day_count",
        "flow_negative_value_count",
        "level_station_count",
        "level_daily_record_count",
        "level_observation_day_count",
        "level_measurement_observation_count",
        "level_mean_measurement_value",
        "level_min_measurement_value",
        "level_max_measurement_value",
        "level_median_measurement_value",
        "level_p95_measurement_value",
        "level_measurement_completeness_ratio",
        "level_negative_value_count",
        "hydro_data_completeness_score",
        "hydro_feature_quality_flag",
    ]

    for column in ordered_columns:
        if column not in result.columns:
            result[column] = pd.NA

    result = (
        result[ordered_columns]
        .sort_values(
            [
                "reference_month",
                "grid_system",
                "grid_cell_key",
            ]
        )
        .reset_index(drop=True)
    )

    summary = {
        "station_month_row_count": int(len(station_month)),
        "grid_month_row_count": int(len(result)),
        "station_count": int(station_month["station_id"].nunique()),
        "basin_polygon_station_count": int(basin_polygon["station_id_norm"].nunique()),
        "basin_matched_station_count": int(len(basin_matched_station_ids)),
        "basin_unmatched_station_count": int(len(basin_unmatched_station_ids)),
        "basin_match_rate": (
            len(basin_matched_station_ids) / len(observed_station_ids)
            if observed_station_ids
            else 0.0
        ),
        "basin_intersection_row_count": int(len(basin_mapping)),
        "basin_covered_grid_count": int(len(basin_covered_grid_keys)),
        "point_in_cell_station_count": (
            int(point_mapping["station_id_norm"].nunique()) if not point_mapping.empty else 0
        ),
        "point_in_cell_grid_count": (
            int(point_mapping["grid_cell_key"].nunique()) if not point_mapping.empty else 0
        ),
        "covered_grid_count": int(
            result.loc[
                result["hydro_spatial_assignment_method"] != HYDRO_SPATIAL_METHOD_NONE,
                "grid_cell_key",
            ].nunique()
        ),
        "no_hydro_coverage_grid_count": int(
            result.loc[
                result["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_NONE,
                "grid_cell_key",
            ].nunique()
        ),
        "month_count": int(result["reference_month"].nunique()),
        "measurement_types": sorted(station_month["measurement_type"].dropna().unique().tolist()),
        "spatial_assignment_method_counts": {
            str(key): int(value)
            for key, value in result[["grid_cell_key", "hydro_spatial_assignment_method"]]
            .drop_duplicates()["hydro_spatial_assignment_method"]
            .value_counts(dropna=False)
            .to_dict()
            .items()
        },
    }

    return result, summary


def _p95(series: pd.Series) -> float:
    return float(series.quantile(0.95))


def _normalize_station_id(value: object) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip().upper()

    return "".join(character for character in text if character.isalnum())


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


def _build_basin_grid_mapping(
    *,
    basin_polygon: pd.DataFrame,
    grid: pd.DataFrame,
    observed_station_ids: set[str],
) -> pd.DataFrame:
    mapping_columns = [
        "station_id_norm",
        "province_key",
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "hydro_spatial_assignment_method",
        "hydro_basin_intersection_area_sq_km",
        "hydro_basin_grid_coverage_ratio",
        "spatial_weight",
    ]

    basin = basin_polygon[basin_polygon["station_id_norm"].isin(observed_station_ids)].copy()

    if basin.empty:
        return pd.DataFrame(columns=mapping_columns)

    if "crs_epsg" in basin.columns:
        basin_crs_values = {int(value) for value in basin["crs_epsg"].dropna().unique()}

        if len(basin_crs_values) > 1:
            raise GoldHydroFeatureError(
                "silver_hydro_basin_polygon must contain at most one crs_epsg value; "
                f"found {sorted(basin_crs_values)}."
            )

        basin_crs_epsg = basin_crs_values.pop() if basin_crs_values else DISPLAY_CRS_EPSG
    else:
        basin_crs_epsg = DISPLAY_CRS_EPSG

    grid = grid.reset_index(drop=True)

    grid_geometries = shapely.from_wkt(grid["analysis_geometry_wkt"].astype(str).to_numpy())
    grid_area_sq_km_values = np.asarray(
        shapely.area(grid_geometries) / 1_000_000,
        dtype=float,
    )

    grid_cell_key_values = grid["grid_cell_key"].astype(str).to_numpy()
    grid_system_values = grid["grid_system"].astype(str).to_numpy()
    grid_level_values = grid["grid_level"].astype(str).to_numpy()
    grid_version_values = grid["grid_version"].astype(str).to_numpy()
    province_key_values = grid["province_key"].astype(str).to_numpy()

    tree = STRtree(grid_geometries)

    mapping_frames: list[pd.DataFrame] = []
    basin_count = len(basin)

    print(
        "[INFO] building hydro basin-grid intersections | "
        f"basins={basin_count} grids={len(grid)}"
    )

    current_row_count = 0

    for basin_index, basin_row in enumerate(
        basin.itertuples(index=False),
        start=1,
    ):
        if basin_index == 1 or basin_index % 25 == 0 or basin_index == basin_count:
            print(
                "[INFO] basin-grid intersection progress | "
                f"{basin_index}/{basin_count} rows={current_row_count}"
            )

        basin_geometry = shapely.from_wkt(str(basin_row.geometry_wkt))
        basin_geometry = _project_geometry_if_needed(
            geometry=basin_geometry,
            source_epsg=basin_crs_epsg,
        )

        if basin_geometry.is_empty:
            continue

        if not basin_geometry.is_valid:
            basin_geometry = shapely.make_valid(basin_geometry)

        candidate_indices = tree.query(
            basin_geometry,
            predicate="intersects",
        )

        if len(candidate_indices) == 0:
            continue

        candidate_indices = np.asarray(candidate_indices, dtype=int)
        candidate_geometries = grid_geometries[candidate_indices]

        intersections = shapely.intersection(
            candidate_geometries,
            basin_geometry,
        )
        intersection_area_sq_km_values = np.asarray(
            shapely.area(intersections) / 1_000_000,
            dtype=float,
        )

        valid_mask = intersection_area_sq_km_values > 0

        if not bool(valid_mask.any()):
            continue

        valid_indices = candidate_indices[valid_mask]
        valid_intersection_area_sq_km_values = intersection_area_sq_km_values[valid_mask]
        valid_grid_area_sq_km_values = grid_area_sq_km_values[valid_indices]

        positive_grid_area_mask = valid_grid_area_sq_km_values > 0

        if not bool(positive_grid_area_mask.any()):
            continue

        valid_indices = valid_indices[positive_grid_area_mask]
        valid_intersection_area_sq_km_values = valid_intersection_area_sq_km_values[
            positive_grid_area_mask
        ]
        valid_grid_area_sq_km_values = valid_grid_area_sq_km_values[positive_grid_area_mask]

        grid_intersection_ratio_values = np.minimum(
            valid_intersection_area_sq_km_values / valid_grid_area_sq_km_values,
            1.0,
        )

        frame = pd.DataFrame(
            {
                "station_id_norm": str(basin_row.station_id_norm),
                "province_key": province_key_values[valid_indices],
                "grid_cell_key": grid_cell_key_values[valid_indices],
                "grid_system": grid_system_values[valid_indices],
                "grid_level": grid_level_values[valid_indices],
                "grid_version": grid_version_values[valid_indices],
                "hydro_spatial_assignment_method": HYDRO_SPATIAL_METHOD_BASIN,
                "hydro_basin_intersection_area_sq_km": valid_intersection_area_sq_km_values.astype(
                    float
                ),
                "hydro_basin_grid_coverage_ratio": grid_intersection_ratio_values.astype(float),
                "spatial_weight": grid_intersection_ratio_values.astype(float),
            }
        )

        current_row_count += len(frame)
        mapping_frames.append(frame)

    if mapping_frames:
        result = pd.concat(
            mapping_frames,
            ignore_index=True,
        )
    else:
        result = pd.DataFrame(columns=mapping_columns)

    print("[INFO] completed hydro basin-grid intersections | " f"rows={len(result)}")

    return result[mapping_columns]


def _project_geometry_if_needed(
    *,
    geometry,
    source_epsg: int,
):
    if source_epsg == ANALYSIS_CRS_EPSG:
        return geometry

    transformer = Transformer.from_crs(
        source_epsg,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )

    return shapely_transform(transformer.transform, geometry)


def _build_station_point_in_cell_mapping(
    *,
    station_month: pd.DataFrame,
    grid: pd.DataFrame,
    station_ids: set[str],
    excluded_grid_cell_keys: set[str],
) -> pd.DataFrame:
    mapping_columns = [
        "station_id_norm",
        "province_key",
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "hydro_spatial_assignment_method",
        "hydro_basin_intersection_area_sq_km",
        "hydro_basin_grid_coverage_ratio",
        "spatial_weight",
    ]

    if not station_ids:
        return pd.DataFrame(columns=mapping_columns)

    station_points = (
        station_month[station_month["station_id_norm"].isin(station_ids)]
        .groupby(["station_id_norm", "province_key"], as_index=False)
        .agg(
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
        )
    )

    if station_points.empty:
        return pd.DataFrame(columns=mapping_columns)

    transformer = Transformer.from_crs(
        DISPLAY_CRS_EPSG,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )

    x_values, y_values = transformer.transform(
        station_points["longitude"].astype(float).tolist(),
        station_points["latitude"].astype(float).tolist(),
    )

    station_points["station_projected_x"] = x_values
    station_points["station_projected_y"] = y_values

    rows: list[dict[str, Any]] = []

    for province_key, grid_group in grid.groupby("province_key"):
        province_stations = station_points[station_points["province_key"] == province_key].copy()

        if province_stations.empty:
            continue

        grid_group = grid_group.reset_index(drop=True)
        geometries = shapely.from_wkt(grid_group["analysis_geometry_wkt"].astype(str).to_numpy())
        tree = STRtree(geometries)

        for station in province_stations.itertuples(index=False):
            point = Point(
                float(station.station_projected_x),
                float(station.station_projected_y),
            )

            candidate_indices = tree.query(point, predicate="intersects")

            if len(candidate_indices) == 0:
                continue

            candidate_rows = grid_group.iloc[
                [int(index) for index in candidate_indices]
            ].sort_values("grid_cell_key")

            grid_row = candidate_rows.iloc[0]
            grid_cell_key = str(grid_row["grid_cell_key"])

            if grid_cell_key in excluded_grid_cell_keys:
                continue

            rows.append(
                {
                    "station_id_norm": str(station.station_id_norm),
                    "province_key": str(grid_row["province_key"]),
                    "grid_cell_key": grid_cell_key,
                    "grid_system": str(grid_row["grid_system"]),
                    "grid_level": str(grid_row["grid_level"]),
                    "grid_version": str(grid_row["grid_version"]),
                    "hydro_spatial_assignment_method": HYDRO_SPATIAL_METHOD_POINT,
                    "hydro_basin_intersection_area_sq_km": np.nan,
                    "hydro_basin_grid_coverage_ratio": np.nan,
                    "spatial_weight": 1.0,
                }
            )

    return pd.DataFrame(rows, columns=mapping_columns)


def _build_spatial_grid_summary(
    *,
    spatial_mapping: pd.DataFrame,
) -> pd.DataFrame:
    summary_columns = [
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "province_key",
        "hydro_spatial_assignment_method",
        "hydro_station_count",
        "hydro_basin_station_count",
        "hydro_point_station_count",
        "hydro_basin_intersection_area_sq_km",
        "hydro_basin_grid_coverage_ratio",
    ]

    if spatial_mapping.empty:
        return pd.DataFrame(columns=summary_columns)

    basin_mapping = spatial_mapping[
        spatial_mapping["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_BASIN
    ].copy()

    point_mapping = spatial_mapping[
        spatial_mapping["hydro_spatial_assignment_method"] == HYDRO_SPATIAL_METHOD_POINT
    ].copy()

    summaries = []

    if not basin_mapping.empty:
        basin_summary = basin_mapping.groupby(
            [
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
            ],
            as_index=False,
        ).agg(
            hydro_station_count=("station_id_norm", "nunique"),
            hydro_basin_station_count=("station_id_norm", "nunique"),
            hydro_basin_intersection_area_sq_km=(
                "hydro_basin_intersection_area_sq_km",
                "sum",
            ),
            hydro_basin_grid_coverage_ratio=(
                "hydro_basin_grid_coverage_ratio",
                "sum",
            ),
        )

        basin_summary["hydro_spatial_assignment_method"] = HYDRO_SPATIAL_METHOD_BASIN
        basin_summary["hydro_point_station_count"] = 0
        summaries.append(basin_summary)

    if not point_mapping.empty:
        point_summary = point_mapping.groupby(
            [
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
            ],
            as_index=False,
        ).agg(
            hydro_station_count=("station_id_norm", "nunique"),
            hydro_point_station_count=("station_id_norm", "nunique"),
        )

        point_summary["hydro_spatial_assignment_method"] = HYDRO_SPATIAL_METHOD_POINT
        point_summary["hydro_basin_station_count"] = 0
        point_summary["hydro_basin_intersection_area_sq_km"] = np.nan
        point_summary["hydro_basin_grid_coverage_ratio"] = np.nan
        summaries.append(point_summary)

    result = pd.concat(summaries, ignore_index=True)
    result["hydro_basin_grid_coverage_ratio"] = result["hydro_basin_grid_coverage_ratio"].clip(
        upper=1.0
    )

    return result[summary_columns]


def _build_grid_month_hydro_values(
    *,
    station_month: pd.DataFrame,
    spatial_mapping: pd.DataFrame,
) -> pd.DataFrame:
    if spatial_mapping.empty:
        return pd.DataFrame(columns=[*GRID_MONTH_IDENTITY_COLUMNS])

    mapped = station_month.merge(
        spatial_mapping,
        on="station_id_norm",
        how="inner",
        suffixes=("_station", ""),
    )

    if mapped.empty:
        return pd.DataFrame(columns=[*GRID_MONTH_IDENTITY_COLUMNS])

    mapped["station_id"] = mapped["station_id"].astype(str)
    mapped["spatial_weight"] = pd.to_numeric(
        mapped["spatial_weight"],
        errors="coerce",
    ).fillna(1.0)

    group_keys = [
        *GRID_MONTH_IDENTITY_COLUMNS,
        "measurement_type",
    ]

    weighted_columns = [
        "mean_measurement_value",
        "median_measurement_value",
        "p95_measurement_value",
        "measurement_completeness_ratio",
    ]

    mapped["_w"] = mapped["spatial_weight"].astype(float)

    for column in weighted_columns:
        value = pd.to_numeric(
            mapped[column],
            errors="coerce",
        )
        valid_weight = mapped["_w"].where(
            value.notna() & mapped["_w"].gt(0),
            0.0,
        )

        mapped[f"_wv_{column}"] = value.fillna(0.0) * valid_weight
        mapped[f"_ws_{column}"] = valid_weight

    grouped = (
        mapped.groupby(
            group_keys,
            dropna=False,
        )
        .agg(
            station_count=("station_id", "nunique"),
            daily_record_count=("daily_record_count", "sum"),
            observation_day_count=("observation_day_count", "sum"),
            measurement_observation_count=("measurement_observation_count", "sum"),
            min_measurement_value=("min_measurement_value", "min"),
            max_measurement_value=("max_measurement_value", "max"),
            zero_day_count=("flow_zero_day_count", "sum"),
            negative_value_count=("negative_value_count", "sum"),
            _wv_mean_measurement_value=("_wv_mean_measurement_value", "sum"),
            _ws_mean_measurement_value=("_ws_mean_measurement_value", "sum"),
            _wv_median_measurement_value=("_wv_median_measurement_value", "sum"),
            _ws_median_measurement_value=("_ws_median_measurement_value", "sum"),
            _wv_p95_measurement_value=("_wv_p95_measurement_value", "sum"),
            _ws_p95_measurement_value=("_ws_p95_measurement_value", "sum"),
            _wv_measurement_completeness_ratio=(
                "_wv_measurement_completeness_ratio",
                "sum",
            ),
            _ws_measurement_completeness_ratio=(
                "_ws_measurement_completeness_ratio",
                "sum",
            ),
        )
        .reset_index()
    )

    weighted_output_columns = {
        "mean_measurement_value": (
            "_wv_mean_measurement_value",
            "_ws_mean_measurement_value",
        ),
        "median_measurement_value": (
            "_wv_median_measurement_value",
            "_ws_median_measurement_value",
        ),
        "p95_measurement_value": (
            "_wv_p95_measurement_value",
            "_ws_p95_measurement_value",
        ),
        "measurement_completeness_ratio": (
            "_wv_measurement_completeness_ratio",
            "_ws_measurement_completeness_ratio",
        ),
    }

    for output_column, (
        weighted_value_column,
        weight_sum_column,
    ) in weighted_output_columns.items():
        grouped[output_column] = np.where(
            grouped[weight_sum_column].gt(0),
            grouped[weighted_value_column] / grouped[weight_sum_column],
            np.nan,
        )

    grouped = grouped.drop(
        columns=[
            column
            for column in grouped.columns
            if column.startswith("_wv_") or column.startswith("_ws_")
        ]
    )

    if grouped.empty:
        return pd.DataFrame(columns=[*GRID_MONTH_IDENTITY_COLUMNS])

    wide_frames = []

    for measurement_type in sorted(EXPECTED_HYDRO_MEASUREMENT_TYPES):
        subset = grouped[grouped["measurement_type"] == measurement_type].copy()

        if subset.empty:
            continue

        subset = subset.drop(columns=["measurement_type"])
        rename_map = {
            column: f"{measurement_type}_{column}"
            for column in subset.columns
            if column not in GRID_MONTH_IDENTITY_COLUMNS
        }
        subset = subset.rename(columns=rename_map)
        wide_frames.append(subset)

    if not wide_frames:
        return pd.DataFrame(columns=[*GRID_MONTH_IDENTITY_COLUMNS])

    result = wide_frames[0]

    for frame in wide_frames[1:]:
        result = result.merge(
            frame,
            on=GRID_MONTH_IDENTITY_COLUMNS,
            how="outer",
            validate="one_to_one",
        )

    return result


def _aggregate_grid_month_measurement(group: pd.DataFrame) -> pd.Series:
    weights = group["spatial_weight"].astype(float)

    return pd.Series(
        {
            "station_count": int(group["station_id"].nunique()),
            "daily_record_count": int(group["daily_record_count"].sum()),
            "observation_day_count": int(group["observation_day_count"].sum()),
            "measurement_observation_count": int(group["measurement_observation_count"].sum()),
            "mean_measurement_value": _weighted_mean(
                group["mean_measurement_value"],
                weights,
            ),
            "min_measurement_value": _min_with_null(group["min_measurement_value"]),
            "max_measurement_value": _max_with_null(group["max_measurement_value"]),
            "median_measurement_value": _weighted_mean(
                group["median_measurement_value"],
                weights,
            ),
            "p95_measurement_value": _weighted_mean(
                group["p95_measurement_value"],
                weights,
            ),
            "measurement_completeness_ratio": _weighted_mean(
                group["measurement_completeness_ratio"],
                weights,
            ),
            "zero_day_count": int(group["flow_zero_day_count"].sum()),
            "negative_value_count": int(group["negative_value_count"].sum()),
        }
    )


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    numeric_values = pd.to_numeric(values, errors="coerce")
    numeric_weights = pd.to_numeric(weights, errors="coerce")

    mask = numeric_values.notna() & numeric_weights.notna() & numeric_weights.gt(0)

    if not mask.any():
        return np.nan

    return float(np.average(numeric_values[mask], weights=numeric_weights[mask]))


def _min_with_null(values: pd.Series) -> float:
    non_null = pd.to_numeric(values, errors="coerce").dropna()

    if non_null.empty:
        return np.nan

    return float(non_null.min())


def _max_with_null(values: pd.Series) -> float:
    non_null = pd.to_numeric(values, errors="coerce").dropna()

    if non_null.empty:
        return np.nan

    return float(non_null.max())


def _build_grid_month_skeleton(
    *,
    grid: pd.DataFrame,
    start_month: str,
    end_month: str,
) -> pd.DataFrame:
    grid_identity = grid[
        [
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
        ]
    ].copy()

    months = pd.period_range(
        start=start_month,
        end=end_month,
        freq="M",
    ).astype(str)

    return grid_identity.merge(
        pd.DataFrame({"reference_month": months}),
        how="cross",
    )


def _fill_count_columns(result: pd.DataFrame) -> None:
    count_columns = [
        "hydro_station_count",
        "hydro_basin_station_count",
        "hydro_point_station_count",
        "flow_station_count",
        "flow_daily_record_count",
        "flow_observation_day_count",
        "flow_measurement_observation_count",
        "flow_zero_day_count",
        "flow_negative_value_count",
        "level_station_count",
        "level_daily_record_count",
        "level_observation_day_count",
        "level_measurement_observation_count",
        "level_negative_value_count",
    ]

    for column in count_columns:
        if column not in result.columns:
            result[column] = 0

        result[column] = (
            pd.to_numeric(
                result[column],
                errors="coerce",
            )
            .fillna(0)
            .astype("int64")
        )


def _build_hydro_data_completeness_score(result: pd.DataFrame) -> pd.Series:
    completeness_columns = [
        column
        for column in [
            "flow_measurement_completeness_ratio",
            "level_measurement_completeness_ratio",
        ]
        if column in result.columns
    ]

    if not completeness_columns:
        return pd.Series(pd.NA, index=result.index, dtype="Float64")

    score = result[completeness_columns].mean(axis=1, skipna=True)
    score = score.where(result[completeness_columns].notna().any(axis=1))

    return score.clip(0, 1)


def _build_hydro_quality_flag(result: pd.DataFrame) -> pd.Series:
    score = pd.to_numeric(
        result["hydro_data_completeness_score"],
        errors="coerce",
    )

    quality = pd.Series(pd.NA, index=result.index, dtype="object")

    covered = result["hydro_spatial_assignment_method"].isin(
        [
            HYDRO_SPATIAL_METHOD_BASIN,
            HYDRO_SPATIAL_METHOD_POINT,
        ]
    )
    has_score = covered & score.notna()

    quality.loc[has_score & score.ge(0.9)] = "high"
    quality.loc[has_score & score.ge(0.7) & score.lt(0.9)] = "medium"
    quality.loc[has_score & score.ge(0.4) & score.lt(0.7)] = "low"
    quality.loc[has_score & score.lt(0.4)] = "very_low"

    point_mask = result["hydro_spatial_assignment_method"].eq(HYDRO_SPATIAL_METHOD_POINT)
    quality.loc[point_mask & quality.eq("high")] = "medium"

    return quality


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldHydroFeatureError(f"{table_name} is missing columns: {sorted(missing_columns)}")
