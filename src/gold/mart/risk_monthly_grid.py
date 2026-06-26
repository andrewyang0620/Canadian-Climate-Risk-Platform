from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet


EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
EXPECTED_HYDRO_MEASUREMENT_TYPES = {"flow", "level"}

REFERENCE_MONTH_START = "2016-01"
REFERENCE_MONTH_END = "2025-12"

MART_TABLE_NAME = "gold_grid_month_risk_feature_mart"


class GoldRiskMartError(Exception):
    """Raised when Gold risk mart generation fails."""


GRID_IDENTITY_COLUMNS = [
    "grid_cell_key",
    "grid_system",
    "grid_level",
    "grid_version",
    "province_key",
    "province_code",
    "province_name",
    "boundary_year",
    "cell_size_m",
    "grid_x_index",
    "grid_y_index",
    "centroid_longitude",
    "centroid_latitude",
    "full_cell_area_sq_km",
    "analysis_area_sq_km",
    "boundary_coverage_ratio",
    "is_boundary_edge_cell",
]


CLIMATE_RENAME_MAP = {
    "station_count": "climate_station_count",
    "daily_record_count": "climate_daily_record_count",
    "temperature_observation_count": "climate_temperature_observation_count",
    "precipitation_observation_count": "climate_precipitation_observation_count",
    "nearest_station_distance_km": "climate_nearest_station_distance_km",
    "mean_station_distance_km": "climate_mean_station_distance_km",
}


HYDRO_VALUE_COLUMNS = [
    "station_count",
    "daily_record_count",
    "observation_day_count",
    "measurement_observation_count",
    "mean_measurement_value",
    "min_measurement_value",
    "max_measurement_value",
    "median_measurement_value",
    "p95_measurement_value",
    "mean_measurement_completeness_ratio",
    "flow_zero_day_count",
    "negative_value_count",
    "nearest_station_distance_km",
    "mean_station_distance_km",
    "hydro_feature_quality_flag",
]


HYDRO_COLUMN_SUFFIX_MAP = {
    "station_count": "station_count",
    "daily_record_count": "daily_record_count",
    "observation_day_count": "observation_day_count",
    "measurement_observation_count": "measurement_observation_count",
    "mean_measurement_value": "mean_measurement_value",
    "min_measurement_value": "min_measurement_value",
    "max_measurement_value": "max_measurement_value",
    "median_measurement_value": "median_measurement_value",
    "p95_measurement_value": "p95_measurement_value",
    "mean_measurement_completeness_ratio": ("mean_measurement_completeness_ratio"),
    "flow_zero_day_count": "zero_day_count",
    "negative_value_count": "negative_value_count",
    "nearest_station_distance_km": "nearest_station_distance_km",
    "mean_station_distance_km": "mean_station_distance_km",
    "hydro_feature_quality_flag": "feature_quality_flag",
}


def read_gold_table(
    *,
    table_name: str,
    gold_root: str | Path = "lakehouse/gold",
) -> pd.DataFrame:
    path = latest_table_parquet(
        root=gold_root,
        table_name=table_name,
    )

    return pd.read_parquet(path)


def read_gold_risk_mart_inputs(
    *,
    gold_root: str | Path = "lakehouse/gold",
) -> dict[str, pd.DataFrame]:
    return {
        "grid": read_gold_table(
            table_name="gold_grid_cell",
            gold_root=gold_root,
        ),
        "municipality_bridge": read_gold_table(
            table_name="gold_grid_municipality_bridge",
            gold_root=gold_root,
        ),
        "climate_grid_month": read_gold_table(
            table_name="gold_grid_month_climate_feature",
            gold_root=gold_root,
        ),
        "hydro_grid_month": read_gold_table(
            table_name="gold_grid_month_hydro_feature",
            gold_root=gold_root,
        ),
    }


def build_gold_grid_month_risk_feature_mart(
    *,
    grid: pd.DataFrame,
    municipality_bridge: pd.DataFrame,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    skeleton = build_grid_month_skeleton(grid)
    primary_municipality = build_primary_municipality_lookup(municipality_bridge)
    climate_features = prepare_climate_grid_month_features(climate_grid_month)
    hydro_features = pivot_hydro_grid_month_features(hydro_grid_month)

    mart = skeleton.merge(
        primary_municipality,
        on="grid_cell_key",
        how="left",
        validate="many_to_one",
    )
    _assert_row_count_unchanged(
        before=skeleton,
        after=mart,
        step_name="primary municipality join",
    )

    mart = mart.merge(
        climate_features,
        on=["grid_cell_key", "reference_month"],
        how="left",
        validate="one_to_one",
    )
    _assert_row_count_unchanged(
        before=skeleton,
        after=mart,
        step_name="climate feature join",
    )

    mart = mart.merge(
        hydro_features,
        on=["grid_cell_key", "reference_month"],
        how="left",
        validate="one_to_one",
    )
    _assert_row_count_unchanged(
        before=skeleton,
        after=mart,
        step_name="hydro feature join",
    )

    mart["has_climate_feature"] = mart["climate_station_count"].notna()
    mart["has_hydro_flow_feature"] = mart["flow_station_count"].notna()
    mart["has_hydro_level_feature"] = mart["level_station_count"].notna()

    mart = mart.sort_values(
        [
            "reference_month",
            "grid_system",
            "grid_cell_key",
        ]
    ).reset_index(drop=True)

    summary = summarize_risk_mart(mart)

    return mart, summary


def build_grid_month_skeleton(grid: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        grid,
        set(GRID_IDENTITY_COLUMNS),
        "gold_grid_cell",
    )

    grid_10km = grid[grid["grid_system"].isin(EXPECTED_GRID_SYSTEMS)][GRID_IDENTITY_COLUMNS].copy()

    if grid_10km.empty:
        raise GoldRiskMartError("gold_grid_cell contains no AB/BC 10km grid cells.")

    duplicate_grid_keys = int(grid_10km["grid_cell_key"].duplicated().sum())

    if duplicate_grid_keys > 0:
        raise GoldRiskMartError(
            "gold_grid_cell contains duplicate 10km grid_cell_key values: "
            f"{duplicate_grid_keys}."
        )

    reference_months = pd.DataFrame(
        {
            "reference_month": pd.period_range(
                REFERENCE_MONTH_START,
                REFERENCE_MONTH_END,
                freq="M",
            ).astype(str)
        }
    )

    grid_10km["_join_key"] = 1
    reference_months["_join_key"] = 1

    skeleton = (
        grid_10km.merge(reference_months, on="_join_key", how="inner")
        .drop(columns=["_join_key"])
        .reset_index(drop=True)
    )

    skeleton["grid_month_risk_feature_key"] = (
        skeleton["grid_cell_key"].astype(str) + "__" + skeleton["reference_month"].astype(str)
    )

    return skeleton[
        [
            "grid_month_risk_feature_key",
            "grid_cell_key",
            "reference_month",
            *[column for column in GRID_IDENTITY_COLUMNS if column != "grid_cell_key"],
        ]
    ]


def build_primary_municipality_lookup(
    municipality_bridge: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "grid_cell_key",
        "municipality_key",
        "municipality_name",
        "municipality_type",
        "grid_coverage_ratio",
        "municipality_coverage_ratio",
        "is_primary_municipality",
        "municipality_match_count",
    }
    _require_columns(
        municipality_bridge,
        required_columns,
        "gold_grid_municipality_bridge",
    )

    primary = municipality_bridge[municipality_bridge["is_primary_municipality"]].copy()

    if primary.empty:
        raise GoldRiskMartError(
            "gold_grid_municipality_bridge contains no primary " "municipality rows."
        )

    primary = primary.sort_values(
        [
            "grid_cell_key",
            "grid_coverage_ratio",
            "municipality_key",
        ],
        ascending=[True, False, True],
    ).drop_duplicates("grid_cell_key", keep="first")

    return primary[
        [
            "grid_cell_key",
            "municipality_key",
            "municipality_name",
            "municipality_type",
            "grid_coverage_ratio",
            "municipality_coverage_ratio",
            "municipality_match_count",
        ]
    ].rename(
        columns={
            "municipality_key": "primary_municipality_key",
            "municipality_name": "primary_municipality_name",
            "municipality_type": "primary_municipality_type",
            "grid_coverage_ratio": ("primary_municipality_grid_coverage_ratio"),
            "municipality_coverage_ratio": ("primary_municipality_coverage_ratio"),
        }
    )


def prepare_climate_grid_month_features(
    climate_grid_month: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "grid_cell_key",
        "reference_month",
        "station_count",
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
        "nearest_station_distance_km",
        "mean_station_distance_km",
        "temperature_completeness_ratio",
        "precipitation_completeness_ratio",
        "climate_data_completeness_score",
        "climate_feature_quality_flag",
        "grid_month_climate_feature_key",
    }
    _require_columns(
        climate_grid_month,
        required_columns,
        "gold_grid_month_climate_feature",
    )

    duplicate_count = int(
        climate_grid_month[["grid_cell_key", "reference_month"]].duplicated().sum()
    )

    if duplicate_count > 0:
        raise GoldRiskMartError(
            "gold_grid_month_climate_feature contains duplicate "
            "grid_cell_key × reference_month rows: "
            f"{duplicate_count}."
        )

    columns = [
        "grid_cell_key",
        "reference_month",
        "station_count",
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
        "nearest_station_distance_km",
        "mean_station_distance_km",
        "temperature_completeness_ratio",
        "precipitation_completeness_ratio",
        "climate_data_completeness_score",
        "climate_feature_quality_flag",
        "grid_month_climate_feature_key",
    ]

    return climate_grid_month[columns].rename(columns=CLIMATE_RENAME_MAP)


def pivot_hydro_grid_month_features(
    hydro_grid_month: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "grid_cell_key",
        "reference_month",
        "measurement_type",
        *HYDRO_VALUE_COLUMNS,
    }
    _require_columns(
        hydro_grid_month,
        required_columns,
        "gold_grid_month_hydro_feature",
    )

    measurement_types = set(hydro_grid_month["measurement_type"].dropna().astype(str).unique())
    unexpected_types = measurement_types - EXPECTED_HYDRO_MEASUREMENT_TYPES

    if unexpected_types:
        raise GoldRiskMartError(
            "gold_grid_month_hydro_feature contains unexpected "
            f"measurement_type values: {sorted(unexpected_types)}."
        )

    duplicate_count = int(
        hydro_grid_month[
            [
                "grid_cell_key",
                "reference_month",
                "measurement_type",
            ]
        ]
        .duplicated()
        .sum()
    )

    if duplicate_count > 0:
        raise GoldRiskMartError(
            "gold_grid_month_hydro_feature contains duplicate "
            "grid_cell_key × reference_month × measurement_type rows: "
            f"{duplicate_count}."
        )

    feature_frames = []

    for measurement_type in sorted(EXPECTED_HYDRO_MEASUREMENT_TYPES):
        subset = hydro_grid_month[hydro_grid_month["measurement_type"] == measurement_type].copy()

        if subset.empty:
            continue

        rename_map = {
            column: f"{measurement_type}_{suffix}"
            for column, suffix in HYDRO_COLUMN_SUFFIX_MAP.items()
        }

        feature_frames.append(
            subset[
                [
                    "grid_cell_key",
                    "reference_month",
                    *HYDRO_VALUE_COLUMNS,
                ]
            ].rename(columns=rename_map)
        )

    if not feature_frames:
        return pd.DataFrame(columns=["grid_cell_key", "reference_month"])

    return reduce(
        lambda left, right: left.merge(
            right,
            on=["grid_cell_key", "reference_month"],
            how="outer",
            validate="one_to_one",
        ),
        feature_frames,
    )


def summarize_risk_mart(mart: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": int(len(mart)),
        "grid_cell_count": int(mart["grid_cell_key"].nunique()),
        "month_count": int(mart["reference_month"].nunique()),
        "minimum_month": str(mart["reference_month"].min()),
        "maximum_month": str(mart["reference_month"].max()),
        "grid_systems": sorted(mart["grid_system"].dropna().unique().tolist()),
        "climate_grid_month_count": int(mart["has_climate_feature"].sum()),
        "hydro_flow_grid_month_count": int(mart["has_hydro_flow_feature"].sum()),
        "hydro_level_grid_month_count": int(mart["has_hydro_level_feature"].sum()),
        "grid_cells_with_climate_feature": int(
            mart.loc[mart["has_climate_feature"], "grid_cell_key"].nunique()
        ),
        "grid_cells_with_hydro_flow_feature": int(
            mart.loc[mart["has_hydro_flow_feature"], "grid_cell_key"].nunique()
        ),
        "grid_cells_with_hydro_level_feature": int(
            mart.loc[mart["has_hydro_level_feature"], "grid_cell_key"].nunique()
        ),
    }


def _assert_row_count_unchanged(
    *,
    before: pd.DataFrame,
    after: pd.DataFrame,
    step_name: str,
) -> None:
    if len(before) != len(after):
        raise GoldRiskMartError(
            f"Row count changed during {step_name}: " f"before={len(before)}, after={len(after)}."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldRiskMartError(f"{table_name} is missing columns: {sorted(missing_columns)}")
