from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.gold.common.io import latest_table_parquet


EXPECTED_GRID_SYSTEMS = {"ab_10km", "bc_10km"}

REFERENCE_MONTH_START = "2016-01"
REFERENCE_MONTH_END = "2025-12"
EXPECTED_MONTH_COUNT = 120

MART_TABLE_NAME = "gold_grid_month_risk_feature_mart"

NO_CLIMATE_COVERAGE_METHOD = "no_station_within_radius"
NO_HYDRO_COVERAGE_METHOD = "no_hydro_coverage"


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
    "daily_record_count": "climate_daily_record_count",
    "temperature_observation_count": "climate_temperature_observation_count",
    "precipitation_observation_count": "climate_precipitation_observation_count",
    "mean_temp_c": "climate_mean_temp_c",
    "min_temp_c": "climate_min_temp_c",
    "max_temp_c": "climate_max_temp_c",
    "observed_min_temp_c": "climate_observed_min_temp_c",
    "observed_max_temp_c": "climate_observed_max_temp_c",
    "total_precip_mm": "climate_total_precip_mm",
    "total_rain_mm": "climate_total_rain_mm",
    "total_snow": "climate_total_snow",
    "precipitation_days": "climate_precipitation_days",
    "heavy_precipitation_days": "climate_heavy_precipitation_days",
    "extreme_heat_days": "climate_extreme_heat_days",
    "extreme_cold_days": "climate_extreme_cold_days",
    "freeze_thaw_days": "climate_freeze_thaw_days",
    "temperature_completeness_ratio": "climate_temperature_completeness_ratio",
    "precipitation_completeness_ratio": "climate_precipitation_completeness_ratio",
}


WILDFIRE_RENAME_MAP = {
    "crs_epsg": "wildfire_crs_epsg",
    "grid_analysis_area_sq_km": "wildfire_grid_analysis_area_sq_km",
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
        "wildfire_grid_month": read_gold_table(
            table_name="gold_grid_month_wildfire_perimeter_feature",
            gold_root=gold_root,
        ),
    }


def build_gold_grid_month_risk_feature_mart(
    *,
    grid: pd.DataFrame,
    municipality_bridge: pd.DataFrame,
    climate_grid_month: pd.DataFrame,
    hydro_grid_month: pd.DataFrame,
    wildfire_grid_month: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    skeleton = build_grid_month_skeleton(grid)
    primary_municipality = build_primary_municipality_lookup(municipality_bridge)

    climate_features = prepare_climate_grid_month_features(climate_grid_month)
    hydro_features = prepare_hydro_grid_month_features(hydro_grid_month)
    wildfire_features = prepare_wildfire_grid_month_features(wildfire_grid_month)

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

    mart = mart.merge(
        wildfire_features,
        on=["grid_cell_key", "reference_month"],
        how="left",
        validate="one_to_one",
    )
    _assert_row_count_unchanged(
        before=skeleton,
        after=mart,
        step_name="wildfire perimeter feature join",
    )

    mart["has_climate_feature"] = mart["climate_mapping_method"].notna() & mart[
        "climate_mapping_method"
    ].ne(NO_CLIMATE_COVERAGE_METHOD)
    mart["has_hydro_spatial_coverage"] = mart["hydro_spatial_assignment_method"].notna() & mart[
        "hydro_spatial_assignment_method"
    ].ne(NO_HYDRO_COVERAGE_METHOD)
    mart["has_hydro_flow_feature"] = mart["flow_mean_measurement_value"].notna()
    mart["has_hydro_level_feature"] = mart["level_mean_measurement_value"].notna()
    mart["has_hydro_feature"] = mart["has_hydro_flow_feature"] | mart["has_hydro_level_feature"]
    mart["has_wildfire_perimeter_feature"] = mart["wildfire_perimeter_count"].notna()
    mart["has_wildfire_observed_perimeter_overlap"] = (
        mart["wildfire_has_observed_perimeter_overlap"].fillna(False).astype(bool)
    )

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

    skeleton = grid_10km.merge(
        reference_months,
        how="cross",
    ).reset_index(drop=True)

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
            "gold_grid_municipality_bridge contains no primary municipality rows."
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
            "grid_coverage_ratio": "primary_municipality_grid_coverage_ratio",
            "municipality_coverage_ratio": "primary_municipality_coverage_ratio",
        }
    )


def prepare_climate_grid_month_features(
    climate_grid_month: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "grid_month_climate_feature_key",
        "grid_cell_key",
        "reference_month",
        "climate_mapping_method",
        "climate_station_count",
        "climate_nearest_station_distance_km",
        "climate_mean_station_distance_km",
        "climate_max_station_distance_km",
        "climate_idw_confidence_score",
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
        "climate_data_completeness_score",
        "climate_feature_quality_flag",
    }
    _require_columns(
        climate_grid_month,
        required_columns,
        "gold_grid_month_climate_feature",
    )
    _assert_unique_grid_month(
        climate_grid_month,
        table_name="gold_grid_month_climate_feature",
    )

    columns = [
        "grid_month_climate_feature_key",
        "grid_cell_key",
        "reference_month",
        "climate_mapping_method",
        "climate_station_count",
        "climate_nearest_station_distance_km",
        "climate_mean_station_distance_km",
        "climate_max_station_distance_km",
        "climate_idw_confidence_score",
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
        "climate_data_completeness_score",
        "climate_feature_quality_flag",
    ]

    return climate_grid_month[columns].rename(columns=CLIMATE_RENAME_MAP)


def prepare_hydro_grid_month_features(
    hydro_grid_month: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "grid_month_hydro_feature_key",
        "grid_cell_key",
        "reference_month",
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
    }
    _require_columns(
        hydro_grid_month,
        required_columns,
        "gold_grid_month_hydro_feature",
    )
    _assert_unique_grid_month(
        hydro_grid_month,
        table_name="gold_grid_month_hydro_feature",
    )

    return hydro_grid_month[
        [
            "grid_month_hydro_feature_key",
            "grid_cell_key",
            "reference_month",
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
    ]


def prepare_wildfire_grid_month_features(
    wildfire_grid_month: pd.DataFrame,
) -> pd.DataFrame:
    required_columns = {
        "wildfire_grid_month_key",
        "grid_cell_key",
        "reference_month",
        "crs_epsg",
        "grid_analysis_area_sq_km",
        "wildfire_perimeter_count",
        "wildfire_intersection_area_sq_km",
        "wildfire_intersection_area_ha",
        "wildfire_intersection_area_ratio_of_grid",
        "wildfire_max_source_size_ha",
        "wildfire_max_calculated_size_ha",
        "wildfire_cause_n_polygon_count",
        "wildfire_cause_h_polygon_count",
        "wildfire_cause_u_polygon_count",
        "wildfire_cause_prescribed_burn_polygon_count",
        "wildfire_cause_other_polygon_count",
        "wildfire_has_observed_perimeter_overlap",
        "wildfire_temporal_assignment_method",
    }
    _require_columns(
        wildfire_grid_month,
        required_columns,
        "gold_grid_month_wildfire_perimeter_feature",
    )
    _assert_unique_grid_month(
        wildfire_grid_month,
        table_name="gold_grid_month_wildfire_perimeter_feature",
    )

    columns = [
        "wildfire_grid_month_key",
        "grid_cell_key",
        "reference_month",
        "crs_epsg",
        "grid_analysis_area_sq_km",
        "wildfire_perimeter_count",
        "wildfire_intersection_area_sq_km",
        "wildfire_intersection_area_ha",
        "wildfire_intersection_area_ratio_of_grid",
        "wildfire_max_source_size_ha",
        "wildfire_max_calculated_size_ha",
        "wildfire_cause_n_polygon_count",
        "wildfire_cause_h_polygon_count",
        "wildfire_cause_u_polygon_count",
        "wildfire_cause_prescribed_burn_polygon_count",
        "wildfire_cause_other_polygon_count",
        "wildfire_has_observed_perimeter_overlap",
        "wildfire_temporal_assignment_method",
    ]

    return wildfire_grid_month[columns].rename(columns=WILDFIRE_RENAME_MAP)


def summarize_risk_mart(mart: pd.DataFrame) -> dict[str, Any]:
    climate_covered = mart["has_climate_feature"]
    hydro_covered = mart["has_hydro_spatial_coverage"]
    hydro_available = mart["has_hydro_feature"]
    wildfire_joined = mart["has_wildfire_perimeter_feature"]
    wildfire_overlap = mart["has_wildfire_observed_perimeter_overlap"]

    return {
        "row_count": int(len(mart)),
        "grid_cell_count": int(mart["grid_cell_key"].nunique()),
        "month_count": int(mart["reference_month"].nunique()),
        "minimum_month": str(mart["reference_month"].min()),
        "maximum_month": str(mart["reference_month"].max()),
        "grid_systems": sorted(mart["grid_system"].dropna().unique().tolist()),
        "climate_covered_grid_month_count": int(climate_covered.sum()),
        "hydro_spatial_coverage_grid_month_count": int(hydro_covered.sum()),
        "hydro_available_grid_month_count": int(hydro_available.sum()),
        "hydro_flow_available_grid_month_count": int(mart["has_hydro_flow_feature"].sum()),
        "hydro_level_available_grid_month_count": int(mart["has_hydro_level_feature"].sum()),
        "wildfire_joined_grid_month_count": int(wildfire_joined.sum()),
        "wildfire_overlap_grid_month_count": int(wildfire_overlap.sum()),
        "grid_cells_with_climate_feature": int(
            mart.loc[climate_covered, "grid_cell_key"].nunique()
        ),
        "grid_cells_with_hydro_spatial_coverage": int(
            mart.loc[hydro_covered, "grid_cell_key"].nunique()
        ),
        "grid_cells_with_hydro_available_feature": int(
            mart.loc[hydro_available, "grid_cell_key"].nunique()
        ),
        "grid_cells_with_wildfire_overlap": int(
            mart.loc[wildfire_overlap, "grid_cell_key"].nunique()
        ),
        "climate_mapping_method_counts": _value_counts(mart["climate_mapping_method"]),
        "hydro_spatial_assignment_method_counts": _value_counts(
            mart["hydro_spatial_assignment_method"]
        ),
        "wildfire_temporal_assignment_method_counts": _value_counts(
            mart["wildfire_temporal_assignment_method"]
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


def _assert_unique_grid_month(
    dataframe: pd.DataFrame,
    *,
    table_name: str,
) -> None:
    duplicate_count = int(dataframe[["grid_cell_key", "reference_month"]].duplicated().sum())

    if duplicate_count > 0:
        raise GoldRiskMartError(
            f"{table_name} contains duplicate grid_cell_key × reference_month "
            f"rows: {duplicate_count}."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldRiskMartError(f"{table_name} is missing columns: {sorted(missing_columns)}")


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value) for key, value in series.value_counts(dropna=False).to_dict().items()
    }
