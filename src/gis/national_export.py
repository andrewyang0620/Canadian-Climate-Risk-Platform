from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import geopandas as gpd
import pandas as pd

from src.gold.common.io import latest_table_parquet


GRID_TABLE_NAME = "gold_grid_cell"
MART_TABLE_NAME = "gold_grid_month_risk_feature_mart"
SCORE_TABLE_NAME = "gold_grid_month_risk_score"

ANALYSIS_CRS = "EPSG:3347"
DISPLAY_CRS = "EPSG:4326"

NATIONAL_GRID_SYSTEMS = {
    "ab_10km",
    "bc_10km",
}


GRID_COLUMNS = [
    "grid_cell_key",
    "grid_system",
    "province_key",
    "province_code",
    "province_name",
    "cell_size_m",
    "centroid_longitude",
    "centroid_latitude",
    "analysis_area_sq_km",
    "boundary_coverage_ratio",
    "is_boundary_edge_cell",
    "analysis_geometry_wkt",
]

GRID_METADATA_COLUMNS = [
    "grid_cell_key",
    "grid_system",
    "province_key",
    "province_code",
    "province_name",
    "cell_size_m",
    "centroid_longitude",
    "centroid_latitude",
    "analysis_area_sq_km",
    "boundary_coverage_ratio",
    "is_boundary_edge_cell",
    "primary_municipality_name",
]


MART_COLUMNS = [
    # identity
    "grid_cell_key",
    "reference_month",
    "grid_system",
    "province_key",
    "primary_municipality_name",

    # climate physical
    "climate_mean_temp_c",
    "climate_min_temp_c",
    "climate_max_temp_c",
    "climate_total_precip_mm",
    "climate_heavy_precipitation_days",
    "climate_extreme_heat_days",
    "climate_extreme_cold_days",
    "climate_freeze_thaw_days",

    # climate quality
    "climate_station_count",
    "climate_mapping_method",
    "climate_nearest_station_distance_km",
    "climate_idw_confidence_score",
    "climate_temperature_completeness_ratio",
    "climate_precipitation_completeness_ratio",
    "climate_feature_quality_flag",

    # hydro physical
    "flow_mean_measurement_value",
    "flow_p95_measurement_value",
    "level_mean_measurement_value",
    "level_p95_measurement_value",

    # hydro quality
    "hydro_station_count",
    "hydro_spatial_assignment_method",
    "hydro_basin_grid_coverage_ratio",
    "flow_measurement_completeness_ratio",
    "level_measurement_completeness_ratio",
    "hydro_feature_quality_flag",

    # wildfire physical
    "wildfire_perimeter_count",
    "wildfire_intersection_area_sq_km",
    "wildfire_intersection_area_ratio_of_grid",
    "wildfire_max_source_size_ha",
    "wildfire_has_observed_perimeter_overlap",
    "wildfire_temporal_assignment_method",
]


SCORE_COLUMNS = [
    "grid_cell_key",
    "reference_month",

    "climate_sub_score",
    "hydro_sub_score",
    "wildfire_sub_score",

    "composite_risk_score",
    "score_confidence",

    "domain_coverage_count",
    "domain_coverage_ratio",

    "climate_effective_quality",
    "hydro_effective_quality",
    "wildfire_effective_quality",

    "composite_score_eligible",
    "ranking_eligible",
    "ranking_exclusion_reason",
    "priority_percentile",
    "priority_tier",
]


MONTHLY_OUTPUT_COLUMNS = [
    # identity
    "grid_cell_key",

    # risk
    "composite_risk_score",
    "score_confidence",
    "priority_percentile",
    "priority_tier",

    # domain scores
    "climate_sub_score",
    "hydro_sub_score",
    "wildfire_sub_score",

    # climate physical
    "climate_mean_temp_c",
    "climate_min_temp_c",
    "climate_max_temp_c",
    "climate_total_precip_mm",
    "climate_heavy_precipitation_days",
    "climate_extreme_heat_days",
    "climate_extreme_cold_days",
    "climate_freeze_thaw_days",

    # hydro physical
    "flow_mean_measurement_value",
    "flow_p95_measurement_value",
    "level_mean_measurement_value",
    "level_p95_measurement_value",

    # wildfire physical
    "wildfire_perimeter_count",
    "wildfire_intersection_area_sq_km",
    "wildfire_intersection_area_ratio_of_grid",
    "wildfire_max_source_size_ha",
    "wildfire_has_observed_perimeter_overlap",

    # climate quality
    "climate_station_count",
    "climate_mapping_method",
    "climate_nearest_station_distance_km",
    "climate_idw_confidence_score",
    "climate_temperature_completeness_ratio",
    "climate_precipitation_completeness_ratio",
    "climate_feature_quality_flag",

    # hydro quality
    "hydro_station_count",
    "hydro_spatial_assignment_method",
    "hydro_basin_grid_coverage_ratio",
    "flow_measurement_completeness_ratio",
    "level_measurement_completeness_ratio",
    "hydro_feature_quality_flag",

    # scoring quality
    "climate_effective_quality",
    "hydro_effective_quality",
    "wildfire_effective_quality",
    "domain_coverage_count",
    "domain_coverage_ratio",
    "composite_score_eligible",
    "ranking_eligible",
    "ranking_exclusion_reason",

    # wildfire method
    "wildfire_temporal_assignment_method",
]


RATIO_COLUMNS = [
    "score_confidence",
    "priority_percentile",
    "climate_sub_score",
    "hydro_sub_score",
    "wildfire_sub_score",
    "composite_risk_score",
    "climate_idw_confidence_score",
    "climate_temperature_completeness_ratio",
    "climate_precipitation_completeness_ratio",
    "hydro_basin_grid_coverage_ratio",
    "flow_measurement_completeness_ratio",
    "level_measurement_completeness_ratio",
    "wildfire_intersection_area_ratio_of_grid",
    "climate_effective_quality",
    "hydro_effective_quality",
    "wildfire_effective_quality",
    "domain_coverage_ratio",
]


LAYER_DEFINITIONS = {
    # risk
    "composite_risk_score": {
        "group": "risk",
        "label": "Composite Risk",
        "unit": None,
        "format": "score",
    },
    "climate_sub_score": {
        "group": "risk",
        "label": "Climate Score",
        "unit": None,
        "format": "score",
    },
    "hydro_sub_score": {
        "group": "risk",
        "label": "Hydro Score",
        "unit": None,
        "format": "score",
    },
    "wildfire_sub_score": {
        "group": "risk",
        "label": "Wildfire Score",
        "unit": None,
        "format": "score",
    },
    "score_confidence": {
        "group": "risk",
        "label": "Score Confidence",
        "unit": None,
        "format": "score",
    },

    # climate
    "climate_mean_temp_c": {
        "group": "climate",
        "label": "Mean Temperature",
        "unit": "°C",
        "format": "number",
    },
    "climate_total_precip_mm": {
        "group": "climate",
        "label": "Total Precipitation",
        "unit": "mm",
        "format": "number",
    },
    "climate_extreme_heat_days": {
        "group": "climate",
        "label": "Extreme Heat Days",
        "unit": "days",
        "format": "integer",
    },
    "climate_heavy_precipitation_days": {
        "group": "climate",
        "label": "Heavy Precipitation Days",
        "unit": "days",
        "format": "integer",
    },

    # hydro
    "flow_mean_measurement_value": {
        "group": "hydro",
        "label": "Mean Flow Measurement",
        "unit": None,
        "format": "number",
    },
    "flow_p95_measurement_value": {
        "group": "hydro",
        "label": "P95 Flow Measurement",
        "unit": None,
        "format": "number",
    },
    "level_mean_measurement_value": {
        "group": "hydro",
        "label": "Mean Water-Level Measurement",
        "unit": None,
        "format": "number",
    },
    "level_p95_measurement_value": {
        "group": "hydro",
        "label": "P95 Water-Level Measurement",
        "unit": None,
        "format": "number",
    },

    # wildfire
    "wildfire_perimeter_count": {
        "group": "wildfire",
        "label": "Wildfire Perimeter Count",
        "unit": "count",
        "format": "integer",
    },
    "wildfire_intersection_area_sq_km": {
        "group": "wildfire",
        "label": "Wildfire Intersected Area",
        "unit": "km²",
        "format": "number",
    },
    "wildfire_intersection_area_ratio_of_grid": {
        "group": "wildfire",
        "label": "Grid Wildfire Overlap Ratio",
        "unit": None,
        "format": "ratio",
    },
}


class NationalGISExportError(ValueError):
    """Raised when national GIS export inputs violate the serving contract."""


def build_national_grid_geometry(grid: pd.DataFrame) -> gpd.GeoDataFrame:
    """Build the static AB/BC 10km display geometry layer."""

    _require_columns(
        grid,
        GRID_COLUMNS,
        table_name=GRID_TABLE_NAME,
    )

    national = grid[
        grid["grid_system"].isin(
            NATIONAL_GRID_SYSTEMS
        )
    ][GRID_COLUMNS].copy()

    if national.empty:
        raise NationalGISExportError(
            "No national 10km grid rows were found."
        )

    if national["grid_cell_key"].duplicated().any():
        raise NationalGISExportError(
            "National grid contains duplicate grid_cell_key values."
        )

    geometry = gpd.GeoSeries.from_wkt(
        national["analysis_geometry_wkt"],
        crs=ANALYSIS_CRS,
    )

    if geometry.isna().any():
        raise NationalGISExportError(
            "National grid contains null geometry."
        )

    if geometry.is_empty.any():
        raise NationalGISExportError(
            "National grid contains empty geometry."
        )

    result = gpd.GeoDataFrame(
        national.drop(
            columns=["analysis_geometry_wkt"]
        ),
        geometry=geometry,
        crs=ANALYSIS_CRS,
    )

    result = result.to_crs(
        DISPLAY_CRS
    )

    if not result.geometry.is_valid.all():
        invalid_count = int(
            (~result.geometry.is_valid).sum()
        )

        raise NationalGISExportError(
            "National display geometry contains "
            f"{invalid_count} invalid geometries."
        )

    return result.sort_values(
        [
            "province_key",
            "grid_cell_key",
        ]
    ).reset_index(drop=True)


def build_monthly_gis_attributes(
    mart: pd.DataFrame,
    score: pd.DataFrame,
    *,
    reference_month: str,
) -> pd.DataFrame:
    """Build one grid-month serving dataset for the national GIS."""

    _require_columns(
        mart,
        MART_COLUMNS,
        table_name=MART_TABLE_NAME,
    )

    _require_columns(
        score,
        SCORE_COLUMNS,
        table_name=SCORE_TABLE_NAME,
    )

    mart = mart.copy()
    score = score.copy()

    mart["reference_month"] = _normalize_month(
        mart["reference_month"]
    )

    score["reference_month"] = _normalize_month(
        score["reference_month"]
    )

    mart_month = mart[
        mart["reference_month"]
        == reference_month
    ][MART_COLUMNS].copy()

    score_month = score[
        score["reference_month"]
        == reference_month
    ][SCORE_COLUMNS].copy()

    if mart_month.empty:
        raise NationalGISExportError(
            f"No mart rows found for {reference_month}."
        )

    if score_month.empty:
        raise NationalGISExportError(
            f"No score rows found for {reference_month}."
        )

    _assert_unique_grid_month(
        mart_month,
        table_name=MART_TABLE_NAME,
    )

    _assert_unique_grid_month(
        score_month,
        table_name=SCORE_TABLE_NAME,
    )

    result = mart_month.merge(
        score_month,
        on=[
            "grid_cell_key",
            "reference_month",
        ],
        how="inner",
        validate="one_to_one",
    )

    if len(result) != len(mart_month):
        raise NationalGISExportError(
            "Risk mart and risk score do not have identical "
            f"grid coverage for {reference_month}: "
            f"mart={len(mart_month)}, "
            f"score={len(score_month)}, "
            f"joined={len(result)}."
        )

    if set(result["grid_system"]) != NATIONAL_GRID_SYSTEMS:
        raise NationalGISExportError(
            "Monthly GIS data contains unexpected grid systems: "
            f"{sorted(result['grid_system'].dropna().unique())}"
        )

    result = result.sort_values(
        [
            "province_key",
            "grid_cell_key",
        ]
    ).reset_index(drop=True)

    result = result[
        MONTHLY_OUTPUT_COLUMNS
    ].copy()

    _validate_monthly_attributes(
        result
    )
    
    return result


def build_grid_metadata(
    geometry: gpd.GeoDataFrame,
    mart_snapshot: pd.DataFrame,
) -> pd.DataFrame:
    """Build static metadata for national GIS grid cells."""

    required_mart_columns = [
        "grid_cell_key",
        "primary_municipality_name",
    ]

    _require_columns(
        mart_snapshot,
        required_mart_columns,
        table_name=MART_TABLE_NAME,
    )

    if mart_snapshot["grid_cell_key"].duplicated().any():
        raise NationalGISExportError(
            "Grid metadata snapshot contains duplicate grid_cell_key values."
        )

    static_grid = (
        geometry
        .drop(columns=["geometry"])
        .copy()
    )

    municipality = mart_snapshot[
        required_mart_columns
    ].copy()

    result = static_grid.merge(
        municipality,
        on="grid_cell_key",
        how="left",
        validate="one_to_one",
    )

    if len(result) != len(geometry):
        raise NationalGISExportError(
            "Grid metadata row count does not match geometry."
        )

    missing_keys = (
        set(geometry["grid_cell_key"])
        - set(result["grid_cell_key"])
    )

    if missing_keys:
        raise NationalGISExportError(
            "Grid metadata is missing "
            f"{len(missing_keys)} geometry keys."
        )

    result = result[
        GRID_METADATA_COLUMNS
    ].copy()

    return result.sort_values(
        [
            "province_key",
            "grid_cell_key",
        ]
    ).reset_index(drop=True)


def export_national_gis_data(
    *,
    gold_root: str | Path = "lakehouse/gold",
    output_root: str | Path = "dashboard/gis/data",
    months: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Export national geometry and monthly GIS serving datasets."""

    gold_root = Path(gold_root)
    output_root = Path(output_root)

    grid_path = latest_table_parquet(
        root=gold_root,
        table_name=GRID_TABLE_NAME,
    )

    mart_path = latest_table_parquet(
        root=gold_root,
        table_name=MART_TABLE_NAME,
    )

    score_path = latest_table_parquet(
        root=gold_root,
        table_name=SCORE_TABLE_NAME,
    )

    available_months = _read_available_months(
        score_path
    )

    selected_months = (
        available_months
        if months is None
        else sorted(set(months))
    )

    unknown_months = (
        set(selected_months)
        - set(available_months)
    )

    if unknown_months:
        raise NationalGISExportError(
            "Requested months are not available: "
            f"{sorted(unknown_months)}"
        )

    grid = pd.read_parquet(
        grid_path,
        columns=GRID_COLUMNS,
    )

    geometry = build_national_grid_geometry(
        grid
    )

    geometry_keys = set(
        geometry["grid_cell_key"]
    )
    
    metadata_reference_month = (
        available_months[0]
    )
    
    metadata_mart = _read_month_filtered_parquet(
        mart_path,
        columns=[
            "grid_cell_key",
            "reference_month",
            "primary_municipality_name",
        ],
        months=[
            metadata_reference_month
        ],
    )
    
    grid_metadata = build_grid_metadata(
        geometry,
        metadata_mart,
    )

    output_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    month_root = (
        output_root / "months"
    )

    month_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    geometry_path = (
        output_root
        / "grid_geometry.geojson"
    )
    
    grid_metadata_path = (
        output_root
        / "grid_metadata.json"
    )

    geometry_serving = geometry[
        [
            "grid_cell_key",
            "province_key",
            "geometry",
        ]
    ].copy()
    
    geometry_path.write_text(
        geometry_serving.to_json(
            drop_id=True,
            na="null",
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    
    grid_metadata_payload = {
        "grid_cell_count": int(
            len(grid_metadata)
        ),
        "columns": GRID_METADATA_COLUMNS,
        "rows": (
            grid_metadata
            .astype(object)
            .where(
                pd.notna(grid_metadata),
                None,
            )
            .values
            .tolist()
        ),
    }
    
    grid_metadata_path.write_text(
        json.dumps(
            grid_metadata_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            default=_json_default,
        ),
        encoding="utf-8",
    )

    month_files: dict[str, str] = {}

    for reference_month in selected_months:
        mart_month = _read_month_filtered_parquet(
            mart_path,
            columns=MART_COLUMNS,
            months=[reference_month],
        )

        score_month = _read_month_filtered_parquet(
            score_path,
            columns=SCORE_COLUMNS,
            months=[reference_month],
        )
        
        monthly = build_monthly_gis_attributes(
            mart_month,
            score_month,
            reference_month=reference_month,
        )

        monthly_keys = set(
            monthly["grid_cell_key"]
        )

        if monthly_keys != geometry_keys:
            missing_from_month = (
                geometry_keys - monthly_keys
            )

            extra_in_month = (
                monthly_keys - geometry_keys
            )
            raise NationalGISExportError(
                "Monthly GIS attributes do not match geometry "
                f"for {reference_month}: "
                f"missing={len(missing_from_month)}, "
                f"extra={len(extra_in_month)}."
            )

        month_path = (
            month_root
            / f"risk_{reference_month}.json"
        )

        payload = {
            "reference_month": reference_month,
            "grid_cell_count": int(
                len(monthly)
            ),
            "rows": (
                monthly
                .astype(object)
                .where(
                    pd.notna(monthly),
                    None,
                )
                .values
                .tolist()
            )
        }

        month_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
                default=_json_default,
            ),
            encoding="utf-8",
        )

        month_files[
            reference_month
        ] = (
            f"months/risk_{reference_month}.json"
        )

    manifest = {
        "product": "national_climate_risk_gis",
        "version": "v1",
        "generated_at_utc": (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
        ),
        "scope": {
            "grid_systems": sorted(
                NATIONAL_GRID_SYSTEMS
            ),
            "grid_cell_count": int(
                len(geometry)
            ),
            "month_count": int(
                len(selected_months)
            ),
            "minimum_month": (
                selected_months[0]
            ),
            "maximum_month": (
                selected_months[-1]
            ),
        },
        "geometry": {
            "file": "grid_geometry.geojson",
            "crs": DISPLAY_CRS,
            "feature_count": int(
                len(geometry)
            ),
        },
        "grid_metadata": {
            "file": "grid_metadata.json",
            "reference_month": metadata_reference_month,
            "grid_cell_count": int(
                len(grid_metadata)
            ),
            "columns": GRID_METADATA_COLUMNS,
        },
        "monthly_data": {
            "directory": "months",
            "months": selected_months,
            "files": month_files,
            "columns": MONTHLY_OUTPUT_COLUMNS,
        },
        "layers": LAYER_DEFINITIONS,
        "sources": {
            "grid": GRID_TABLE_NAME,
            "risk_feature_mart": MART_TABLE_NAME,
            "risk_score": SCORE_TABLE_NAME,
        },
    }

    manifest_path = (
        output_root / "manifest.json"
    )

    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return {
        "grid_cell_count": int(
            len(geometry)
        ),
        "month_count": int(
            len(selected_months)
        ),
        "minimum_month": (
            selected_months[0]
        ),
        "maximum_month": (
            selected_months[-1]
        ),
        "geometry_output_path": (
            geometry_path.as_posix()
        ),
        "grid_metadata_output_path": (
            grid_metadata_path.as_posix()
        ),
        "manifest_output_path": (
            manifest_path.as_posix()
        ),
        "month_files": month_files,
    }


def _read_available_months(
    parquet_path: Path,
) -> list[str]:
    months = pd.read_parquet(
        parquet_path,
        columns=["reference_month"],
    )

    normalized = _normalize_month(
        months["reference_month"]
    )

    return sorted(
        normalized
        .dropna()
        .unique()
        .tolist()
    )


def _read_month_filtered_parquet(
    parquet_path: Path,
    *,
    columns: list[str],
    months: list[str],
) -> pd.DataFrame:
    if not months:
        raise NationalGISExportError(
            "At least one export month is required."
        )

    try:
        return pd.read_parquet(
            parquet_path,
            columns=columns,
            filters=[
                (
                    "reference_month",
                    "in",
                    months,
                )
            ],
        )
    except (TypeError, ValueError):
        dataframe = pd.read_parquet(
            parquet_path,
            columns=columns,
        )

        normalized_month = _normalize_month(
            dataframe["reference_month"]
        )

        return dataframe[
            normalized_month.isin(months)
        ].copy()


def _normalize_month(
    series: pd.Series,
) -> pd.Series:
    return (
        pd.to_datetime(
            series,
            errors="raise",
        )
        .dt.to_period("M")
        .astype(str)
    )


def _validate_monthly_attributes(
    dataframe: pd.DataFrame,
) -> None:
    for column in RATIO_COLUMNS:
        values = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        ).dropna()

        if not values.between(
            0.0,
            1.0,
        ).all():
            raise NationalGISExportError(
                f"{column} contains values outside [0, 1]."
            )

    wildfire_count = pd.to_numeric(
        dataframe[
            "wildfire_perimeter_count"
        ],
        errors="coerce",
    )

    if (
        wildfire_count.dropna()
        < 0
    ).any():
        raise NationalGISExportError(
            "wildfire_perimeter_count contains "
            "negative values."
        )


def _assert_unique_grid_month(
    dataframe: pd.DataFrame,
    *,
    table_name: str,
) -> None:
    duplicate_count = int(
        dataframe[
            [
                "grid_cell_key",
                "reference_month",
            ]
        ]
        .duplicated()
        .sum()
    )

    if duplicate_count:
        raise NationalGISExportError(
            f"{table_name} contains "
            f"{duplicate_count} duplicate grid-month rows."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: Iterable[str],
    *,
    table_name: str,
) -> None:
    missing = (
        set(required_columns)
        - set(dataframe.columns)
    )

    if missing:
        raise NationalGISExportError(
            f"{table_name} is missing columns: "
            f"{sorted(missing)}"
        )


def _json_default(value: Any) -> Any:
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    raise TypeError(
        f"Object of type {type(value).__name__} "
        "is not JSON serializable."
    )