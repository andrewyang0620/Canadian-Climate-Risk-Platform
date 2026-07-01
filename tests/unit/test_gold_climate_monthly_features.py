from __future__ import annotations

import math

import pandas as pd
import pytest
from pyproj import Transformer
from shapely.geometry import box

from src.gold.climate.monthly_features import (
    GoldClimateFeatureError,
    build_gold_climate_station_month_feature,
    build_gold_grid_month_climate_feature,
)


PROJECT_TO_DISPLAY = Transformer.from_crs("EPSG:3347", "EPSG:4326", always_xy=True)


def lat_lon_from_projected(x: float, y: float) -> tuple[float, float]:
    lon, lat = PROJECT_TO_DISPLAY.transform(x, y)
    return lat, lon


def station_month_row(
    *,
    station_id: str,
    province_key: str = "AB",
    reference_month: str = "2020-01",
    x: float,
    y: float,
    mean_temp_c: float = 10.0,
    min_temp_c: float = 5.0,
    max_temp_c: float = 15.0,
    total_precip_mm: float = 20.0,
) -> dict[str, object]:
    lat, lon = lat_lon_from_projected(x, y)

    return {
        "station_id": station_id,
        "station_name": f"Station {station_id}",
        "province_key": province_key,
        "reference_month": reference_month,
        "latitude": lat,
        "longitude": lon,
        "daily_record_count": 31,
        "temperature_observation_count": 31,
        "precipitation_observation_count": 31,
        "mean_temp_c": mean_temp_c,
        "min_temp_c": min_temp_c,
        "max_temp_c": max_temp_c,
        "observed_min_temp_c": min_temp_c - 2,
        "observed_max_temp_c": max_temp_c + 2,
        "total_precip_mm": total_precip_mm,
        "total_rain_mm": total_precip_mm * 0.8,
        "total_snow": total_precip_mm * 0.2,
        "precipitation_days": 8,
        "heavy_precipitation_days": 2,
        "extreme_heat_days": 0,
        "extreme_cold_days": 0,
        "freeze_thaw_days": 4,
        "temperature_completeness_ratio": 1.0,
        "precipitation_completeness_ratio": 1.0,
    }


def grid_row(
    *,
    grid_cell_key: str,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    province_key: str = "AB",
) -> dict[str, object]:
    return {
        "grid_cell_key": grid_cell_key,
        "grid_system": "ab_10km" if province_key == "AB" else "bc_10km",
        "grid_level": "province",
        "grid_version": "v1",
        "province_key": province_key,
        "centroid_x": (min_x + max_x) / 2,
        "centroid_y": (min_y + max_y) / 2,
        "analysis_geometry_wkt": box(min_x, min_y, max_x, max_y).wkt,
        "crs_epsg": 3347,
    }


def test_build_gold_climate_station_month_feature_aggregates_daily_records():
    climate_daily = pd.DataFrame(
        [
            {
                "station_id": "S1",
                "station_name": "Station 1",
                "province": "AB",
                "observation_date": "2020-01-01",
                "latitude": 52.0,
                "longitude": -114.0,
                "mean_temp_c": 1.0,
                "min_temp_c": -2.0,
                "max_temp_c": 4.0,
                "total_precip_mm": 10.0,
                "total_rain_mm": 8.0,
                "total_snow": 2.0,
            },
            {
                "station_id": "S1",
                "station_name": "Station 1",
                "province": "AB",
                "observation_date": "2020-01-02",
                "latitude": 52.0,
                "longitude": -114.0,
                "mean_temp_c": 3.0,
                "min_temp_c": -1.0,
                "max_temp_c": 5.0,
                "total_precip_mm": 0.0,
                "total_rain_mm": 0.0,
                "total_snow": 0.0,
            },
        ]
    )

    result = build_gold_climate_station_month_feature(climate_daily)

    assert len(result) == 1
    row = result.iloc[0]

    assert row["climate_station_month_key"] == "AB__S1__2020-01"
    assert row["daily_record_count"] == 2
    assert row["mean_temp_c"] == 2.0
    assert row["total_precip_mm"] == 10.0
    assert row["precipitation_days"] == 1
    assert row["temperature_completeness_ratio"] == pytest.approx(2 / 31)


def test_build_gold_grid_month_climate_feature_outputs_full_skeleton_with_mapping_methods():
    grid = pd.DataFrame(
        [
            grid_row(
                grid_cell_key="ab_direct",
                min_x=1_000_000,
                min_y=1_000_000,
                max_x=1_010_000,
                max_y=1_010_000,
            ),
            grid_row(
                grid_cell_key="ab_idw",
                min_x=1_080_000,
                min_y=1_000_000,
                max_x=1_090_000,
                max_y=1_010_000,
            ),
            grid_row(
                grid_cell_key="ab_no_station",
                min_x=1_400_000,
                min_y=1_000_000,
                max_x=1_410_000,
                max_y=1_010_000,
            ),
        ]
    )

    station_month = pd.DataFrame(
        [
            station_month_row(
                station_id="S1",
                x=1_005_000,
                y=1_005_000,
                mean_temp_c=10.0,
                total_precip_mm=20.0,
            )
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    assert len(result) == 3
    assert result["grid_cell_key"].nunique() == 3
    assert result["reference_month"].nunique() == 1

    direct = result[result["grid_cell_key"] == "ab_direct"].iloc[0]
    idw = result[result["grid_cell_key"] == "ab_idw"].iloc[0]
    no_station = result[result["grid_cell_key"] == "ab_no_station"].iloc[0]

    assert direct["climate_mapping_method"] == "direct_station_in_cell"
    assert direct["climate_station_count"] == 1
    assert direct["climate_feature_quality_flag"] == "direct"
    assert direct["climate_nearest_station_distance_km"] == 0.0
    assert direct["mean_temp_c"] == 10.0

    assert idw["climate_mapping_method"] == "idw_interpolated"
    assert idw["climate_station_count"] == 1
    assert idw["climate_nearest_station_distance_km"] > 0
    assert idw["climate_nearest_station_distance_km"] <= 150
    assert idw["climate_idw_confidence_score"] > 0
    assert idw["climate_feature_quality_flag"] in {
        "high",
        "medium",
        "low",
        "very_low",
    }
    assert idw["mean_temp_c"] == pytest.approx(10.0)

    assert no_station["climate_mapping_method"] == "no_station_within_radius"
    assert no_station["climate_station_count"] == 0
    assert no_station["climate_idw_confidence_score"] == 0.0
    assert pd.isna(no_station["climate_feature_quality_flag"])
    assert pd.isna(no_station["mean_temp_c"])
    assert pd.isna(no_station["total_precip_mm"])

    assert summary.grid_month_row_count == 3
    assert summary.grid_cell_count == 3
    assert summary.month_count == 1
    assert summary.direct_station_in_cell_grid_month_count == 1
    assert summary.idw_interpolated_grid_month_count == 1
    assert summary.no_station_within_radius_grid_month_count == 1
    assert summary.climate_value_coverage_rate == pytest.approx(2 / 3)


def test_build_gold_grid_month_climate_feature_averages_multiple_direct_stations():
    grid = pd.DataFrame(
        [
            grid_row(
                grid_cell_key="ab_direct_average",
                min_x=1_000_000,
                min_y=1_000_000,
                max_x=1_010_000,
                max_y=1_010_000,
            )
        ]
    )

    station_month = pd.DataFrame(
        [
            station_month_row(
                station_id="S1",
                x=1_003_000,
                y=1_003_000,
                mean_temp_c=10.0,
                total_precip_mm=20.0,
            ),
            station_month_row(
                station_id="S2",
                x=1_007_000,
                y=1_007_000,
                mean_temp_c=20.0,
                total_precip_mm=40.0,
            ),
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    row = result.iloc[0]

    assert len(result) == 1
    assert row["climate_mapping_method"] == "direct_station_average_in_cell"
    assert row["climate_station_count"] == 2
    assert row["climate_feature_quality_flag"] == "direct"
    assert row["mean_temp_c"] == pytest.approx(15.0)
    assert row["total_precip_mm"] == pytest.approx(30.0)
    assert summary.direct_station_average_in_cell_grid_month_count == 1


def test_build_gold_grid_month_climate_feature_uses_idw_weighting():
    grid = pd.DataFrame(
        [
            grid_row(
                grid_cell_key="ab_idw_only",
                min_x=1_000_000,
                min_y=1_000_000,
                max_x=1_010_000,
                max_y=1_010_000,
            )
        ]
    )

    station_month = pd.DataFrame(
        [
            station_month_row(
                station_id="S_near",
                x=955_000,
                y=1_005_000,
                mean_temp_c=10.0,
                total_precip_mm=10.0,
            ),
            station_month_row(
                station_id="S_far",
                x=905_000,
                y=1_005_000,
                mean_temp_c=30.0,
                total_precip_mm=30.0,
            ),
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    row = result.iloc[0]

    assert row["climate_mapping_method"] == "idw_interpolated"
    assert row["climate_station_count"] == 2
    assert row["climate_nearest_station_distance_km"] == pytest.approx(50.0)
    assert row["climate_max_station_distance_km"] == pytest.approx(100.0)

    expected = ((10.0 * (1 / 50.0**2)) + (30.0 * (1 / 100.0**2))) / (
        (1 / 50.0**2) + (1 / 100.0**2)
    )

    assert row["mean_temp_c"] == pytest.approx(expected)
    assert summary.idw_interpolated_grid_month_count == 1


def test_build_gold_grid_month_climate_feature_requires_ab_bc_10km_grid():
    station_month = pd.DataFrame(
        [
            station_month_row(
                station_id="S1",
                x=1_005_000,
                y=1_005_000,
            )
        ]
    )

    grid = pd.DataFrame(
        [
            {
                **grid_row(
                    grid_cell_key="calgary_1km_test",
                    min_x=1_000_000,
                    min_y=1_000_000,
                    max_x=1_001_000,
                    max_y=1_001_000,
                ),
                "grid_system": "calgary_1km",
            }
        ]
    )

    with pytest.raises(GoldClimateFeatureError, match="No AB/BC 10km grid cells"):
        build_gold_grid_month_climate_feature(
            station_month=station_month,
            grid=grid,
        )


def test_build_gold_grid_month_climate_feature_rejects_non_analysis_crs():
    station_month = pd.DataFrame(
        [
            station_month_row(
                station_id="S1",
                x=1_005_000,
                y=1_005_000,
            )
        ]
    )

    grid = pd.DataFrame(
        [
            {
                **grid_row(
                    grid_cell_key="ab_bad_crs",
                    min_x=1_000_000,
                    min_y=1_000_000,
                    max_x=1_010_000,
                    max_y=1_010_000,
                ),
                "crs_epsg": 4326,
            }
        ]
    )

    with pytest.raises(GoldClimateFeatureError, match="EPSG:3347"):
        build_gold_grid_month_climate_feature(
            station_month=station_month,
            grid=grid,
        )


def test_build_gold_climate_station_month_feature_requires_columns():
    climate_daily = pd.DataFrame(
        [
            {
                "station_id": "S1",
            }
        ]
    )

    with pytest.raises(GoldClimateFeatureError, match="missing columns"):
        build_gold_climate_station_month_feature(climate_daily)


def test_idw_expected_value_sanity():
    expected = ((10.0 * (1 / 50.0**2)) + (30.0 * (1 / 100.0**2))) / (
        (1 / 50.0**2) + (1 / 100.0**2)
    )

    assert math.isclose(expected, 14.0)
