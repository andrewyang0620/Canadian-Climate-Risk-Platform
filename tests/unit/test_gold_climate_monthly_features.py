import pandas as pd
from shapely.geometry import box

from src.gold.climate.monthly_features import (
    build_gold_climate_station_month_feature,
    build_gold_grid_month_climate_feature,
)


def test_station_month_feature_aggregates_daily_climate():
    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
            daily("2026-01-02", 2.0, -2.0, 5.0, 0.0),
        ]
    )

    result = build_gold_climate_station_month_feature(climate_daily)

    assert len(result) == 1

    row = result.iloc[0]

    assert row["station_id"] == "station_1"
    assert row["reference_month"] == "2026-01"
    assert row["daily_record_count"] == 2
    assert row["total_precip_mm"] == 12.0
    assert row["precipitation_days"] == 1
    assert row["heavy_precipitation_days"] == 1
    assert row["freeze_thaw_days"] == 2


def test_grid_month_feature_maps_station_to_grid():
    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
            daily("2026-01-02", 2.0, -2.0, 5.0, 0.0),
        ]
    )

    station_month = build_gold_climate_station_month_feature(climate_daily)

    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_geometry_wkt": box(
                    -1_000_000,
                    -1_000_000,
                    3_000_000,
                    3_000_000,
                ).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    assert len(result) == 1
    assert result.iloc[0]["grid_cell_key"] == "ab_10km_test"
    assert result.iloc[0]["station_count"] == 1
    assert result.iloc[0]["heavy_precipitation_days"] == 1
    assert summary.mapped_station_count == 1


def daily(date, mean_temp, min_temp, max_temp, precip):
    return {
        "station_id": "station_1",
        "station_name": "Station 1",
        "province": "AB",
        "observation_date": date,
        "latitude": 53.5,
        "longitude": -113.5,
        "mean_temp_c": mean_temp,
        "min_temp_c": min_temp,
        "max_temp_c": max_temp,
        "total_precip_mm": precip,
        "total_rain_mm": precip,
        "total_snow": 0.0,
    }


def test_grid_month_feature_handles_duplicate_station_coordinates():
    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
            {
                **daily("2026-02-01", 2.0, -2.0, 5.0, 0.0),
                "latitude": 53.5001,
                "longitude": -113.5001,
            },
        ]
    )

    station_month = build_gold_climate_station_month_feature(climate_daily)

    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_geometry_wkt": box(
                    -1_000_000,
                    -1_000_000,
                    3_000_000,
                    3_000_000,
                ).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    assert len(result) == 2
    assert summary.mapped_station_count == 1
    assert summary.unmapped_station_count == 0


def test_grid_month_feature_handles_duplicate_station_coordinates():
    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
            {
                **daily("2026-02-01", 2.0, -2.0, 5.0, 0.0),
                "latitude": 53.5001,
                "longitude": -113.5001,
            },
        ]
    )

    station_month = build_gold_climate_station_month_feature(climate_daily)

    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_geometry_wkt": box(
                    -1_000_000,
                    -1_000_000,
                    3_000_000,
                    3_000_000,
                ).wkt,
                "crs_epsg": 3347,
            }
        ]
    )

    result, summary = build_gold_grid_month_climate_feature(
        station_month=station_month,
        grid=grid,
    )

    assert len(result) == 2
    assert summary.mapped_station_count == 1
    assert summary.unmapped_station_count == 0
