import os
import pandas as pd
from pyproj import Transformer
from shapely.geometry import box

from src.gold.spatial.grid import ANALYSIS_CRS_EPSG
from src.gold.climate.monthly_features import (
    build_gold_climate_station_month_feature,
    build_gold_grid_month_climate_feature,
    read_silver_climate_daily,
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
                "analysis_geometry_wkt": grid_box_around_station().wkt,
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


def grid_box_around_station(
    *,
    longitude=-113.5,
    latitude=53.5,
    buffer_m=10_000,
):
    transformer = Transformer.from_crs(
        4326,
        ANALYSIS_CRS_EPSG,
        always_xy=True,
    )
    x, y = transformer.transform(longitude, latitude)

    return box(
        x - buffer_m,
        y - buffer_m,
        x + buffer_m,
        y + buffer_m,
    )


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
                "analysis_geometry_wkt": grid_box_around_station().wkt,
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
                "analysis_geometry_wkt": grid_box_around_station().wkt,
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


def test_read_silver_climate_daily_uses_latest_extract_date_and_run(tmp_path):
    table_root = tmp_path / "silver_climate_daily"

    old_extract_file = write_silver_climate_partition(
        table_root=table_root,
        extract_date="2026-01-01",
        run_id="old_run",
        station_id="old_extract",
        modified_time=1000,
    )
    latest_extract_old_run_file = write_silver_climate_partition(
        table_root=table_root,
        extract_date="2026-01-02",
        run_id="old_run_same_extract",
        station_id="old_run_same_extract",
        modified_time=2000,
    )
    latest_file = write_silver_climate_partition(
        table_root=table_root,
        extract_date="2026-01-02",
        run_id="latest_run_same_extract",
        station_id="latest_run_same_extract",
        modified_time=3000,
    )

    result = read_silver_climate_daily(
        silver_climate_root=table_root,
    )

    assert result["station_id"].tolist() == ["latest_run_same_extract"]

    assert old_extract_file.exists()
    assert latest_extract_old_run_file.exists()
    assert latest_file.exists()


def write_silver_climate_partition(
    *,
    table_root,
    extract_date,
    run_id,
    station_id,
    modified_time,
):
    output_dir = (
        table_root / f"extract_date={extract_date}" / f"run_id={run_id}" / "observation_year=2026"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "silver_climate_daily.parquet"

    row = daily(
        "2026-01-01",
        1.0,
        -1.0,
        4.0,
        12.0,
    )
    row["station_id"] = station_id

    pd.DataFrame([row]).to_parquet(
        output_path,
        index=False,
    )

    os.utime(
        output_path,
        (modified_time, modified_time),
    )

    return output_path


def test_station_month_feature_uses_station_id_month_grain_not_station_name():
    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
            {
                **daily("2026-01-02", 2.0, -2.0, 5.0, 0.0),
                "station_name": "Station 1 Renamed",
            },
        ]
    )

    result = build_gold_climate_station_month_feature(climate_daily)

    assert len(result) == 1
    assert result.iloc[0]["climate_station_month_key"] == "AB__station_1__2026-01"
    assert result.iloc[0]["daily_record_count"] == 2


def test_grid_month_feature_rejects_non_3347_grid_crs():
    import pytest
    from shapely.geometry import box

    from src.gold.climate.monthly_features import (
        GoldClimateFeatureError,
        build_gold_grid_month_climate_feature,
        build_gold_climate_station_month_feature,
    )

    climate_daily = pd.DataFrame(
        [
            daily("2026-01-01", 1.0, -1.0, 4.0, 12.0),
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
                "analysis_geometry_wkt": grid_box_around_station().wkt,
                "crs_epsg": 4326,
            }
        ]
    )

    with pytest.raises(
        GoldClimateFeatureError,
        match="EPSG:3347",
    ):
        build_gold_grid_month_climate_feature(
            station_month=station_month,
            grid=grid,
        )
