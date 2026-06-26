import pandas as pd
import pytest

from src.gold.hydro.monthly_features import (
    GoldHydroFeatureError,
    build_gold_hydro_station_month_feature,
)


def hydro_daily_row(
    date,
    measurement_type,
    value,
    symbol=None,
    station_id="05AA001",
    province="AB",
    latitude=49.606392,
    longitude=-114.04528,
):
    date_value = pd.Timestamp(date)

    return {
        "station_id": station_id,
        "observation_date": date_value.strftime("%Y-%m-%d"),
        "observation_year": date_value.year,
        "observation_month": date_value.month,
        "observation_day": date_value.day,
        "measurement_type": measurement_type,
        "measurement_value": value,
        "measurement_symbol": symbol,
        "province": province,
        "latitude": latitude,
        "longitude": longitude,
    }


def hydro_station_row(
    station_id="05AA001",
    station_name="OLDMAN RIVER NEAR COWLEY",
):
    return {
        "station_id": station_id,
        "station_name": station_name,
        "province": "AB",
        "latitude": 49.606392,
        "longitude": -114.04528,
        "drainage_area_gross": 1937.0,
        "drainage_area_effect": None,
        "rhbn": "0",
        "real_time": "0",
        "source_name": "hydat_archive",
        "geometry_type": "Point",
        "geometry_wkt": "POINT (-114.04528 49.606392)",
    }


def test_build_hydro_station_month_feature_aggregates_flow_and_level():
    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2026-01-01", "flow", 0.0),
            hydro_daily_row("2026-01-02", "flow", 10.0, "E"),
            hydro_daily_row("2026-01-03", "level", -0.5),
            hydro_daily_row("2026-01-04", "level", 1.5, "A"),
        ]
    )
    hydro_station = pd.DataFrame([hydro_station_row()])

    result = build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
        start_month="2026-01",
        end_month="2026-01",
    )

    assert len(result) == 2

    flow = result[result["measurement_type"] == "flow"].iloc[0]
    level = result[result["measurement_type"] == "level"].iloc[0]

    assert flow["hydro_station_month_key"] == "AB__05AA001__flow__2026-01"
    assert flow["daily_record_count"] == 2
    assert flow["observation_day_count"] == 2
    assert flow["flow_zero_day_count"] == 1
    assert flow["negative_value_count"] == 0
    assert flow["estimated_symbol_count"] == 1
    assert flow["mean_measurement_value"] == 5.0

    assert level["hydro_station_month_key"] == "AB__05AA001__level__2026-01"
    assert level["daily_record_count"] == 2
    assert level["negative_value_count"] == 1
    assert level["approved_symbol_count"] == 1
    assert level["mean_measurement_value"] == 0.5


def test_build_hydro_station_month_feature_filters_to_requested_month_window():
    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2015-12-31", "flow", 1.0),
            hydro_daily_row("2016-01-01", "flow", 2.0),
            hydro_daily_row("2025-12-31", "flow", 3.0),
            hydro_daily_row("2026-01-01", "flow", 4.0),
        ]
    )
    hydro_station = pd.DataFrame([hydro_station_row()])

    result = build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
        start_month="2016-01",
        end_month="2025-12",
    )

    assert result["reference_month"].tolist() == [
        "2016-01",
        "2025-12",
    ]
    assert result["daily_record_count"].sum() == 2


def test_build_hydro_station_month_feature_rejects_missing_station_metadata():
    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2026-01-01", "flow", 1.0),
        ]
    )
    hydro_station = pd.DataFrame(
        [
            hydro_station_row(station_id="DIFFERENT"),
        ]
    )

    with pytest.raises(
        GoldHydroFeatureError,
        match="missing from silver_hydro_station",
    ):
        build_gold_hydro_station_month_feature(
            hydro_daily=hydro_daily,
            hydro_station=hydro_station,
            start_month="2026-01",
            end_month="2026-01",
        )


def test_build_hydro_station_month_feature_rejects_unexpected_measurement_type():
    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2026-01-01", "temperature", 1.0),
        ]
    )
    hydro_station = pd.DataFrame([hydro_station_row()])

    with pytest.raises(
        GoldHydroFeatureError,
        match="unexpected measurement_type",
    ):
        build_gold_hydro_station_month_feature(
            hydro_daily=hydro_daily,
            hydro_station=hydro_station,
            start_month="2026-01",
            end_month="2026-01",
        )


def grid_box_around_hydro_station(
    *,
    longitude=-114.04528,
    latitude=49.606392,
    buffer_m=10_000,
):
    from pyproj import Transformer
    from shapely.geometry import box

    from src.gold.spatial.grid import ANALYSIS_CRS_EPSG

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


def make_test_grid(crs_epsg=3347):
    return pd.DataFrame(
        [
            {
                "grid_cell_key": "ab_10km_test",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_geometry_wkt": grid_box_around_hydro_station().wkt,
                "crs_epsg": crs_epsg,
            }
        ]
    )


def test_build_grid_month_hydro_feature_maps_station_to_grid():
    from src.gold.hydro.monthly_features import (
        build_gold_grid_month_hydro_feature,
    )

    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2026-01-01", "flow", 0.0),
            hydro_daily_row("2026-01-02", "flow", 10.0),
            hydro_daily_row("2026-01-01", "level", -0.5),
            hydro_daily_row("2026-01-02", "level", 1.5),
        ]
    )
    hydro_station = pd.DataFrame([hydro_station_row()])

    station_month = build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
        start_month="2026-01",
        end_month="2026-01",
    )

    grid_month, summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=make_test_grid(),
    )

    assert len(grid_month) == 2
    assert summary["grid_month_row_count"] == 2
    assert summary["mapped_station_count"] == 1
    assert summary["unmapped_station_count"] == 0

    assert set(grid_month["measurement_type"]) == {"flow", "level"}
    assert set(grid_month["grid_system"]) == {"ab_10km"}
    assert grid_month["station_count"].tolist() == [1, 1]
    assert grid_month["nearest_station_distance_km"].max() == 0.0

    flow = grid_month[grid_month["measurement_type"] == "flow"].iloc[0]
    assert flow["grid_month_hydro_feature_key"] == "ab_10km_test__flow__2026-01"
    assert flow["daily_record_count"] == 2
    assert flow["mean_measurement_value"] == 5.0


def test_build_grid_month_hydro_feature_rejects_non_3347_grid():
    from src.gold.hydro.monthly_features import (
        build_gold_grid_month_hydro_feature,
    )

    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row("2026-01-01", "flow", 1.0),
        ]
    )
    hydro_station = pd.DataFrame([hydro_station_row()])

    station_month = build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
        start_month="2026-01",
        end_month="2026-01",
    )

    with pytest.raises(
        GoldHydroFeatureError,
        match="EPSG:3347",
    ):
        build_gold_grid_month_hydro_feature(
            station_month=station_month,
            grid=make_test_grid(crs_epsg=4326),
        )
