import pandas as pd
import pytest

from src.gold.hydro.monthly_features import (
    GoldHydroFeatureError,
    build_gold_grid_month_hydro_feature,
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


def projected_box_around_point(
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


def make_test_grid(
    *,
    crs_epsg=3347,
    include_far_grid=False,
    longitude=-114.04528,
    latitude=49.606392,
):
    rows = [
        {
            "grid_cell_key": "ab_10km_test",
            "grid_system": "ab_10km",
            "grid_level": "province",
            "grid_version": "v1",
            "province_key": "AB",
            "analysis_geometry_wkt": projected_box_around_point(
                longitude=longitude,
                latitude=latitude,
            ).wkt,
            "crs_epsg": crs_epsg,
        }
    ]

    if include_far_grid:
        rows.append(
            {
                "grid_cell_key": "ab_10km_far",
                "grid_system": "ab_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "AB",
                "analysis_geometry_wkt": projected_box_around_point(
                    longitude=-112.0,
                    latitude=53.0,
                ).wkt,
                "crs_epsg": crs_epsg,
            }
        )

    return pd.DataFrame(rows)


def basin_polygon_row(
    *,
    station_id="05AA001",
    geometry=None,
    crs_epsg=3347,
):
    if geometry is None:
        geometry = projected_box_around_point(buffer_m=8_000)

    return {
        "station_id": station_id,
        "geometry_wkt": geometry.wkt,
        "crs_epsg": crs_epsg,
    }


def make_basin_polygon(
    *,
    station_id="05AA001",
    geometry=None,
    crs_epsg=3347,
):
    return pd.DataFrame(
        [
            basin_polygon_row(
                station_id=station_id,
                geometry=geometry,
                crs_epsg=crs_epsg,
            )
        ]
    )


def make_station_month(
    *,
    station_id="05AA001",
    start_month="2026-01",
    end_month="2026-01",
):
    hydro_daily = pd.DataFrame(
        [
            hydro_daily_row(
                "2026-01-01",
                "flow",
                0.0,
                station_id=station_id,
            ),
            hydro_daily_row(
                "2026-01-02",
                "flow",
                10.0,
                station_id=station_id,
            ),
            hydro_daily_row(
                "2026-01-01",
                "level",
                -0.5,
                station_id=station_id,
            ),
            hydro_daily_row(
                "2026-01-02",
                "level",
                1.5,
                station_id=station_id,
            ),
        ]
    )
    hydro_station = pd.DataFrame(
        [
            hydro_station_row(station_id=station_id),
        ]
    )

    return build_gold_hydro_station_month_feature(
        hydro_daily=hydro_daily,
        hydro_station=hydro_station,
        start_month=start_month,
        end_month=end_month,
    )


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


def test_build_grid_month_hydro_feature_uses_basin_intersection_first():
    station_month = make_station_month()
    grid = make_test_grid(include_far_grid=True)
    basin_polygon = make_basin_polygon()

    grid_month, summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=grid,
        basin_polygon=basin_polygon,
        start_month="2026-01",
        end_month="2026-01",
    )

    assert len(grid_month) == 2
    assert summary["grid_month_row_count"] == 2
    assert summary["basin_matched_station_count"] == 1
    assert summary["basin_unmatched_station_count"] == 0
    assert summary["basin_covered_grid_count"] == 1
    assert summary["covered_grid_count"] == 1
    assert summary["no_hydro_coverage_grid_count"] == 1

    covered = grid_month[grid_month["grid_cell_key"] == "ab_10km_test"].iloc[0]
    no_coverage = grid_month[grid_month["grid_cell_key"] == "ab_10km_far"].iloc[0]

    assert covered["grid_month_hydro_feature_key"] == "ab_10km_test__2026-01"
    assert covered["hydro_spatial_assignment_method"] == "basin_polygon_intersection"
    assert covered["hydro_station_count"] == 1
    assert covered["hydro_basin_station_count"] == 1
    assert covered["hydro_point_station_count"] == 0
    assert covered["flow_station_count"] == 1
    assert covered["level_station_count"] == 1
    assert covered["flow_daily_record_count"] == 2
    assert covered["level_daily_record_count"] == 2
    assert covered["flow_mean_measurement_value"] == 5.0
    assert covered["level_mean_measurement_value"] == 0.5
    assert covered["hydro_basin_grid_coverage_ratio"] > 0

    assert no_coverage["hydro_spatial_assignment_method"] == "no_hydro_coverage"
    assert no_coverage["hydro_station_count"] == 0
    assert no_coverage["flow_station_count"] == 0
    assert pd.isna(no_coverage["flow_mean_measurement_value"])
    assert pd.isna(no_coverage["hydro_feature_quality_flag"])


def test_build_grid_month_hydro_feature_uses_point_in_cell_for_unmatched_station():
    station_month = make_station_month(station_id="05ZZ999")
    grid = make_test_grid()
    basin_polygon = make_basin_polygon(station_id="DIFFERENT")

    grid_month, summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=grid,
        basin_polygon=basin_polygon,
        start_month="2026-01",
        end_month="2026-01",
    )

    assert len(grid_month) == 1
    assert summary["basin_matched_station_count"] == 0
    assert summary["basin_unmatched_station_count"] == 1
    assert summary["point_in_cell_station_count"] == 1
    assert summary["point_in_cell_grid_count"] == 1
    assert summary["covered_grid_count"] == 1

    row = grid_month.iloc[0]

    assert row["hydro_spatial_assignment_method"] == "station_point_in_cell"
    assert row["hydro_station_count"] == 1
    assert row["hydro_basin_station_count"] == 0
    assert row["hydro_point_station_count"] == 1
    assert row["flow_station_count"] == 1
    assert row["flow_mean_measurement_value"] == 5.0


def test_build_grid_month_hydro_feature_does_not_use_nearest_grid_fallback():
    station_month = make_station_month(station_id="05ZZ999")
    grid = make_test_grid(
        longitude=-112.0,
        latitude=53.0,
    )
    basin_polygon = make_basin_polygon(station_id="DIFFERENT")

    grid_month, summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=grid,
        basin_polygon=basin_polygon,
        start_month="2026-01",
        end_month="2026-01",
    )

    assert len(grid_month) == 1
    assert summary["point_in_cell_station_count"] == 0
    assert summary["covered_grid_count"] == 0
    assert summary["no_hydro_coverage_grid_count"] == 1

    row = grid_month.iloc[0]

    assert row["hydro_spatial_assignment_method"] == "no_hydro_coverage"
    assert row["hydro_station_count"] == 0
    assert row["flow_station_count"] == 0
    assert pd.isna(row["flow_mean_measurement_value"])
    assert pd.isna(row["hydro_feature_quality_flag"])


def test_build_grid_month_hydro_feature_rejects_non_3347_grid():
    station_month = make_station_month()
    basin_polygon = make_basin_polygon()

    with pytest.raises(
        GoldHydroFeatureError,
        match="EPSG:3347",
    ):
        build_gold_grid_month_hydro_feature(
            station_month=station_month,
            grid=make_test_grid(crs_epsg=4326),
            basin_polygon=basin_polygon,
            start_month="2026-01",
            end_month="2026-01",
        )


def test_build_grid_month_hydro_feature_keeps_basin_values_when_grid_province_differs_from_station():
    station_month = make_station_month()

    grid = pd.DataFrame(
        [
            {
                "grid_cell_key": "bc_10km_cross_province_test",
                "grid_system": "bc_10km",
                "grid_level": "province",
                "grid_version": "v1",
                "province_key": "BC",
                "analysis_geometry_wkt": projected_box_around_point().wkt,
                "crs_epsg": 3347,
            }
        ]
    )
    basin_polygon = make_basin_polygon()

    grid_month, summary = build_gold_grid_month_hydro_feature(
        station_month=station_month,
        grid=grid,
        basin_polygon=basin_polygon,
        start_month="2026-01",
        end_month="2026-01",
    )

    assert len(grid_month) == 1
    assert summary["basin_covered_grid_count"] == 1

    row = grid_month.iloc[0]

    assert row["province_key"] == "BC"
    assert row["hydro_spatial_assignment_method"] == "basin_polygon_intersection"
    assert row["hydro_station_count"] == 1
    assert row["flow_station_count"] == 1
    assert row["level_station_count"] == 1
    assert row["flow_mean_measurement_value"] == 5.0
    assert row["level_mean_measurement_value"] == 0.5
