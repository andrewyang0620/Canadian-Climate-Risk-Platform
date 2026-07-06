from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.silver.validate_hydro_basin_polygon import (
    validate_hydro_basin_polygon_silver_outputs,
)


def test_validate_hydro_basin_polygon_silver_outputs_passes(tmp_path: Path):
    run_id = "test-run"
    extract_date = "2026-06-28"

    write_table(
        tmp_path,
        table_name="silver_hydro_basin_polygon",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=polygon_df(["11AA001", "11AA002", "08HDX03"]),
    )
    write_table(
        tmp_path,
        table_name="silver_hydro_basin_pour_point",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=pour_point_df(["11AA001", "11AA002", "08HDX03"]),
    )
    write_table(
        tmp_path,
        table_name="silver_hydro_basin_station_point",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=station_point_df(["11AA001", "11AA002", "08HDX03"]),
    )

    output_json = tmp_path / "_validation" / "hydro_basin_polygon" / "latest_validation.json"

    report = validate_hydro_basin_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=output_json,
    )

    assert report.passed
    assert output_json.exists()
    assert len(report.checks) == 20


def test_validate_hydro_basin_polygon_silver_outputs_fails_on_station_mismatch(
    tmp_path: Path,
):
    run_id = "test-run"
    extract_date = "2026-06-28"

    write_table(
        tmp_path,
        table_name="silver_hydro_basin_polygon",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=polygon_df(["11AA001", "11AA002"]),
    )
    write_table(
        tmp_path,
        table_name="silver_hydro_basin_pour_point",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=pour_point_df(["11AA001"]),
    )
    write_table(
        tmp_path,
        table_name="silver_hydro_basin_station_point",
        extract_date=extract_date,
        run_id=run_id,
        dataframe=station_point_df(["11AA001", "11AA002"]),
    )

    report = validate_hydro_basin_polygon_silver_outputs(
        silver_root=tmp_path,
        output_json_path=None,
    )

    assert not report.passed

    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "silver_hydro_basin_pour_point_station_alignment_with_polygon" in failed_checks


def write_table(
    root: Path,
    *,
    table_name: str,
    extract_date: str,
    run_id: str,
    dataframe: pd.DataFrame,
) -> None:
    output_path = (
        root
        / table_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / f"{table_name}.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)


def base_rows(station_ids: list[str], *, geometry_type: str, source_layer: str) -> list[dict]:
    return [
        {
            "station_id": station_id,
            "station_name": f"Station {station_id}",
            "status": "active",
            "status_fr": "en service",
            "geometry_type": geometry_type,
            "geometry_wkt": (
                "POINT (-113 49)"
                if geometry_type == "Point"
                else "POLYGON ((-113 49, -112.9 49, -112.9 49.1, -113 49.1, -113 49))"
            ),
            "geometry_original_is_valid": True,
            "geometry_was_repaired": False,
            "geometry_is_valid": True,
            "source_crs": "EPSG:4326",
            "source_name": "national_hydrometric_basin_polygons",
            "source_layer": source_layer,
            "source_file": f"MDA_ADP_11_{source_layer}.geojson",
            "mda_adp_region": "11",
        }
        for station_id in station_ids
    ]


def polygon_df(station_ids: list[str]) -> pd.DataFrame:
    rows = []

    for row in base_rows(
        station_ids,
        geometry_type="Polygon",
        source_layer="drainage_basin",
    ):
        rows.append(
            {
                "hydro_basin_polygon_key": row["station_id"],
                **row,
                "basin_area_sq_km": 239.413,
                "basin_area_sq_km_fr": 239.413,
                "remark": None,
                "remark_fr": None,
                "source_version": "June 2024 / juin 2024",
                "source_revision_date": "2024-06-01",
                "shape_length_m": 116640.0,
                "shape_area_sq_m": 239412600.0,
            }
        )

    return pd.DataFrame(rows)


def pour_point_df(station_ids: list[str]) -> pd.DataFrame:
    rows = []

    for row in base_rows(
        station_ids,
        geometry_type="Point",
        source_layer="pour_point",
    ):
        rows.append(
            {
                "hydro_basin_pour_point_key": row["station_id"],
                **row,
                "province_or_territory": "AB",
            }
        )

    return pd.DataFrame(rows)


def station_point_df(station_ids: list[str]) -> pd.DataFrame:
    rows = []

    for row in base_rows(
        station_ids,
        geometry_type="Point",
        source_layer="station_point",
    ):
        rows.append(
            {
                "hydro_basin_station_point_key": row["station_id"],
                **row,
                "province_or_territory": "AB",
                "hydat_version": "2021-01",
            }
        )

    return pd.DataFrame(rows)
