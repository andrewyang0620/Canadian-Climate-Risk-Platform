from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from src.silver.hydro_basin_polygon import (
    SILVER_TABLES,
    standardize_hydro_basin_package,
)


def test_standardize_hydro_basin_package_writes_three_aligned_tables(tmp_path: Path):
    archive_path = build_hydro_basin_raw_archive(
        tmp_path=tmp_path,
        polygon_station_ids=["11AA001", "11AA002"],
        pour_point_station_ids=["11AA001", "11AA002"],
        station_point_station_ids=["11AA001", "11AA002"],
    )

    tables = standardize_hydro_basin_package(archive_path)

    assert set(tables) == set(SILVER_TABLES.values())

    polygon = tables["silver_hydro_basin_polygon"]
    pour_point = tables["silver_hydro_basin_pour_point"]
    station_point = tables["silver_hydro_basin_station_point"]

    assert len(polygon) == 2
    assert len(pour_point) == 2
    assert len(station_point) == 2

    assert set(polygon["station_id"]) == {"11AA001", "11AA002"}
    assert set(pour_point["station_id"]) == {"11AA001", "11AA002"}
    assert set(station_point["station_id"]) == {"11AA001", "11AA002"}

    assert set(polygon["geometry_type"]) == {"Polygon"}
    assert set(pour_point["geometry_type"]) == {"Point"}
    assert set(station_point["geometry_type"]) == {"Point"}

    assert polygon["source_crs"].eq("EPSG:4326").all()
    assert pour_point["source_crs"].eq("EPSG:4326").all()
    assert station_point["source_crs"].eq("EPSG:4326").all()

    first_polygon = polygon[polygon["station_id"] == "11AA001"].iloc[0]
    assert first_polygon["hydro_basin_polygon_key"] == "11AA001"
    assert first_polygon["basin_area_sq_km"] == 239.413
    assert first_polygon["source_revision_date"] == "2024-06-01"

    first_station_point = station_point[station_point["station_id"] == "11AA001"].iloc[0]
    assert first_station_point["hydat_version"] == "2021-01"
    assert first_station_point["province_or_territory"] == "AB"


def test_standardize_hydro_basin_package_rejects_cross_layer_station_mismatch(
    tmp_path: Path,
):
    archive_path = build_hydro_basin_raw_archive(
        tmp_path=tmp_path,
        polygon_station_ids=["11AA001", "11AA002"],
        pour_point_station_ids=["11AA001", "11AA002"],
        station_point_station_ids=["11AA001"],
    )

    with pytest.raises(ValueError, match="Station ID mismatch"):
        standardize_hydro_basin_package(archive_path)


def build_hydro_basin_raw_archive(
    *,
    tmp_path: Path,
    polygon_station_ids: list[str],
    pour_point_station_ids: list[str],
    station_point_station_ids: list[str],
) -> Path:
    inner_zip_path = tmp_path / "MDA_ADP_11.zip"

    with zipfile.ZipFile(inner_zip_path, "w") as archive:
        archive.writestr(
            "MDA_ADP_11_DrainageBasin_BassinDeDrainage.geojson",
            json.dumps(
                feature_collection(
                    [polygon_feature(station_id) for station_id in polygon_station_ids]
                )
            ),
        )
        archive.writestr(
            "MDA_ADP_11_PourPoint_PointExutoire.geojson",
            json.dumps(
                feature_collection(
                    [
                        point_feature(station_id, include_hydat_version=False)
                        for station_id in pour_point_station_ids
                    ]
                )
            ),
        )
        archive.writestr(
            "MDA_ADP_11_Station.geojson",
            json.dumps(
                feature_collection(
                    [
                        point_feature(station_id, include_hydat_version=True)
                        for station_id in station_point_station_ids
                    ]
                )
            ),
        )

    outer_zip_path = tmp_path / "national_hydrometric_basin_polygons_raw.zip"

    with zipfile.ZipFile(outer_zip_path, "w") as archive:
        archive.write(inner_zip_path, arcname="geojson/MDA_ADP_11.zip")

    return outer_zip_path


def feature_collection(features: list[dict]) -> dict:
    return {
        "type": "FeatureCollection",
        "name": "test_hydro_basin_layer",
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:EPSG::4326"},
        },
        "features": features,
    }


def polygon_feature(station_id: str) -> dict:
    return {
        "type": "Feature",
        "properties": {
            "StationNum": station_id,
            "NameNom": f"Station {station_id}",
            "Status": "active",
            "Etat": "en service",
            "Area_km2": 239.413,
            "Aire_km2": 239.413,
            "Remark": None,
            "Remarque": None,
            "Version": "June 2024 / juin 2024",
            "Date_rev": "2024-06-01",
            "Shape_Leng": 116640.0,
            "Shape_Area": 239412600.0,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-113.0, 49.0],
                    [-112.9, 49.0],
                    [-112.9, 49.1],
                    [-113.0, 49.1],
                    [-113.0, 49.0],
                ]
            ],
        },
    }


def point_feature(station_id: str, *, include_hydat_version: bool) -> dict:
    properties = {
        "StationNum": station_id,
        "NameNom": f"Station {station_id}",
        "Status": "active",
        "Etat": "en service",
        "ProvTerr": "AB",
    }

    if include_hydat_version:
        properties["HYDAT_ver"] = "2021-01"

    return {
        "type": "Feature",
        "properties": properties,
        "geometry": {
            "type": "Point",
            "coordinates": [-113.0, 49.0],
        },
    }
