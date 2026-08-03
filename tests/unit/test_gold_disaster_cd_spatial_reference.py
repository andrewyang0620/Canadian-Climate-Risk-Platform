from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.gold.disaster.cd_spatial_reference import (
    GoldDisasterCDSpatialReferenceError,
    build_gold_disaster_cd_spatial_reference,
)
from src.gold.disaster.cd_spatial_validation import (
    GoldDisasterCDSpatialReferenceValidationError,
    validate_gold_disaster_cd_spatial_reference,
)


def _write_test_cd_shapefile(path: Path) -> Path:
    shapefile = pytest.importorskip("shapefile")

    shp_path = path / "test_cd_boundary.shp"

    writer = shapefile.Writer(str(shp_path))
    writer.field("CDUID", "C")
    writer.field("CDNAME", "C")
    writer.field("CDTYPE", "C")
    writer.field("DGUID", "C")
    writer.field("PRUID", "C")
    writer.field("LANDAREA", "N", decimal=3)

    writer.poly(
        [
            [
                [0.0, 0.0],
                [0.0, 1.0],
                [1.0, 1.0],
                [1.0, 0.0],
                [0.0, 0.0],
            ]
        ]
    )
    writer.record(
        "4806",
        "Calgary",
        "CDR",
        "2021A00034806",
        "48",
        1234.5,
    )

    writer.poly(
        [
            [
                [2.0, 2.0],
                [2.0, 3.0],
                [3.0, 3.0],
                [3.0, 2.0],
                [2.0, 2.0],
            ]
        ]
    )
    writer.record(
        "5935",
        "Central Okanagan",
        "RD",
        "2021A00035935",
        "59",
        9876.5,
    )

    writer.poly(
        [
            [
                [4.0, 4.0],
                [4.0, 5.0],
                [5.0, 5.0],
                [5.0, 4.0],
                [4.0, 4.0],
            ]
        ]
    )
    writer.record(
        "3506",
        "Ottawa",
        "CDR",
        "2021A00033506",
        "35",
        1111.1,
    )

    writer.close()

    shp_path.with_suffix(".prj").write_text(
        'PROJCS["NAD83_Statistics_Canada_Lambert"]',
        encoding="utf-8",
    )

    return shp_path


def _valid_cd_reference_frame() -> pd.DataFrame:
    ab_keys = [f"48{suffix:02d}" for suffix in range(1, 20)]

    bc_keys = [
        "5901",
        "5903",
        "5905",
        "5907",
        "5909",
        "5915",
        "5917",
        "5919",
        "5921",
        "5923",
        "5924",
        "5926",
        "5927",
        "5929",
        "5931",
        "5933",
        "5935",
        "5937",
        "5939",
        "5941",
        "5943",
        "5945",
        "5947",
        "5949",
        "5951",
        "5953",
        "5955",
        "5957",
        "5959",
    ]

    rows = []

    for key in ab_keys:
        rows.append(
            {
                "census_division_key": key,
                "census_division_name": f"AB CD {key}",
                "census_division_type": "CDR",
                "dguid": f"2021A0003{key}",
                "province_uid": "48",
                "province_key": "AB",
                "land_area_sq_km": 100.0,
                "geometry_area_m2": 100_000_000.0,
                "geometry_crs_epsg": 3347,
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            }
        )

    for key in bc_keys:
        rows.append(
            {
                "census_division_key": key,
                "census_division_name": f"BC CD {key}",
                "census_division_type": "RD",
                "dguid": f"2021A0003{key}",
                "province_uid": "59",
                "province_key": "BC",
                "land_area_sq_km": 200.0,
                "geometry_area_m2": 200_000_000.0,
                "geometry_crs_epsg": 3347,
                "geometry_wkt": "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)))",
            }
        )

    return pd.DataFrame(rows)


def test_build_gold_disaster_cd_spatial_reference_filters_ab_bc_only(
    tmp_path: Path,
) -> None:
    shp_path = _write_test_cd_shapefile(tmp_path)

    result, summary = build_gold_disaster_cd_spatial_reference(
        source_path=shp_path,
    )

    assert len(result) == 2
    assert summary["row_count"] == 2
    assert summary["province_counts"] == {"BC": 1, "AB": 1}

    assert set(result["province_key"]) == {"AB", "BC"}
    assert set(result["census_division_key"]) == {"4806", "5935"}
    assert result["geometry_wkt"].notna().all()
    assert result["geometry_wkt"].str.startswith("POLYGON").all()
    assert (result["geometry_area_m2"] > 0).all()


def test_build_gold_disaster_cd_spatial_reference_rejects_missing_source() -> None:
    with pytest.raises(
        GoldDisasterCDSpatialReferenceError,
        match="Source path does not exist",
    ):
        build_gold_disaster_cd_spatial_reference(
            source_path="missing_file.shp",
        )


def test_validate_gold_disaster_cd_spatial_reference_passes_for_valid_table() -> None:
    dataframe = _valid_cd_reference_frame()

    report = validate_gold_disaster_cd_spatial_reference(dataframe)

    assert report["validation_status"] == "passed"
    assert report["row_count"] == 48
    assert report["province_counts"] == {"BC": 29, "AB": 19}
    assert report["geometry_null_count"] == 0
    assert report["geometry_area_positive_count"] == 48


def test_validate_gold_disaster_cd_spatial_reference_rejects_duplicate_cd_key() -> None:
    dataframe = _valid_cd_reference_frame()
    dataframe.loc[1, "census_division_key"] = dataframe.loc[0, "census_division_key"]

    with pytest.raises(
        GoldDisasterCDSpatialReferenceValidationError,
        match="duplicates",
    ):
        validate_gold_disaster_cd_spatial_reference(dataframe)


def test_validate_gold_disaster_cd_spatial_reference_rejects_wrong_province_count() -> None:
    dataframe = _valid_cd_reference_frame()
    dataframe = dataframe[dataframe["census_division_key"] != "4819"].copy()

    with pytest.raises(
        GoldDisasterCDSpatialReferenceValidationError,
        match="Expected 19 Alberta Census Divisions",
    ):
        validate_gold_disaster_cd_spatial_reference(dataframe)


def test_validate_gold_disaster_cd_spatial_reference_rejects_invalid_geometry() -> None:
    dataframe = _valid_cd_reference_frame()
    dataframe.loc[0, "geometry_wkt"] = "POINT (0 0)"

    with pytest.raises(
        GoldDisasterCDSpatialReferenceValidationError,
        match="POLYGON or MULTIPOLYGON",
    ):
        validate_gold_disaster_cd_spatial_reference(dataframe)


def test_validate_gold_disaster_cd_spatial_reference_rejects_wrong_crs() -> None:
    dataframe = _valid_cd_reference_frame()
    dataframe.loc[0, "geometry_crs_epsg"] = 4326

    with pytest.raises(
        GoldDisasterCDSpatialReferenceValidationError,
        match="geometry_crs_epsg",
    ):
        validate_gold_disaster_cd_spatial_reference(dataframe)
