from __future__ import annotations

from datetime import date
from pathlib import Path
import zipfile

import shapefile

from src.silver.wildfire_perimeter_polygon import (
    standardize_wildfire_perimeter_package,
)


PRJ_TEXT = (
    'PROJCS["NAD_1983_Lambert_Conformal_Conic",'
    'GEOGCS["GCS_North_American_1983",'
    'DATUM["D_North_American_1983",'
    'SPHEROID["GRS_1980",6378137.0,298.257222101]]]]'
)


def write_nfdb_polygon_shapefile(
    base_path: Path,
    *,
    records: list[dict],
) -> None:
    writer = shapefile.Writer(str(base_path), shapeType=shapefile.POLYGON)

    writer.field("SRC_AGENCY", "C", size=10)
    writer.field("NAT_PARK", "C", size=8)
    writer.field("FIRE_ID", "C", size=16)
    writer.field("FIRENAME", "C", size=50)
    writer.field("YEAR", "N", size=10, decimal=0)
    writer.field("MONTH", "N", size=10, decimal=0)
    writer.field("DAY", "N", size=10, decimal=0)
    writer.field("REP_DATE", "D")
    writer.field("DATE_TYPE", "C", size=16)
    writer.field("OUT_DATE", "D")
    writer.field("DECADE", "C", size=10)
    writer.field("SIZE_HA", "F", size=19, decimal=11)
    writer.field("CALC_HA", "F", size=19, decimal=11)
    writer.field("CAUSE", "C", size=9)
    writer.field("PRESCRIBED", "C", size=10)
    writer.field("MAP_SOURCE", "C", size=50)
    writer.field("SOURCE_KEY", "C", size=50)
    writer.field("MAP_METHOD", "C", size=50)
    writer.field("WATER_REM", "C", size=3)
    writer.field("UNBURN_REM", "C", size=3)
    writer.field("MORE_INFO", "C", size=250)
    writer.field("POLY_DATE", "D")
    writer.field("CFS_REF_ID", "C", size=50)
    writer.field("CFS_NOTE1", "C", size=200)
    writer.field("CFS_NOTE2", "C", size=200)
    writer.field("AG_SRCFILE", "C", size=50)
    writer.field("ACQ_DATE", "D")

    for idx, record in enumerate(records):
        x = float(idx)

        writer.poly(
            [
                [
                    [x, 0.0],
                    [x + 0.1, 0.0],
                    [x + 0.1, 0.1],
                    [x, 0.1],
                    [x, 0.0],
                ]
            ]
        )

        writer.record(
            record.get("SRC_AGENCY", "BC"),
            record.get("NAT_PARK", ""),
            record.get("FIRE_ID", "2016-N10037"),
            record.get("FIRENAME", "TEST FIRE"),
            record.get("YEAR", 2016),
            record.get("MONTH", 5),
            record.get("DAY", 13),
            record.get("REP_DATE", date(2016, 5, 13)),
            record.get("DATE_TYPE", "REPORT"),
            record.get("OUT_DATE", None),
            record.get("DECADE", "2010"),
            record.get("SIZE_HA", 3.5),
            record.get("CALC_HA", 3.50593667863),
            record.get("CAUSE", "N"),
            record.get("PRESCRIBED", "N"),
            record.get("MAP_SOURCE", "unit_test"),
            record.get("SOURCE_KEY", ""),
            record.get("MAP_METHOD", "digitized"),
            record.get("WATER_REM", "N"),
            record.get("UNBURN_REM", "N"),
            record.get("MORE_INFO", ""),
            record.get("POLY_DATE", date(2016, 5, 14)),
            record.get("CFS_REF_ID", "BC-2016-N10037"),
            record.get("CFS_NOTE1", ""),
            record.get("CFS_NOTE2", ""),
            record.get("AG_SRCFILE", "unit_test.shp"),
            record.get("ACQ_DATE", date(2025, 6, 30)),
        )

    writer.close()
    base_path.with_suffix(".prj").write_text(PRJ_TEXT, encoding="utf-8")


def make_nfdb_zip(tmp_path: Path, shapefile_records: dict[str, list[dict]]) -> Path:
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    for stem, records in shapefile_records.items():
        write_nfdb_polygon_shapefile(source_dir / stem, records=records)

    archive_path = tmp_path / "NFDB_poly.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        for file_path in sorted(source_dir.iterdir()):
            archive.write(file_path, arcname=file_path.name)

    return archive_path


def test_standardize_wildfire_perimeter_package_reads_two_shapefiles_filters_province_only(
    tmp_path: Path,
):
    archive_path = make_nfdb_zip(
        tmp_path,
        {
            "NFDB_poly_1972to2020_20250630": [
                {
                    "SRC_AGENCY": "BC",
                    "YEAR": 2016,
                    "FIRE_ID": "2016-N10037",
                    "CFS_REF_ID": "BC-2016-N10037",
                    "SIZE_HA": 3.5,
                    "CALC_HA": 3.6,
                },
                {
                    "SRC_AGENCY": "ON",
                    "YEAR": 2016,
                    "FIRE_ID": "2016-ON001",
                    "CFS_REF_ID": "ON-2016-ON001",
                },
                {
                    "SRC_AGENCY": "AB",
                    "YEAR": 1985,
                    "FIRE_ID": "1985-AB001",
                    "CFS_REF_ID": "AB-1985-AB001",
                },
            ],
            "NFDB_poly_2021to2024_20250630": [
                {
                    "SRC_AGENCY": "AB",
                    "YEAR": 2023,
                    "FIRE_ID": "GWF018",
                    "CFS_REF_ID": "AB-2023-GWF018",
                    "SIZE_HA": 49362.8,
                    "CALC_HA": 49380.882141,
                }
            ],
        },
    )

    dataframe = standardize_wildfire_perimeter_package(archive_path)

    assert len(dataframe) == 3
    assert set(dataframe["province"]) == {"BC", "AB"}
    assert set(dataframe["fire_year"]) == {1985, 2016, 2023}
    assert set(dataframe["cfs_ref_id"]) == {
        "BC-2016-N10037",
        "AB-1985-AB001",
        "AB-2023-GWF018",
    }
    assert dataframe["wildfire_perimeter_key"].is_unique
    assert all(dataframe["wildfire_perimeter_key"].str.startswith("nfdb_poly__"))
    assert set(dataframe["geometry_type"]) == {"Polygon"}
    assert dataframe["geometry_is_valid"].all()
    assert set(dataframe["source_crs"]) == {"NAD_1983_Lambert_Conformal_Conic"}


def test_standardize_wildfire_perimeter_package_preserves_duplicate_cfs_ref_id_with_lineage_key(
    tmp_path: Path,
):
    archive_path = make_nfdb_zip(
        tmp_path,
        {
            "NFDB_poly_1972to2020_20250630": [
                {
                    "SRC_AGENCY": "BC",
                    "YEAR": 2016,
                    "FIRE_ID": "2016-N10037",
                    "CFS_REF_ID": "BC-2016-N10037",
                },
                {
                    "SRC_AGENCY": "BC",
                    "YEAR": 2017,
                    "FIRE_ID": "2017-N10037",
                    "CFS_REF_ID": "BC-2016-N10037",
                },
            ]
        },
    )

    dataframe = standardize_wildfire_perimeter_package(archive_path)

    assert len(dataframe) == 2
    assert dataframe["cfs_ref_id"].nunique() == 1
    assert dataframe["wildfire_perimeter_key"].nunique() == 2
    assert dataframe["wildfire_perimeter_key"].is_unique
