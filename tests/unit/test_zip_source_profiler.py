import zipfile

from src.profiling.source_profiler import profile_raw_file


def test_profile_zip_file_detects_shapefile_members(tmp_path):
    zip_path = tmp_path / "boundaries.zip"

    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("province/lpr_000b21a_e.shp", b"fake shp")
        archive.writestr("province/lpr_000b21a_e.dbf", b"fake dbf")
        archive.writestr("province/lpr_000b21a_e.prj", b"fake prj")
        archive.writestr("csd/lcsd000b21a_e.shp", b"fake shp")
        archive.writestr("csd/lcsd000b21a_e.dbf", b"fake dbf")
        archive.writestr("csd/lcsd000b21a_e.prj", b"fake prj")

    profile = profile_raw_file(zip_path)

    assert profile["file_type"] == "zip_archive"
    assert profile["column_count"] == 1
    assert profile["columns"] == ["geometry"]
    assert profile["shapefile_count"] == 2
    assert ".shp" in profile["extension_counts"]
    assert ".prj" in profile["extension_counts"]


def test_profile_zip_file_detects_nested_shapefile_archives(tmp_path):
    inner_zip_path = tmp_path / "inner_boundary.zip"

    with zipfile.ZipFile(inner_zip_path, "w") as inner:
        inner.writestr("boundary/province.shp", b"fake shp")
        inner.writestr("boundary/province.dbf", b"fake dbf")
        inner.writestr("boundary/province.shx", b"fake shx")
        inner.writestr("boundary/province.prj", b"fake prj")

    outer_zip_path = tmp_path / "combined_package.zip"

    with zipfile.ZipFile(outer_zip_path, "w") as outer:
        outer.write(inner_zip_path, arcname="province_cartographic_2021/inner_boundary.zip")

    profile = profile_raw_file(outer_zip_path)

    assert profile["file_type"] == "zip_archive"
    assert profile["nested_archive_count"] == 1
    assert profile["shapefile_count"] == 1
    assert profile["columns"] == ["geometry"]
    assert profile["column_count"] == 1
    assert ".shp" in profile["extension_counts"]
    assert ".prj" in profile["extension_counts"]


def test_profile_zip_file_detects_sqlite_database(tmp_path):
    zip_path = tmp_path / "hydat.zip"

    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("Hydat.sqlite3", b"fake sqlite content")

    profile = profile_raw_file(zip_path)

    assert profile["file_type"] == "zip_archive"
    assert "sqlite_database" in profile["columns"]
    assert profile["column_count"] == 1
    assert ".sqlite3" in profile["extension_counts"]
