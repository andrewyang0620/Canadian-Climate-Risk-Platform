import sqlite3
import zipfile

from src.profiling.archive_internal_profiler import (
    check_hydat_contracts,
    profile_sqlite_archive,
)


def test_profile_sqlite_archive_detects_tables_and_columns(tmp_path):
    sqlite_path = tmp_path / "Hydat.sqlite3"

    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute("CREATE TABLE STATIONS (STATION_NUMBER TEXT PRIMARY KEY, STATION_NAME TEXT)")
        conn.execute("CREATE TABLE DLY_FLOWS (STATION_NUMBER TEXT, YEAR INTEGER, FLOW REAL)")
        conn.execute("INSERT INTO STATIONS VALUES ('08NM116', 'TEST STATION')")
        conn.execute("INSERT INTO DLY_FLOWS VALUES ('08NM116', 2024, 12.5)")
        conn.commit()
    finally:
        conn.close()

    zip_path = tmp_path / "hydat.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(sqlite_path, arcname="Hydat.sqlite3")

    profile = profile_sqlite_archive(raw_path=zip_path, sample_rows=2)

    assert profile["archive_type"] == "sqlite_zip"
    assert "STATIONS" in profile["table_names"]
    assert "DLY_FLOWS" in profile["table_names"]

    stations = next(table for table in profile["tables"] if table["table_name"] == "STATIONS")
    assert stations["row_count"] == 1
    assert stations["columns"][0]["name"] == "STATION_NUMBER"


def test_check_hydat_contracts_detects_candidate_tables():
    source_config = {
        "measurement_contract": {"candidate_tables": ["STATIONS", "DLY_FLOWS", "DLY_LEVELS"]}
    }

    internal_profile = {
        "table_names": ["STATIONS", "DLY_FLOWS"],
    }

    checks = check_hydat_contracts(
        source_config=source_config,
        internal_profile=internal_profile,
    )

    assert checks["candidate_tables"]["STATIONS"]["passed"] is True
    assert checks["candidate_tables"]["DLY_FLOWS"]["passed"] is True
    assert checks["candidate_tables"]["DLY_LEVELS"]["passed"] is False
    assert checks["stations_table_detected"]["passed"] is True
    assert checks["daily_flow_or_level_detected"]["passed"] is True
