from pathlib import Path

import pytest

from src.warehouse.run_snowflake_core_load import (
    CoreLoadError,
    fq_name,
    load_manifest,
    selected_tables,
    validate_identifier,
    validate_stage_path,
)


MANIFEST_PATH = Path(
    "configs/warehouse/snowflake_core_load_manifest.json"
)


EXPECTED_CORE_TABLES = {
    "GRID_CELL",
    "GRID_MUNICIPALITY_BRIDGE",
    "GRID_MONTH_RISK_FEATURE",
    "GRID_MONTH_RISK_SCORE",
    "DISASTER_EVENT_REFERENCE",
    "DISASTER_EVENT_GRID_SCOPE",
    "GRID_MONTH_DISASTER_EVENT_LABEL",
    "VANCOUVER_PARCEL_RISK_CONTEXT",
    "VANCOUVER_BUILDING_PERMIT_CONTEXT",
    "VANCOUVER_LAND_COORDINATE_ASSESSMENT",
    "CALGARY_PROPERTY_RISK_CONTEXT",
    "CALGARY_BUILDING_PERMIT_CONTEXT",
    "CALGARY_DEVELOPMENT_PERMIT_CONTEXT",
}


def test_core_manifest_contract() -> None:
    manifest = load_manifest(MANIFEST_PATH)

    assert manifest["manifest_version"] == 1

    target = manifest["target"]

    assert target["database"] == "CLIMATE_RISK"
    assert target["schema"] == "CORE"
    assert target["stage"] == "ADLS_GOLD_STAGE"
    assert target["file_format"] == "PARQUET_FORMAT"

    tables = manifest["tables"]

    assert len(tables) == 13

    actual_tables = {
        entry["table"]
        for entry in tables
    }

    assert actual_tables == EXPECTED_CORE_TABLES


def test_core_manifest_entries_have_frozen_snapshots() -> None:
    manifest = load_manifest(MANIFEST_PATH)

    for entry in manifest["tables"]:
        stage_path = entry["stage_path"]

        assert "extract_date=" in stage_path
        assert "run_id=" in stage_path
        assert stage_path.endswith("/")

        assert entry["expected_rows"] > 0
        assert entry["expected_columns"] > 0
        assert entry["primary_key"]


def test_selected_tables_preserves_requested_order() -> None:
    manifest = load_manifest(MANIFEST_PATH)

    selected = selected_tables(
        manifest,
        [
            "GRID_MONTH_RISK_SCORE",
            "GRID_CELL",
        ],
    )

    assert [
        entry["table"]
        for entry in selected
    ] == [
        "GRID_MONTH_RISK_SCORE",
        "GRID_CELL",
    ]


def test_selected_tables_rejects_unknown_table() -> None:
    manifest = load_manifest(MANIFEST_PATH)

    with pytest.raises(CoreLoadError):
        selected_tables(
            manifest,
            ["NOT_A_REAL_TABLE"],
        )


def test_validate_stage_path() -> None:
    path = (
        "gold_grid_cell/"
        "extract_date=2026-06-17/"
        "run_id=b947ed24-47da-4134-94b5-0b4d1a818c32/"
    )

    assert validate_stage_path(path) == path


@pytest.mark.parametrize(
    "bad_path",
    [
        "../gold_grid_cell/",
        "gold_grid_cell/../../other/",
        "",
        "gold grid cell/",
    ],
)
def test_validate_stage_path_rejects_invalid_paths(
    bad_path: str,
) -> None:
    with pytest.raises(CoreLoadError):
        validate_stage_path(bad_path)


@pytest.mark.parametrize(
    "identifier",
    [
        "GRID_CELL",
        "CORE",
        "ADLS_GOLD_STAGE",
        "GRID_MONTH_RISK_SCORE",
    ],
)
def test_validate_identifier_accepts_valid_names(
    identifier: str,
) -> None:
    assert validate_identifier(
        identifier,
        "test",
    ) == identifier


@pytest.mark.parametrize(
    "identifier",
    [
        "__LOAD_TABLE",
        "grid_cell",
        "GRID-CELL",
        "1GRID_CELL",
        "GRID CELL",
    ],
)
def test_validate_identifier_rejects_invalid_names(
    identifier: str,
) -> None:
    with pytest.raises(CoreLoadError):
        validate_identifier(
            identifier,
            "test",
        )


def test_fully_qualified_name() -> None:
    assert fq_name(
        "CLIMATE_RISK",
        "CORE",
        "GRID_CELL",
    ) == '"CLIMATE_RISK"."CORE"."GRID_CELL"'