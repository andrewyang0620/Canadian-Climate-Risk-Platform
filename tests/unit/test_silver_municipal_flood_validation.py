import pandas as pd

from src.silver.validation import validate_municipal_flood_hazard_silver_outputs


def write_flood_table(root, dataframe):
    path = root / "silver_flood_hazard_zone" / "extract_date=2026-05-28" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / "silver_flood_hazard_zone.parquet", index=False)


def base_row(key, city, source_name, geometry_type="Polygon"):
    return {
        "flood_hazard_zone_key": key,
        "city": city,
        "source_zone_id": "100",
        "hazard_class": "Flood Fringe",
        "geometry_type": geometry_type,
        "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 0))",
        "source_feature_index": 1,
        "source_name": source_name,
        "source_properties_json": "{}",
        "source_record_count": 1,
    }


def test_validate_municipal_flood_hazard_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row(
                "calgary_1",
                "calgary",
                "calgary_flood_hazard",
                "MultiPolygon",
            ),
            base_row(
                "vancouver_1",
                "vancouver",
                "vancouver_floodplain",
                "Polygon",
            ),
        ]
    )

    write_flood_table(silver_root, dataframe)

    report = validate_municipal_flood_hazard_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 9


def test_validate_municipal_flood_hazard_silver_outputs_fails_duplicate_key(tmp_path):
    silver_root = tmp_path / "silver"

    row = base_row("calgary_1", "calgary", "calgary_flood_hazard")
    dataframe = pd.DataFrame([row, row])

    write_flood_table(silver_root, dataframe)

    report = validate_municipal_flood_hazard_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "flood_hazard_key_not_null_and_unique" in failed_checks


def test_validate_municipal_flood_hazard_silver_outputs_fails_unexpected_geometry_type(tmp_path):
    silver_root = tmp_path / "silver"

    dataframe = pd.DataFrame(
        [
            base_row(
                "calgary_1",
                "calgary",
                "calgary_flood_hazard",
                geometry_type="LineString",
            ),
            base_row(
                "vancouver_1",
                "vancouver",
                "vancouver_floodplain",
                geometry_type="Polygon",
            ),
        ]
    )

    write_flood_table(silver_root, dataframe)

    report = validate_municipal_flood_hazard_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "flood_hazard_geometry_types_are_polygonal" in failed_checks
