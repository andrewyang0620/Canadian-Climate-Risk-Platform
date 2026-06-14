import pandas as pd

from src.silver.validation import validate_census_boundary_silver_outputs


def write_table(root, table_name, dataframe):
    path = root / table_name / "extract_date=2026-05-20" / "run_id=test"
    path.mkdir(parents=True)
    dataframe.to_parquet(path / f"{table_name}.parquet", index=False)


def test_validate_census_boundary_silver_outputs_passes(tmp_path):
    silver_root = tmp_path / "silver"

    province_df = pd.DataFrame(
        [
            {
                "province_key": "AB",
                "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 0))",
                "crs": "EPSG:3347",
            },
            {
                "province_key": "BC",
                "geometry_wkt": "POLYGON ((0 0, 2 0, 2 2, 0 0))",
                "crs": "EPSG:3347",
            },
        ]
    )

    municipality_df = pd.DataFrame(
        [
            {
                "municipality_key": "4801003",
                "province_key": "AB",
                "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 0))",
                "crs": "EPSG:3347",
            },
            {
                "municipality_key": "5901001",
                "province_key": "BC",
                "geometry_wkt": "POLYGON ((0 0, 2 0, 2 2, 0 0))",
                "crs": "EPSG:3347",
            },
        ]
    )

    write_table(silver_root, "silver_boundary_province", province_df)
    write_table(silver_root, "silver_boundary_municipality", municipality_df)

    report = validate_census_boundary_silver_outputs(silver_root=silver_root)

    assert report.passed is True
    assert len(report.checks) == 10


def test_validate_census_boundary_silver_outputs_fails_missing_bc(tmp_path):
    silver_root = tmp_path / "silver"

    province_df = pd.DataFrame(
        [
            {
                "province_key": "AB",
                "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 0))",
                "crs": "EPSG:3347",
            },
            {
                "province_key": "SK",
                "geometry_wkt": "POLYGON ((0 0, 2 0, 2 2, 0 0))",
                "crs": "EPSG:3347",
            },
        ]
    )

    municipality_df = pd.DataFrame(
        [
            {
                "municipality_key": "4801003",
                "province_key": "AB",
                "geometry_wkt": "POLYGON ((0 0, 1 0, 1 1, 0 0))",
                "crs": "EPSG:3347",
            }
        ]
    )

    write_table(silver_root, "silver_boundary_province", province_df)
    write_table(silver_root, "silver_boundary_municipality", municipality_df)

    report = validate_census_boundary_silver_outputs(silver_root=silver_root)

    assert report.passed is False
    failed_checks = [check.name for check in report.checks if not check.passed]
    assert "province_keys_are_ab_bc" in failed_checks
    assert "municipality_province_keys_are_ab_bc" in failed_checks
