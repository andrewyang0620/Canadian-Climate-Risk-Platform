from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from shapely import wkt

from src.gold.common.io import latest_table_parquet

from src.gold.spatial.grid import (
    ANALYSIS_CRS_EPSG,
    normalize_polygonal_geometry,
)


EXPECTED_GRID_SYSTEMS = {
    "ab_10km",
    "bc_10km",
    "calgary_1km",
    "vancouver_1km",
}

EXPECTED_CELL_SIZES = {
    "ab_10km": 10_000,
    "bc_10km": 10_000,
    "calgary_1km": 1_000,
    "vancouver_1km": 1_000,
}

AREA_TOLERANCE_SQ_KM = 1e-6
COVERAGE_TOLERANCE = 1e-8


class GoldSpatialValidationError(Exception):
    """Raised when Gold spatial validation cannot be executed."""


@dataclass(frozen=True)
class GoldValidationCheck:
    name: str
    passed: bool
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "details": self.details,
        }


@dataclass
class GoldValidationReport:
    validation_name: str
    checks: list[GoldValidationCheck] = field(default_factory=list)
    output_paths: dict[str, str] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def add_check(
        self,
        *,
        name: str,
        passed: bool,
        details: dict[str, Any],
    ) -> None:
        self.checks.append(
            GoldValidationCheck(
                name=name,
                passed=bool(passed),
                details=details,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_name": self.validation_name,
            "passed": self.passed,
            "checks": [check.to_dict() for check in self.checks],
            "output_paths": self.output_paths,
        }


def validate_spatial_grid_foundation_dataframes(
    *,
    grid: pd.DataFrame,
    bridge: pd.DataFrame,
    province: pd.DataFrame,
    municipality: pd.DataFrame,
) -> GoldValidationReport:
    report = GoldValidationReport(validation_name=("gold_spatial_grid_foundation_validation"))

    _require_columns(
        grid,
        {
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "city_name",
            "cell_size_m",
            "grid_x_index",
            "grid_y_index",
            "cell_min_x",
            "cell_min_y",
            "analysis_area_sq_km",
            "full_cell_area_sq_km",
            "boundary_coverage_ratio",
            "is_boundary_edge_cell",
            "full_cell_geometry_wkt",
            "analysis_geometry_wkt",
            "crs_epsg",
        },
        "gold_grid_cell",
    )

    _require_columns(
        bridge,
        {
            "grid_municipality_bridge_key",
            "grid_cell_key",
            "grid_system",
            "province_key",
            "municipality_key",
            "municipality_name",
            "intersection_area_sq_km",
            "grid_coverage_ratio",
            "municipality_coverage_ratio",
            "municipality_geometry_repaired",
            "is_primary_municipality",
        },
        "gold_grid_municipality_bridge",
    )

    _require_columns(
        province,
        {
            "province_key",
            "geometry_wkt",
        },
        "silver_boundary_province",
    )

    _require_columns(
        municipality,
        {
            "municipality_key",
            "municipality_name",
            "province_key",
            "geometry_wkt",
        },
        "silver_boundary_municipality",
    )

    _validate_grid(
        report=report,
        grid=grid,
        province=province,
        municipality=municipality,
    )

    _validate_bridge(
        report=report,
        grid=grid,
        bridge=bridge,
        municipality=municipality,
    )

    return report


def validate_spatial_grid_foundation_outputs(
    *,
    silver_root: str | Path = "lakehouse/silver",
    gold_root: str | Path = "lakehouse/gold",
    output_json_path: str | Path = (
        "lakehouse/gold/_validation/" "spatial_grid_foundation/latest_validation.json"
    ),
) -> GoldValidationReport:
    grid_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_cell",
    )
    bridge_path = latest_table_parquet(
        root=gold_root,
        table_name="gold_grid_municipality_bridge",
    )
    province_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_boundary_province",
    )
    municipality_path = latest_table_parquet(
        root=silver_root,
        table_name="silver_boundary_municipality",
    )

    report = validate_spatial_grid_foundation_dataframes(
        grid=pd.read_parquet(grid_path),
        bridge=pd.read_parquet(bridge_path),
        province=pd.read_parquet(province_path),
        municipality=pd.read_parquet(municipality_path),
    )

    report.output_paths = {
        "gold_grid_cell": grid_path.as_posix(),
        "gold_grid_municipality_bridge": (bridge_path.as_posix()),
        "silver_boundary_province": (province_path.as_posix()),
        "silver_boundary_municipality": (municipality_path.as_posix()),
    }

    final_output_path = Path(output_json_path)
    final_output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    final_output_path.write_text(
        json.dumps(
            report.to_dict(),
            indent=2,
        ),
        encoding="utf-8",
    )

    return report


def _validate_grid(
    *,
    report: GoldValidationReport,
    grid: pd.DataFrame,
    province: pd.DataFrame,
    municipality: pd.DataFrame,
) -> None:
    row_count = len(grid)

    report.add_check(
        name="gold_grid_row_count_gt_zero",
        passed=row_count > 0,
        details={"row_count": row_count},
    )

    actual_grid_systems = set(grid["grid_system"].dropna().unique())

    report.add_check(
        name="gold_grid_systems_are_expected",
        passed=(actual_grid_systems == EXPECTED_GRID_SYSTEMS),
        details={
            "actual": sorted(actual_grid_systems),
            "expected": sorted(EXPECTED_GRID_SYSTEMS),
            "counts": {
                str(key): int(value)
                for key, value in grid["grid_system"].value_counts().to_dict().items()
            },
        },
    )

    key_nulls = int(grid["grid_cell_key"].isna().sum())
    key_duplicates = int(grid["grid_cell_key"].duplicated().sum())

    report.add_check(
        name="gold_grid_key_not_null_and_unique",
        passed=(key_nulls == 0 and key_duplicates == 0),
        details={
            "null_count": key_nulls,
            "duplicate_count": key_duplicates,
        },
    )

    crs_values = sorted(int(value) for value in grid["crs_epsg"].dropna().unique())

    report.add_check(
        name="gold_grid_crs_is_epsg_3347",
        passed=crs_values == [ANALYSIS_CRS_EPSG],
        details={
            "actual": crs_values,
            "expected": [ANALYSIS_CRS_EPSG],
        },
    )

    cell_size_failures: dict[str, int] = {}

    for grid_system, expected_size in EXPECTED_CELL_SIZES.items():
        rows = grid[grid["grid_system"] == grid_system]

        cell_size_failures[grid_system] = int((rows["cell_size_m"] != expected_size).sum())

    report.add_check(
        name="gold_grid_cell_sizes_are_expected",
        passed=all(value == 0 for value in cell_size_failures.values()),
        details={
            "expected": EXPECTED_CELL_SIZES,
            "invalid_counts": cell_size_failures,
        },
    )

    expected_min_x = grid["grid_x_index"] * grid["cell_size_m"]
    expected_min_y = grid["grid_y_index"] * grid["cell_size_m"]

    x_mismatches = int(
        (
            ~np.isclose(
                grid["cell_min_x"].to_numpy(dtype=float),
                expected_min_x.to_numpy(dtype=float),
                atol=1e-9,
            )
        ).sum()
    )
    y_mismatches = int(
        (
            ~np.isclose(
                grid["cell_min_y"].to_numpy(dtype=float),
                expected_min_y.to_numpy(dtype=float),
                atol=1e-9,
            )
        ).sum()
    )

    report.add_check(
        name="gold_grid_index_alignment_valid",
        passed=(x_mismatches == 0 and y_mismatches == 0),
        details={
            "x_mismatch_count": x_mismatches,
            "y_mismatch_count": y_mismatches,
        },
    )

    geometry_nulls = {
        "analysis_geometry_wkt": int(grid["analysis_geometry_wkt"].isna().sum()),
        "full_cell_geometry_wkt": int(grid["full_cell_geometry_wkt"].isna().sum()),
    }

    geometry_ready = all(value == 0 for value in geometry_nulls.values())

    invalid_analysis = row_count
    invalid_full = row_count
    max_analysis_area_difference = None
    max_full_area_difference = None

    if geometry_ready:
        analysis_geometry = shapely.from_wkt(grid["analysis_geometry_wkt"].astype(str).to_numpy())
        full_geometry = shapely.from_wkt(grid["full_cell_geometry_wkt"].astype(str).to_numpy())

        invalid_analysis = int(
            (
                ~np.asarray(
                    shapely.is_valid(analysis_geometry),
                    dtype=bool,
                )
            ).sum()
        )
        invalid_full = int(
            (
                ~np.asarray(
                    shapely.is_valid(full_geometry),
                    dtype=bool,
                )
            ).sum()
        )

        analysis_geometry_area = (
            np.asarray(
                shapely.area(analysis_geometry),
                dtype=float,
            )
            / 1_000_000
        )
        full_geometry_area = (
            np.asarray(
                shapely.area(full_geometry),
                dtype=float,
            )
            / 1_000_000
        )

        max_analysis_area_difference = float(
            np.max(
                np.abs(analysis_geometry_area - grid["analysis_area_sq_km"].to_numpy(dtype=float))
            )
        )
        max_full_area_difference = float(
            np.max(np.abs(full_geometry_area - grid["full_cell_area_sq_km"].to_numpy(dtype=float)))
        )

    report.add_check(
        name=("gold_grid_geometry_valid_and_areas_consistent"),
        passed=(
            geometry_ready
            and invalid_analysis == 0
            and invalid_full == 0
            and max_analysis_area_difference is not None
            and max_analysis_area_difference <= AREA_TOLERANCE_SQ_KM
            and max_full_area_difference is not None
            and max_full_area_difference <= AREA_TOLERANCE_SQ_KM
        ),
        details={
            "geometry_nulls": geometry_nulls,
            "invalid_analysis_geometry_count": (invalid_analysis),
            "invalid_full_geometry_count": (invalid_full),
            "maximum_analysis_area_difference_sq_km": (max_analysis_area_difference),
            "maximum_full_area_difference_sq_km": (max_full_area_difference),
            "tolerance_sq_km": (AREA_TOLERANCE_SQ_KM),
        },
    )

    coverage_nulls = int(grid["boundary_coverage_ratio"].isna().sum())
    coverage_out_of_range = int(
        (
            ~grid["boundary_coverage_ratio"].between(
                0,
                1,
                inclusive="both",
            )
        ).sum()
    )

    report.add_check(
        name="gold_grid_boundary_coverage_valid",
        passed=(coverage_nulls == 0 and coverage_out_of_range == 0),
        details={
            "null_count": coverage_nulls,
            "out_of_range_count": (coverage_out_of_range),
            "minimum": float(grid["boundary_coverage_ratio"].min()),
            "maximum": float(grid["boundary_coverage_ratio"].max()),
        },
    )

    source_geometry_by_system = {
        "ab_10km": _select_boundary_geometry(
            province,
            province_key="AB",
        ),
        "bc_10km": _select_boundary_geometry(
            province,
            province_key="BC",
        ),
        "calgary_1km": _select_boundary_geometry(
            municipality,
            municipality_name="Calgary",
            province_key="AB",
        ),
        "vancouver_1km": (
            _select_boundary_geometry(
                municipality,
                municipality_name="Vancouver",
                province_key="BC",
            )
        ),
    }

    area_differences: dict[str, float] = {}

    for grid_system, source_geometry in source_geometry_by_system.items():
        source_area_sq_km = source_geometry.area / 1_000_000
        grid_area_sq_km = float(
            grid.loc[
                grid["grid_system"] == grid_system,
                "analysis_area_sq_km",
            ].sum()
        )

        area_differences[grid_system] = float(grid_area_sq_km - source_area_sq_km)

    report.add_check(
        name="gold_grid_boundary_area_conserved",
        passed=all(abs(value) <= AREA_TOLERANCE_SQ_KM for value in area_differences.values()),
        details={
            "area_differences_sq_km": (area_differences),
            "tolerance_sq_km": (AREA_TOLERANCE_SQ_KM),
        },
    )


def _validate_bridge(
    *,
    report: GoldValidationReport,
    grid: pd.DataFrame,
    bridge: pd.DataFrame,
    municipality: pd.DataFrame,
) -> None:
    report.add_check(
        name="gold_grid_municipality_bridge_row_count_gt_zero",
        passed=len(bridge) > 0,
        details={"row_count": len(bridge)},
    )

    key_nulls = int(bridge["grid_municipality_bridge_key"].isna().sum())
    key_duplicates = int(bridge["grid_municipality_bridge_key"].duplicated().sum())

    report.add_check(
        name="gold_grid_municipality_bridge_key_valid",
        passed=(key_nulls == 0 and key_duplicates == 0),
        details={
            "null_count": key_nulls,
            "duplicate_count": key_duplicates,
        },
    )

    ratio_details: dict[str, dict[str, int]] = {}
    ratio_passed = True

    for column in [
        "grid_coverage_ratio",
        "municipality_coverage_ratio",
    ]:
        null_count = int(bridge[column].isna().sum())
        out_of_range_count = int(
            (
                ~bridge[column].between(
                    0,
                    1,
                    inclusive="both",
                )
            ).sum()
        )

        ratio_details[column] = {
            "null_count": null_count,
            "out_of_range_count": (out_of_range_count),
        }

        if null_count > 0 or out_of_range_count > 0:
            ratio_passed = False

    report.add_check(
        name="gold_grid_municipality_bridge_ratios_valid",
        passed=ratio_passed,
        details=ratio_details,
    )

    grid_keys = set(grid["grid_cell_key"].astype(str))
    bridge_grid_keys = set(bridge["grid_cell_key"].astype(str))

    unmatched_grid_keys = sorted(grid_keys - bridge_grid_keys)
    unknown_grid_keys = sorted(bridge_grid_keys - grid_keys)

    report.add_check(
        name="gold_grid_municipality_bridge_all_grids_matched",
        passed=(len(unmatched_grid_keys) == 0 and len(unknown_grid_keys) == 0),
        details={
            "grid_count": len(grid_keys),
            "matched_grid_count": len(grid_keys & bridge_grid_keys),
            "unmatched_grid_count": len(unmatched_grid_keys),
            "unknown_grid_count": len(unknown_grid_keys),
            "unmatched_sample": (unmatched_grid_keys[:20]),
            "unknown_sample": (unknown_grid_keys[:20]),
        },
    )

    primary_counts = bridge.groupby("grid_cell_key")["is_primary_municipality"].sum()

    invalid_primary_count = int((primary_counts != 1).sum())

    primary_not_maximum = 0

    for _, group in bridge.groupby("grid_cell_key"):
        primary_rows = group[group["is_primary_municipality"]]

        if len(primary_rows) != 1:
            continue

        primary_coverage = float(primary_rows.iloc[0]["grid_coverage_ratio"])
        maximum_coverage = float(group["grid_coverage_ratio"].max())

        if abs(primary_coverage - maximum_coverage) > COVERAGE_TOLERANCE:
            primary_not_maximum += 1

    report.add_check(
        name="gold_grid_primary_municipality_valid",
        passed=(invalid_primary_count == 0 and primary_not_maximum == 0),
        details={
            "invalid_primary_count": (invalid_primary_count),
            "primary_not_maximum_coverage_count": (primary_not_maximum),
        },
    )

    intersection_area = (
        bridge.groupby("grid_cell_key")["intersection_area_sq_km"]
        .sum()
        .rename("intersection_area_sq_km")
    )
    coverage_sum = (
        bridge.groupby("grid_cell_key")["grid_coverage_ratio"].sum().rename("coverage_sum")
    )

    area_audit = (
        grid[
            [
                "grid_cell_key",
                "analysis_area_sq_km",
            ]
        ]
        .merge(
            intersection_area,
            left_on="grid_cell_key",
            right_index=True,
            how="left",
            validate="one_to_one",
        )
        .merge(
            coverage_sum,
            left_on="grid_cell_key",
            right_index=True,
            how="left",
            validate="one_to_one",
        )
    )

    area_difference = (
        area_audit["intersection_area_sq_km"] - area_audit["analysis_area_sq_km"]
    ).abs()

    coverage_difference = (area_audit["coverage_sum"] - 1.0).abs()

    area_mismatch_count = int((area_difference > AREA_TOLERANCE_SQ_KM).sum())
    coverage_mismatch_count = int((coverage_difference > COVERAGE_TOLERANCE).sum())

    report.add_check(
        name=("gold_grid_municipality_bridge_area_conserved"),
        passed=(area_mismatch_count == 0 and coverage_mismatch_count == 0),
        details={
            "area_mismatch_count": (area_mismatch_count),
            "coverage_mismatch_count": (coverage_mismatch_count),
            "maximum_area_difference_sq_km": float(area_difference.max()),
            "maximum_coverage_difference": float(coverage_difference.max()),
            "area_tolerance_sq_km": (AREA_TOLERANCE_SQ_KM),
            "coverage_tolerance": (COVERAGE_TOLERANCE),
        },
    )

    expected_city_scope = {
        "calgary_1km": {
            "municipality_name": "Calgary",
            "province_key": "AB",
        },
        "vancouver_1km": {
            "municipality_name": "Vancouver",
            "province_key": "BC",
        },
    }

    city_scope_details: dict[
        str,
        dict[str, Any],
    ] = {}
    city_scope_passed = True

    for grid_system, expected in expected_city_scope.items():
        rows = bridge[bridge["grid_system"] == grid_system]

        names = sorted(rows["municipality_name"].dropna().unique().tolist())
        province_keys = sorted(rows["province_key"].dropna().unique().tolist())
        maximum_match_count = int(rows.groupby("grid_cell_key").size().max())

        expected_names = [expected["municipality_name"]]
        expected_provinces = [expected["province_key"]]

        passed = (
            names == expected_names
            and province_keys == expected_provinces
            and maximum_match_count == 1
        )

        city_scope_details[grid_system] = {
            "municipality_names": names,
            "province_keys": province_keys,
            "maximum_match_count": (maximum_match_count),
            "passed": passed,
        }

        if not passed:
            city_scope_passed = False

    report.add_check(
        name="gold_city_grid_municipality_scope_valid",
        passed=city_scope_passed,
        details=city_scope_details,
    )

    source_geometry = shapely.from_wkt(municipality["geometry_wkt"].astype(str).to_numpy())

    invalid_mask = ~np.asarray(
        shapely.is_valid(source_geometry),
        dtype=bool,
    )

    expected_repaired_keys = set(
        municipality.loc[
            invalid_mask,
            "municipality_key",
        ].astype(str)
    )

    actual_repaired_keys = set(
        bridge.loc[
            bridge["municipality_geometry_repaired"],
            "municipality_key",
        ].astype(str)
    )

    report.add_check(
        name=("gold_municipality_geometry_repair_tracking_consistent"),
        passed=(actual_repaired_keys == expected_repaired_keys),
        details={
            "expected_repaired_count": len(expected_repaired_keys),
            "actual_repaired_count": len(actual_repaired_keys),
            "missing_repair_keys": sorted(expected_repaired_keys - actual_repaired_keys),
            "unexpected_repair_keys": sorted(actual_repaired_keys - expected_repaired_keys),
        },
    )


def _select_boundary_geometry(
    dataframe: pd.DataFrame,
    *,
    province_key: str,
    municipality_name: str | None = None,
):
    mask = dataframe["province_key"] == province_key

    if municipality_name is not None:
        mask &= dataframe["municipality_name"] == municipality_name

    matches = dataframe.loc[
        mask,
        "geometry_wkt",
    ]

    if len(matches) != 1:
        raise GoldSpatialValidationError(
            "Expected exactly one source boundary for "
            f"province_key={province_key}, "
            f"municipality_name={municipality_name}; "
            f"found {len(matches)}."
        )

    geometry, _ = normalize_polygonal_geometry(matches.iloc[0])

    return geometry


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldSpatialValidationError(
            f"{table_name} is missing columns: " f"{sorted(missing_columns)}"
        )
