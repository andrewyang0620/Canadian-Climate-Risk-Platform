import pandas as pd
import pytest
from pyproj import Transformer
from shapely.geometry import box
from shapely.ops import transform

from src.gold.city.city_property_risk_context import (
    CityPropertyRiskContextError,
    build_gold_calgary_property_risk_context,
    build_gold_vancouver_parcel_risk_context,
)


ANALYSIS_TO_WGS84 = Transformer.from_crs(
    "EPSG:3347",
    "EPSG:4326",
    always_xy=True,
)


def _to_wgs84_wkt(geometry):
    return transform(
        ANALYSIS_TO_WGS84.transform,
        geometry,
    ).wkt


def _grid(
    key: str,
    grid_system: str,
    geometry,
) -> dict:
    return {
        "grid_cell_key": key,
        "grid_system": grid_system,
        "cell_size_m": 10_000,
        "full_cell_geometry_wkt": geometry.wkt,
        "crs_epsg": 3347,
    }


def _vancouver_flood(
    key: str,
    geometry,
    *,
    exposed: bool = False,
) -> dict:
    return {
        "property_parcel_key": key,
        "geometry_wkt": _to_wgs84_wkt(
            geometry
        ),
        "address_text": f"{key} ADDRESS",
        "is_flood_exposed": exposed,
        "flood_scenario_count": (
            1 if exposed else 0
        ),
    }


def _vancouver_assessment(
    key: str,
    *,
    has_assessment: bool = True,
    value: float = 500_000.0,
) -> dict:
    return {
        "property_parcel_key": key,
        "has_latest_assessment": (
            has_assessment
        ),
        "latest_report_year": (
            2026
            if has_assessment
            else None
        ),
        "latest_total_assessed_value": (
            value
            if has_assessment
            else None
        ),
        "assessment_mapping_status": (
            "exact_1_to_1"
            if has_assessment
            else "no_assessment"
        ),
    }


def _calgary_assessment(
    key: str,
    geometry,
    *,
    value: float = 600_000.0,
) -> dict:
    return {
        "source_parcel_id": key,
        "assessment_year": 2026,
        "assessment_record_count": 1,
        "assessed_value_total_sum": value,
        "community_name": "TEST COMMUNITY",
        "property_type": "Residential",
        "geometry_wkt": _to_wgs84_wkt(
            geometry
        ),
    }


def _calgary_flood(
    key: str,
    *,
    exposed: bool = False,
) -> dict:
    return {
        "source_parcel_id": key,
        "geometry_wkt": "IGNORED_DUPLICATE_GEOMETRY",
        "is_flood_exposed": exposed,
        "intersects_regulatory_flood_layer": (
            exposed
        ),
        "intersects_normal_river_channel": False,
        "flood_zone_membership_count": (
            1 if exposed else 0
        ),
    }


def test_vancouver_merges_serving_context_and_assigns_grid():
    grid_geometry = box(
        1_200_000,
        450_000,
        1_210_000,
        460_000,
    )

    parcel_geometry = box(
        1_202_000,
        452_000,
        1_203_000,
        453_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "bc_grid_1",
                "bc_10km",
                grid_geometry,
            )
        ]
    )

    flood = pd.DataFrame(
        [
            _vancouver_flood(
                "parcel_1",
                parcel_geometry,
                exposed=True,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _vancouver_assessment(
                "parcel_1",
                value=750_000.0,
            )
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert (
        row["property_parcel_key"]
        == "parcel_1"
    )

    assert bool(
        row["is_flood_exposed"]
    )

    assert (
        row["latest_total_assessed_value"]
        == 750_000.0
    )

    assert (
        row["national_grid_cell_key"]
        == "bc_grid_1"
    )

    assert (
        row["national_grid_candidate_count"]
        == 1
    )

    assert (
        row["national_grid_overlap_ratio"]
        == pytest.approx(1.0)
    )

    assert bool(
        row["has_national_grid_assignment"]
    )

    assert (
        row["national_risk_resolution"]
        == "10km"
    )

    assert (
        row["spatial_assignment_method"]
        == "max_area_overlap"
    )

    assert (
        row[
            "national_grid_assignment_geometry"
        ]
        == "full_cell_geometry_wkt"
    )

    assert (
        summary[
            "national_grid_assigned_count"
        ]
        == 1
    )

    assert (
        summary[
            "national_grid_unassigned_count"
        ]
        == 0
    )


def test_calgary_merges_serving_context_and_assigns_grid():
    grid_geometry = box(
        1_500_000,
        600_000,
        1_510_000,
        610_000,
    )

    property_geometry = box(
        1_502_000,
        602_000,
        1_504_000,
        604_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "ab_grid_1",
                "ab_10km",
                grid_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _calgary_assessment(
                "property_1",
                property_geometry,
                value=900_000.0,
            )
        ]
    )

    flood = pd.DataFrame(
        [
            _calgary_flood(
                "property_1",
                exposed=True,
            )
        ]
    )

    result, summary = (
        build_gold_calgary_property_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    assert len(result) == 1

    row = result.iloc[0]

    assert (
        row["source_parcel_id"]
        == "property_1"
    )

    assert (
        row["assessed_value_total_sum"]
        == 900_000.0
    )

    assert bool(
        row["is_flood_exposed"]
    )

    assert (
        row["national_grid_cell_key"]
        == "ab_grid_1"
    )

    assert (
        row["national_grid_candidate_count"]
        == 1
    )

    assert (
        row["national_grid_overlap_ratio"]
        == pytest.approx(1.0)
    )

    assert (
        summary[
            "national_grid_assigned_count"
        ]
        == 1
    )

    assert (
        summary[
            "flood_exposed_count"
        ]
        == 1
    )


def test_multi_grid_entity_uses_max_area_overlap():
    left_grid = box(
        1_200_000,
        450_000,
        1_210_000,
        460_000,
    )

    right_grid = box(
        1_210_000,
        450_000,
        1_220_000,
        460_000,
    )

    # 25% in left grid, 75% in right grid.
    parcel_geometry = box(
        1_208_000,
        452_000,
        1_216_000,
        454_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "bc_grid_left",
                "bc_10km",
                left_grid,
            ),
            _grid(
                "bc_grid_right",
                "bc_10km",
                right_grid,
            ),
        ]
    )

    flood = pd.DataFrame(
        [
            _vancouver_flood(
                "parcel_1",
                parcel_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _vancouver_assessment(
                "parcel_1"
            )
        ]
    )

    result, summary = (
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    row = result.iloc[0]

    assert (
        row["national_grid_candidate_count"]
        == 2
    )

    assert (
        row["national_grid_cell_key"]
        == "bc_grid_right"
    )

    assert (
        row["national_grid_overlap_ratio"]
        == pytest.approx(0.75)
    )

    assert (
        row[
            "national_grid_overlap_area_sq_m"
        ]
        == pytest.approx(
            6_000 * 2_000
        )
    )

    assert (
        summary[
            "multi_grid_candidate_count"
        ]
        == 1
    )


def test_full_cell_geometry_is_used_for_assignment():
    full_cell = box(
        1_200_000,
        450_000,
        1_210_000,
        460_000,
    )

    parcel_geometry = box(
        1_208_000,
        458_000,
        1_209_000,
        459_000,
    )

    grids = pd.DataFrame(
        [
            {
                **_grid(
                    "bc_grid_1",
                    "bc_10km",
                    full_cell,
                ),
                # Deliberately does not contain
                # the parcel.
                "analysis_geometry_wkt": box(
                    1_200_000,
                    450_000,
                    1_202_000,
                    452_000,
                ).wkt,
            }
        ]
    )

    flood = pd.DataFrame(
        [
            _vancouver_flood(
                "parcel_1",
                parcel_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _vancouver_assessment(
                "parcel_1"
            )
        ]
    )

    result, _ = (
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    row = result.iloc[0]

    assert (
        row["national_grid_cell_key"]
        == "bc_grid_1"
    )

    assert (
        row["national_grid_overlap_ratio"]
        == pytest.approx(1.0)
    )

    assert (
        row[
            "national_grid_assignment_geometry"
        ]
        == "full_cell_geometry_wkt"
    )


def test_phase_d_serving_fields_are_preserved():
    grid_geometry = box(
        1_200_000,
        450_000,
        1_210_000,
        460_000,
    )

    parcel_geometry = box(
        1_202_000,
        452_000,
        1_203_000,
        453_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "bc_grid_1",
                "bc_10km",
                grid_geometry,
            )
        ]
    )

    flood = pd.DataFrame(
        [
            {
                **_vancouver_flood(
                    "parcel_1",
                    parcel_geometry,
                    exposed=True,
                ),
                "custom_flood_metric": 123.45,
            }
        ]
    )

    assessment = pd.DataFrame(
        [
            {
                **_vancouver_assessment(
                    "parcel_1"
                ),
                "custom_assessment_metric": 678.90,
            }
        ]
    )

    result, _ = (
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    assert (
        "custom_flood_metric"
        in result.columns
    )

    assert (
        "custom_assessment_metric"
        in result.columns
    )

    assert (
        result.iloc[0][
            "custom_flood_metric"
        ]
        == 123.45
    )

    assert (
        result.iloc[0][
            "custom_assessment_metric"
        ]
        == 678.90
    )


def test_primary_table_wins_for_duplicate_columns():
    grid_geometry = box(
        1_500_000,
        600_000,
        1_510_000,
        610_000,
    )

    property_geometry = box(
        1_502_000,
        602_000,
        1_503_000,
        603_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "ab_grid_1",
                "ab_10km",
                grid_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            {
                **_calgary_assessment(
                    "property_1",
                    property_geometry,
                ),
                "shared_field": (
                    "assessment_value"
                ),
            }
        ]
    )

    flood = pd.DataFrame(
        [
            {
                **_calgary_flood(
                    "property_1",
                ),
                "shared_field": (
                    "flood_value"
                ),
            }
        ]
    )

    result, _ = (
        build_gold_calgary_property_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )
    )

    assert (
        result.iloc[0][
            "shared_field"
        ]
        == "assessment_value"
    )


def test_different_entity_universes_fail():
    grid_geometry = box(
        1_200_000,
        450_000,
        1_210_000,
        460_000,
    )

    parcel_geometry = box(
        1_202_000,
        452_000,
        1_203_000,
        453_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "bc_grid_1",
                "bc_10km",
                grid_geometry,
            )
        ]
    )

    flood = pd.DataFrame(
        [
            _vancouver_flood(
                "parcel_1",
                parcel_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _vancouver_assessment(
                "parcel_2"
            )
        ]
    )

    with pytest.raises(
        CityPropertyRiskContextError,
        match="same property_parcel_key universe",
    ):
        build_gold_vancouver_parcel_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )


def test_duplicate_entity_key_fails():
    grid_geometry = box(
        1_500_000,
        600_000,
        1_510_000,
        610_000,
    )

    property_geometry = box(
        1_502_000,
        602_000,
        1_503_000,
        603_000,
    )

    grids = pd.DataFrame(
        [
            _grid(
                "ab_grid_1",
                "ab_10km",
                grid_geometry,
            )
        ]
    )

    assessment = pd.DataFrame(
        [
            _calgary_assessment(
                "property_1",
                property_geometry,
            ),
            _calgary_assessment(
                "property_1",
                property_geometry,
            ),
        ]
    )

    flood = pd.DataFrame(
        [
            _calgary_flood(
                "property_1"
            )
        ]
    )

    with pytest.raises(
        CityPropertyRiskContextError,
        match="duplicate source_parcel_id",
    ):
        build_gold_calgary_property_risk_context(
            assessment_dataframe=assessment,
            flood_dataframe=flood,
            grid_dataframe=grids,
        )