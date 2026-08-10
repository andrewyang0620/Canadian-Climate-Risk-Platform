import pandas as pd
import pytest

from src.scoring.builder import (
    RiskScoreBuildError,
    build_gold_grid_month_risk_score,
)


def _config() -> dict:
    return {
        "domain_weights": {
            "climate": 0.35,
            "hydro": 0.35,
            "wildfire": 0.30,
        },
        "climate": {
            "normalization": (
                "province_calendar_month_"
                "zero_preserving_positive_percentile"
            ),
            "signals": {
                "extreme_heat_days": {
                    "field": "climate_extreme_heat_days",
                    "weight": 0.30,
                },
                "heavy_precipitation_days": {
                    "field": (
                        "climate_heavy_precipitation_days"
                    ),
                    "weight": 0.25,
                },
                "freeze_thaw_days": {
                    "field": "climate_freeze_thaw_days",
                    "weight": 0.15,
                },
                "extreme_cold_days": {
                    "field": "climate_extreme_cold_days",
                    "weight": 0.15,
                },
                "total_precip_mm": {
                    "field": "climate_total_precip_mm",
                    "weight": 0.15,
                },
            },
        },
        "hydro": {
            "minimum_history_years": 5,
            "signals": {
                "flow_p95": {
                    "weight": 0.30,
                    "normalization": (
                        "grid_calendar_month_"
                        "historical_percentile"
                    ),
                },
                "flow_variability": {
                    "weight": 0.15,
                    "normalization": (
                        "grid_calendar_month_"
                        "historical_percentile"
                    ),
                },
                "flow_zero_observation_ratio": {
                    "weight": 0.15,
                    "normalization": "none",
                },
                "level_p95": {
                    "weight": 0.25,
                    "normalization": (
                        "grid_calendar_month_"
                        "historical_percentile"
                    ),
                },
                "level_variability": {
                    "weight": 0.15,
                    "normalization": (
                        "grid_calendar_month_"
                        "historical_percentile"
                    ),
                },
            },
            "quality": {
                "point_quality_factor": 0.70,
            },
        },
        "wildfire": {
            "normalization": (
                "province_zero_preserving_"
                "positive_percentile"
            ),
            "quality": 1.0,
        },
        "missing_data": {
            "renormalize_available_signal_weights": True,
            "renormalize_available_domain_weights": True,
            "fill_missing_with_zero": False,
        },
        "composite": {
            "minimum_available_domains": 2,
        },
        "confidence": {
            "renormalize_missing_domains": False,
        },
        "ranking": {
            "group_by": [
                "province_key",
                "reference_month",
            ],
            "minimum_boundary_coverage_ratio": 0.01,
        },
        "priority_tiers": {
            "very_high": 0.90,
            "high": 0.75,
            "elevated": 0.50,
            "moderate": 0.25,
        },
    }


def _mart() -> pd.DataFrame:
    rows = []

    years = [
        2019,
        2020,
        2021,
        2022,
        2023,
    ]

    for grid_number, grid_cell_key in enumerate(
        [
            "ab_grid_a",
            "ab_grid_b",
        ]
    ):
        for year_index, year in enumerate(years):
            base = (
                year_index
                + 1
                + grid_number * 5
            )

            wildfire_overlap = (
                grid_cell_key == "ab_grid_b"
            )

            rows.append(
                {
                    "grid_month_risk_feature_key": (
                        f"{grid_cell_key}__{year}-07"
                    ),
                    "grid_cell_key": grid_cell_key,
                    "reference_month": f"{year}-07",
                    "grid_system": "ab_10km",
                    "province_key": "AB",
                    "boundary_coverage_ratio": 1.0,
                    "has_climate_feature": True,
                    "climate_mapping_method": (
                        "direct_station_in_cell"
                    ),
                    "climate_idw_confidence_score": 1.0,
                    "climate_data_completeness_score": 1.0,
                    "climate_extreme_heat_days": float(base),
                    "climate_heavy_precipitation_days": float(
                        base
                    ),
                    "climate_freeze_thaw_days": float(base),
                    "climate_extreme_cold_days": float(base),
                    "climate_total_precip_mm": float(
                        base * 10
                    ),
                    "has_hydro_spatial_coverage": True,
                    "has_hydro_flow_feature": True,
                    "has_hydro_level_feature": True,
                    "has_hydro_feature": True,
                    "hydro_spatial_assignment_method": (
                        "basin_polygon_intersection"
                    ),
                    "hydro_basin_grid_coverage_ratio": 1.0,
                    "hydro_data_completeness_score": 1.0,
                    "flow_mean_measurement_value": float(
                        base
                    ),
                    "flow_p95_measurement_value": float(
                        base * 2
                    ),
                    "flow_min_measurement_value": float(
                        base
                    ),
                    "flow_max_measurement_value": float(
                        base * 2
                    ),
                    "flow_zero_day_count": year_index,
                    "flow_observation_day_count": 10,
                    "level_mean_measurement_value": float(
                        base * 10
                    ),
                    "level_p95_measurement_value": float(
                        base * 20
                    ),
                    "level_min_measurement_value": float(
                        base * 10
                    ),
                    "level_max_measurement_value": float(
                        base * 20
                    ),
                    "has_wildfire_perimeter_feature": True,
                    "has_wildfire_observed_perimeter_overlap": (
                        wildfire_overlap
                    ),
                    "wildfire_perimeter_count": (
                        1
                        if wildfire_overlap
                        else 0
                    ),
                    (
                        "wildfire_"
                        "intersection_area_ratio_of_grid"
                    ): (
                        float(
                            (year_index + 1) / 100
                        )
                        if wildfire_overlap
                        else 0.0
                    ),
                }
            )

    return pd.DataFrame(rows)


def test_builder_preserves_full_grid_month_skeleton():
    source = _mart()

    result, summary = (
        build_gold_grid_month_risk_score(
            source,
            config=_config(),
        )
    )

    assert len(result) == len(source)
    assert summary["row_count"] == len(source)

    assert result[
        [
            "grid_cell_key",
            "reference_month",
        ]
    ].equals(
        source[
            [
                "grid_cell_key",
                "reference_month",
            ]
        ]
    )


def test_builder_creates_unique_risk_score_key():
    result, _ = build_gold_grid_month_risk_score(
        _mart(),
        config=_config(),
    )

    assert result[
        "risk_score_key"
    ].is_unique

    assert result.loc[
        0,
        "risk_score_key",
    ] == "ab_grid_a__2019-07"


def test_builder_combines_all_three_domains():
    result, _ = build_gold_grid_month_risk_score(
        _mart(),
        config=_config(),
    )

    assert result[
        "climate_domain_available"
    ].all()

    assert result[
        "hydro_domain_available"
    ].all()

    assert result[
        "wildfire_domain_available"
    ].all()

    assert (
        result["domain_coverage_count"]
        == 3
    ).all()

    assert result[
        "composite_score_eligible"
    ].all()


def test_builder_produces_full_confidence_for_complete_data():
    result, _ = build_gold_grid_month_risk_score(
        _mart(),
        config=_config(),
    )

    assert result[
        "score_confidence"
    ].tolist() == pytest.approx(
        [1.0] * len(result)
    )


def test_builder_produces_monthly_rankings():
    result, _ = build_gold_grid_month_risk_score(
        _mart(),
        config=_config(),
    )

    assert result[
        "ranking_eligible"
    ].all()

    for _, month in result.groupby(
        "reference_month"
    ):
        percentiles = sorted(
            month[
                "priority_percentile"
            ].tolist()
        )

        assert percentiles == pytest.approx(
            [
                0.5,
                1.0,
            ]
        )


def test_builder_summary_tracks_scoring_coverage():
    result, summary = (
        build_gold_grid_month_risk_score(
            _mart(),
            config=_config(),
        )
    )

    assert (
        summary[
            "composite_score_eligible_count"
        ]
        == len(result)
    )

    assert (
        summary["ranking_eligible_count"]
        == len(result)
    )

    assert summary[
        "composite_score_null_count"
    ] == 0

    assert summary[
        "domain_coverage_counts"
    ] == {
        "3": len(result),
    }


def test_builder_rejects_duplicate_grid_month_grain():
    mart = _mart()

    duplicate = pd.concat(
        [
            mart,
            mart.iloc[[0]],
        ],
        ignore_index=True,
    )

    with pytest.raises(
        RiskScoreBuildError,
        match="duplicate",
    ):
        build_gold_grid_month_risk_score(
            duplicate,
            config=_config(),
        )


def test_builder_rejects_fill_missing_with_zero_policy():
    config = _config()

    config["missing_data"][
        "fill_missing_with_zero"
    ] = True

    with pytest.raises(
        RiskScoreBuildError,
        match="must not be filled with zero",
    ):
        build_gold_grid_month_risk_score(
            _mart(),
            config=config,
        )