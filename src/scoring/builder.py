from __future__ import annotations

from typing import Any

import pandas as pd

from src.scoring.climate import build_climate_scoring_features
from src.scoring.composite import build_composite_scoring_features
from src.scoring.hydro import build_hydro_scoring_features
from src.scoring.ranking import build_ranking_features
from src.scoring.wildfire import build_wildfire_scoring_features
from src.utils.config import load_project_config

SCORE_TABLE_NAME = "gold_grid_month_risk_score"
CONFIG_FILENAME = "risk_score_config.yml"

IDENTITY_COLUMNS = [
    "grid_month_risk_feature_key",
    "grid_cell_key",
    "reference_month",
    "grid_system",
    "province_key",
    "boundary_coverage_ratio",
]

OUTPUT_SCORING_COLUMNS = [
    "climate_sub_score",
    "climate_signal_weight_coverage",
    "climate_effective_quality",
    "hydro_sub_score",
    "hydro_signal_weight_coverage",
    "hydro_effective_quality",
    "wildfire_sub_score",
    "wildfire_effective_quality",
    "climate_domain_available",
    "hydro_domain_available",
    "wildfire_domain_available",
    "domain_coverage_count",
    "domain_coverage_ratio",
    "composite_score_eligible",
    "composite_risk_score",
    "score_confidence",
    "climate_effective_weight",
    "hydro_effective_weight",
    "wildfire_effective_weight",
    "climate_component_contribution",
    "hydro_component_contribution",
    "wildfire_component_contribution",
    "ranking_eligible",
    "ranking_exclusion_reason",
    "priority_percentile",
    "priority_tier",
]

class RiskScoreBuildError(ValueError):
    """Raised when risk-score build inputs or config are invalid."""
    
def load_risk_scoring_config() -> dict[str, Any]:
    # load confug from the project config file, originally from src/config/risk_score_config.yml
    config = load_project_config(CONFIG_FILENAME)
    _validate_config(config)
    return config

def build_gold_grid_month_risk_score(
    risk_feature_mart: pd.DataFrame,
    *,
    config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    # load config 
    scoring_config = (
        load_risk_scoring_config()
        if config is None
        else config
    )

    _validate_config(scoring_config)
    _validate_input_mart(risk_feature_mart)
    
    working = risk_feature_mart.reset_index(
        drop=True
    ).copy()
    
    climate_weights = {
        signal_config["field"]: float(
            signal_config["weight"]
        )
        for signal_config in scoring_config[
            "climate"
        ]["signals"].values()
    }

    hydro_weights = {
        signal_name: float(
            signal_config["weight"]
        )
        for signal_name, signal_config in scoring_config[
            "hydro"
        ]["signals"].items()
    }

    climate = build_climate_scoring_features(
        working,
        signal_weights=climate_weights,
    )

    hydro = build_hydro_scoring_features(
        working,
        signal_weights=hydro_weights,
        minimum_history_years=int(
            scoring_config["hydro"][
                "minimum_history_years"
            ]
        ),
        point_quality_factor=float(
            scoring_config["hydro"]["quality"][
                "point_quality_factor"
            ]
        ),
    )

    wildfire = build_wildfire_scoring_features(
        working,
        fixed_quality=float(
            scoring_config["wildfire"]["quality"]
        ),
    )

    domain_features = pd.concat(
        [
            climate,
            hydro,
            wildfire,
        ],
        axis=1,
    )

    composite = build_composite_scoring_features(
        domain_features,
        domain_weights=scoring_config[
            "domain_weights"
        ],
        minimum_available_domains=int(
            scoring_config["composite"][
                "minimum_available_domains"
            ]
        ),
    )

    scoring_features = pd.concat(
        [
            domain_features,
            composite,
        ],
        axis=1,
    )

    ranking_input = pd.concat(
        [
            working[
                [
                    "province_key",
                    "reference_month",
                    "boundary_coverage_ratio",
                ]
            ],
            scoring_features[
                [
                    "composite_score_eligible",
                    "composite_risk_score",
                ]
            ],
        ],
        axis=1,
    )

    ranking = build_ranking_features(
        ranking_input,
        group_columns=scoring_config[
            "ranking"
        ]["group_by"],
        minimum_boundary_coverage_ratio=float(
            scoring_config["ranking"][
                "minimum_boundary_coverage_ratio"
            ]
        ),
        priority_tiers=scoring_config[
            "priority_tiers"
        ],
    )

    scoring_features = pd.concat(
        [
            scoring_features,
            ranking,
        ],
        axis=1,
    )

    result = working[
        IDENTITY_COLUMNS
    ].copy()

    result["risk_score_key"] = (
        result["grid_cell_key"].astype(str)
        + "__"
        + result["reference_month"].astype(str)
    )

    result = result[
        [
            "risk_score_key",
            *IDENTITY_COLUMNS,
        ]
    ]

    result = pd.concat(
        [
            result,
            scoring_features[
                OUTPUT_SCORING_COLUMNS
            ],
        ],
        axis=1,
    )

    _validate_output(
        result,
        expected_row_count=len(working),
    )

    summary = summarize_risk_score(result)

    return result, summary
    
    
def summarize_risk_score(dataframe: pd.DataFrame) -> dict[str, Any]:
    composite_score = dataframe[
        "composite_risk_score"
    ]
    
    return {
        "row_count": int(len(dataframe)),
        "grid_cell_count": int(
            dataframe["grid_cell_key"].nunique()
        ),
        "month_count": int(
            dataframe["reference_month"].nunique()
        ),
        "minimum_month": str(
            dataframe["reference_month"].min()
        ),
        "maximum_month": str(
            dataframe["reference_month"].max()
        ),
        "composite_score_eligible_count": int(
            dataframe[
                "composite_score_eligible"
            ].sum()
        ),
        "ranking_eligible_count": int(
            dataframe["ranking_eligible"].sum()
        ),
        "composite_score_null_count": int(
            composite_score.isna().sum()
        ),
        "minimum_composite_score": (
            float(composite_score.min())
            if composite_score.notna().any()
            else None
        ),
        "maximum_composite_score": (
            float(composite_score.max())
            if composite_score.notna().any()
            else None
        ),
        "domain_coverage_counts": {
            str(key): int(value)
            for key, value in dataframe[
                "domain_coverage_count"
            ]
            .value_counts()
            .sort_index()
            .items()
        },
        "priority_tier_counts": {
            str(key): int(value)
            for key, value in dataframe[
                "priority_tier"
            ]
            .value_counts()
            .items()
        },
        "ranking_exclusion_reason_counts": {
            str(key): int(value)
            for key, value in dataframe[
                "ranking_exclusion_reason"
            ]
            .value_counts()
            .items()
        },
    }


def _validate_config(config: dict[str, Any]) -> None:
    required_sections = {
        "domain_weights",
        "climate",
        "hydro",
        "wildfire",
        "missing_data",
        "composite",
        "confidence",
        "ranking",
        "priority_tiers",
    }

    missing_sections = (
        required_sections - set(config)
    )

    if missing_sections:
        raise RiskScoreBuildError(
            "Risk score config is missing sections: "
            f"{sorted(missing_sections)}"
        )

    _require_config_key(
        config["climate"],
        "signals",
        "climate",
    )
    _require_config_key(
        config["hydro"],
        "signals",
        "hydro",
    )
    _require_config_key(
        config["hydro"],
        "minimum_history_years",
        "hydro",
    )
    _require_config_key(
        config["hydro"],
        "quality",
        "hydro",
    )
    _require_config_key(
        config["hydro"]["quality"],
        "point_quality_factor",
        "hydro.quality",
    )
    _require_config_key(
        config["wildfire"],
        "quality",
        "wildfire",
    )
    _require_config_key(
        config["composite"],
        "minimum_available_domains",
        "composite",
    )
    _require_config_key(
        config["ranking"],
        "group_by",
        "ranking",
    )
    _require_config_key(
        config["ranking"],
        "minimum_boundary_coverage_ratio",
        "ranking",
    )

    _validate_scoring_policy(config)


def _require_config_key(
    section: dict[str, Any],
    key: str,
    section_name: str,
) -> None:
    if key not in section:
        raise RiskScoreBuildError(
            f"Risk score config section {section_name} "
            f"is missing key: {key}"
        )


def _validate_scoring_policy(
    config: dict[str, Any],
) -> None:
    if config["climate"]["normalization"] != (
        "province_calendar_month_zero_preserving_positive_percentile"
    ):
        raise RiskScoreBuildError(
            "Unsupported Climate normalization."
        )

    expected_hydro_normalization = {
        "flow_p95": "grid_calendar_month_historical_percentile",
        "flow_variability": "grid_calendar_month_historical_percentile",
        "flow_zero_observation_ratio": "none",
        "level_p95": "grid_calendar_month_historical_percentile",
        "level_variability": "grid_calendar_month_historical_percentile",
    }

    for signal, expected in expected_hydro_normalization.items():
        actual = config["hydro"]["signals"][
            signal
        ]["normalization"]

        if actual != expected:
            raise RiskScoreBuildError(
                f"Unsupported Hydro normalization for {signal}."
            )

    if config["wildfire"]["normalization"] != (
        "province_zero_preserving_positive_percentile"
    ):
        raise RiskScoreBuildError(
            "Unsupported Wildfire normalization."
        )

    if config["missing_data"]["fill_missing_with_zero"]:
        raise RiskScoreBuildError(
            "Missing values must not be filled with zero."
        )

    if not config["missing_data"][
        "renormalize_available_signal_weights"
    ]:
        raise RiskScoreBuildError(
            "Available signal weights must be renormalized."
        )

    if not config["missing_data"][
        "renormalize_available_domain_weights"
    ]:
        raise RiskScoreBuildError(
            "Available domain weights must be renormalized."
        )

    if config["confidence"][
        "renormalize_missing_domains"
    ]:
        raise RiskScoreBuildError(
            "Confidence must not renormalize missing domains."
        )

def _validate_input_mart(
    dataframe: pd.DataFrame,
) -> None:
    missing = set(IDENTITY_COLUMNS) - set(
        dataframe.columns
    )

    if missing:
        raise RiskScoreBuildError(
            "Risk feature mart is missing identity columns: "
            f"{sorted(missing)}"
        )

    if dataframe[
        [
            "grid_cell_key",
            "reference_month",
            "province_key",
        ]
    ].isna().any().any():
        raise RiskScoreBuildError(
            "Risk feature mart identity columns "
            "must not contain null values."
        )

    duplicate_count = int(
        dataframe[
            [
                "grid_cell_key",
                "reference_month",
            ]
        ].duplicated().sum()
    )

    if duplicate_count > 0:
        raise RiskScoreBuildError(
            "Risk feature mart contains duplicate "
            "grid_cell_key x reference_month rows: "
            f"{duplicate_count}"
        )
        
def _validate_output(
    dataframe: pd.DataFrame,
    *,
    expected_row_count: int,
) -> None:
    if len(dataframe) != expected_row_count:
        raise RiskScoreBuildError(
            "Risk score build changed the input row count."
        )

    if dataframe[
        "risk_score_key"
    ].duplicated().any():
        raise RiskScoreBuildError(
            "risk_score_key must be unique."
        )

    if dataframe[
        "risk_score_key"
    ].isna().any():
        raise RiskScoreBuildError(
            "risk_score_key must not contain null values."
        )
