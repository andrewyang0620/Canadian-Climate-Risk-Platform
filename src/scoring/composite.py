from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd


DOMAINS = (
    "climate",
    "hydro",
    "wildfire",
)

REQUIRED_COLUMNS = {
    "climate_sub_score",
    "hydro_sub_score",
    "wildfire_sub_score",
    "climate_domain_available",
    "hydro_domain_available",
    "wildfire_domain_available",
    "climate_effective_quality",
    "hydro_effective_quality",
    "wildfire_effective_quality",
}

class CompositeScoringError(ValueError):
    """Raised when composite scoring inputs are invalid."""


def build_composite_scoring_features(
    dataframe:pd.DataFrame,
    *,
    domain_weights: Mapping[str, float],
    minimum_available_domains: int,
) -> pd.DataFrame:
    """The main method for building the multi-hazard composite score and evidence confidence."""
    _require_columns(dataframe)

    _validate_parameters(
        domain_weights=domain_weights,
        minimum_available_domains=minimum_available_domains,
    )

    _validate_inputs(dataframe)

    result = pd.DataFrame(index=dataframe.index)

    availability = pd.DataFrame(
        {
            domain: dataframe[
                f"{domain}_domain_available"
            ].astype(bool)
            for domain in DOMAINS
        },
        index=dataframe.index,
    )

    result["domain_coverage_count"] = (
        availability.sum(axis=1).astype("int8")
    )

    result["domain_coverage_ratio"] = (
        result["domain_coverage_count"]
        / float(len(DOMAINS))
    )

    result["composite_score_eligible"] = (
        result["domain_coverage_count"]
        >= minimum_available_domains
    )

    available_domain_weight = pd.Series(
        0.0,
        index=dataframe.index,
        dtype="float64",
    )

    weighted_score_sum = pd.Series(
        0.0,
        index=dataframe.index,
        dtype="float64",
    )

    for domain in DOMAINS:
        weight = float(domain_weights[domain])

        domain_available = availability[domain]

        domain_score = pd.to_numeric(
            dataframe[f"{domain}_sub_score"],
            errors="raise",
        )

        available_domain_weight += (
            domain_available.astype("float64")
            * weight
        )

        weighted_score_sum += (
            domain_score.fillna(0.0)
            * domain_available.astype("float64")
            * weight
        )

    result["composite_risk_score"] = (
        weighted_score_sum
        / available_domain_weight.replace(0.0, np.nan)
    )

    result.loc[
        ~result["composite_score_eligible"],
        "composite_risk_score",
    ] = np.nan

    for domain in DOMAINS:
        weight = float(domain_weights[domain])

        domain_available = availability[domain]

        effective_weight = pd.Series(
            0.0,
            index=dataframe.index,
            dtype="float64",
        )

        eligible_and_available = (
            result["composite_score_eligible"]
            & domain_available
        )

        effective_weight.loc[
            eligible_and_available
        ] = (
            weight
            / available_domain_weight.loc[
                eligible_and_available
            ]
        )

        effective_weight.loc[
            ~result["composite_score_eligible"]
        ] = np.nan

        result[
            f"{domain}_effective_weight"
        ] = effective_weight

        contribution = (
            effective_weight
            * pd.to_numeric(
                dataframe[f"{domain}_sub_score"],
                errors="raise",
            ).fillna(0.0)
        )

        contribution.loc[
            ~result["composite_score_eligible"]
        ] = np.nan

        result[
            f"{domain}_component_contribution"
        ] = contribution

    confidence = pd.Series(
        0.0,
        index=dataframe.index,
        dtype="float64",
    )

    for domain in DOMAINS:
        weight = float(domain_weights[domain])

        quality = pd.to_numeric(
            dataframe[f"{domain}_effective_quality"],
            errors="raise",
        )

        confidence += weight * quality

    result["score_confidence"] = confidence

    _validate_outputs(result)

    return result
    

def _validate_parameters(
    *,
    domain_weights: Mapping[str, float],
    minimum_available_domains: int,
) -> None:
    expected = set(DOMAINS)
    provided = set(domain_weights)

    missing = expected - provided
    extra = provided - expected

    if missing or extra:
        raise CompositeScoringError(
            "Domain weights do not match expected domains. "
            f"Missing: {sorted(missing)}; "
            f"extra: {sorted(extra)}"
        )

    weights = pd.Series(
        domain_weights,
        dtype="float64",
    )

    if weights.le(0).any():
        raise CompositeScoringError(
            "Domain weights must be positive."
        )

    if not np.isclose(weights.sum(), 1.0):
        raise CompositeScoringError(
            "Domain weights must sum to 1.0."
        )

    if not 1 <= minimum_available_domains <= len(DOMAINS):
        raise CompositeScoringError(
            "minimum_available_domains must be between "
            f"1 and {len(DOMAINS)}."
        )

def _validate_inputs(dataframe: pd.DataFrame) -> None:
    for domain in DOMAINS:
        score_column = f"{domain}_sub_score"
        availability_column = (
            f"{domain}_domain_available"
        )
        quality_column = (
            f"{domain}_effective_quality"
        )

        if dataframe[
            availability_column
        ].isna().any():
            raise CompositeScoringError(
                f"{availability_column} must not contain null values."
            )

        score = pd.to_numeric(
            dataframe[score_column],
            errors="raise",
        )

        quality = pd.to_numeric(
            dataframe[quality_column],
            errors="raise",
        )

        invalid_score = (
            score.notna()
            & ~score.between(0.0, 1.0)
        )

        if invalid_score.any():
            raise CompositeScoringError(
                f"{score_column} must be in [0, 1]."
            )

        if quality.isna().any():
            raise CompositeScoringError(
                f"{quality_column} must not contain null values."
            )

        if not quality.between(
            0.0,
            1.0,
        ).all():
            raise CompositeScoringError(
                f"{quality_column} must be in [0, 1]."
            )

        available = dataframe[
            availability_column
        ].astype(bool)

        if not available.equals(score.notna()):
            raise CompositeScoringError(
                f"{availability_column} is inconsistent with "
                f"{score_column}."
            )

        unavailable_quality = quality.loc[
            ~available
        ]

        if not np.allclose(
            unavailable_quality,
            0.0,
        ):
            raise CompositeScoringError(
                f"{quality_column} must be 0 when "
                f"{availability_column} is false."
            )
            
def _validate_outputs(result: pd.DataFrame) -> None:
    bounded_columns = [
        "domain_coverage_ratio",
        "composite_risk_score",
        "score_confidence",
        "climate_effective_weight",
        "hydro_effective_weight",
        "wildfire_effective_weight",
        "climate_component_contribution",
        "hydro_component_contribution",
        "wildfire_component_contribution",
    ]

    for column in bounded_columns:
        values = result[column].dropna()

        if not values.between(
            0.0,
            1.0,
        ).all():
            raise CompositeScoringError(
                f"{column} contains values outside [0, 1]."
            )

    eligible = result[
        "composite_score_eligible"
    ]

    effective_weight_columns = [
        f"{domain}_effective_weight"
        for domain in DOMAINS
    ]

    contribution_columns = [
        f"{domain}_component_contribution"
        for domain in DOMAINS
    ]

    effective_weight_sum = result[
        effective_weight_columns
    ].sum(
        axis=1,
        min_count=1,
    )

    if not np.allclose(
        effective_weight_sum.loc[eligible],
        1.0,
    ):
        raise CompositeScoringError(
            "Effective domain weights must sum to 1.0 "
            "for eligible rows."
        )

    contribution_sum = result[
        contribution_columns
    ].sum(
        axis=1,
        min_count=1,
    )

    if not np.allclose(
        contribution_sum.loc[eligible],
        result.loc[
            eligible,
            "composite_risk_score",
        ],
    ):
        raise CompositeScoringError(
            "Component contributions must sum to "
            "composite_risk_score."
        )
        
def _require_columns(dataframe: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(
        dataframe.columns
    )

    if missing:
        raise CompositeScoringError(
            "Missing required composite scoring columns: "
            f"{sorted(missing)}"
        )