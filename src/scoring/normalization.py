from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


class RiskScoringNormalizationError(ValueError):
    """Raised when risk-score normalization inputs are invalid."""


def calendar_month_from_reference_month(
    reference_month: pd.Series,
) -> pd.Series:
    parsed = pd.to_datetime(
        reference_month,
        format="%Y-%m",
        errors="raise",
    )

    return parsed.dt.month.astype("int8")


def grouped_zero_preserving_positive_percentile(
    dataframe: pd.DataFrame,
    *,
    value_column: str,
    group_columns: Sequence[str],
) -> pd.Series:
    """Keep zero values at 0 and percentile-rank only positive values."""
    _require_columns(
        dataframe,
        [value_column, *group_columns],
    )

    values = pd.to_numeric(
        dataframe[value_column],
        errors="raise",
    )

    _validate_group_keys(
        dataframe,
        values=values,
        group_columns=group_columns,
    )

    negative_mask = values.lt(0) & values.notna()

    if negative_mask.any():
        raise RiskScoringNormalizationError(
            f"{value_column} contains " f"{int(negative_mask.sum())} negative values."
        )

    result = pd.Series(
        float("nan"),
        index=dataframe.index,
        dtype="float64",
        name=f"{value_column}_normalized",
    )

    zero_mask = values.eq(0)
    positive_mask = values.gt(0)

    result.loc[zero_mask] = 0.0

    if not positive_mask.any():
        return result

    positive = dataframe.loc[
        positive_mask,
        list(group_columns),
    ].copy()

    positive["_value"] = values.loc[positive_mask]

    positive["_percentile"] = positive.groupby(
        list(group_columns),
        observed=True,
        dropna=False,
    )["_value"].rank(
        pct=True,
        method="average",
    )

    result.loc[positive.index] = positive["_percentile"]

    return result


def grouped_historical_percentile(
    dataframe: pd.DataFrame,
    *,
    value_column: str,
    group_columns: Sequence[str],
    minimum_history_count: int,
) -> pd.Series:
    """Percentile-rank a value inside its local historical group (month)."""
    if minimum_history_count < 1:
        raise RiskScoringNormalizationError("minimum_history_count must be at least 1.")

    _require_columns(
        dataframe,
        [value_column, *group_columns],
    )

    values = pd.to_numeric(
        dataframe[value_column],
        errors="raise",
    )

    _validate_group_keys(
        dataframe,
        values=values,
        group_columns=group_columns,
    )

    working = dataframe[list(group_columns)].copy()

    working["_value"] = values

    grouped = working.groupby(
        list(group_columns),
        observed=True,
        dropna=False,
    )["_value"]

    history_count = grouped.transform("count")

    percentile = grouped.rank(
        pct=True,
        method="average",
    )

    result = percentile.where(history_count >= minimum_history_count)

    result.name = f"{value_column}_normalized"

    return result.astype("float64")


def _validate_group_keys(
    dataframe: pd.DataFrame,
    *,
    values: pd.Series,
    group_columns: Sequence[str],
) -> None:
    nonnull_values = values.notna()

    if not nonnull_values.any():
        return

    missing_group_keys = dataframe.loc[
        nonnull_values,
        list(group_columns),
    ].isna()

    if missing_group_keys.any().any():
        raise RiskScoringNormalizationError(
            "Normalization group columns contain null values "
            "for rows with non-null scoring values."
        )


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: Sequence[str],
) -> None:
    missing = set(required_columns) - set(dataframe.columns)

    if missing:
        raise RiskScoringNormalizationError(f"Missing required columns: {sorted(missing)}")
