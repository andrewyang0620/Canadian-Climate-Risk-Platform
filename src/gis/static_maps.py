from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class MapSpec:
    column: str
    title: str
    legend_label: str
    cmap: str
    vmin: float | None = None
    vmax: float | None = None


MAP_SPECS = [
    MapSpec(
        column="composite_risk_score",
        title="Composite Climate Risk",
        legend_label="Composite Risk Score",
        cmap="YlOrRd",
        vmin=0.0,
        vmax=1.0,
    ),
    MapSpec(
        column="climate_mean_temp_c",
        title="Mean Temperature",
        legend_label="Mean Temperature (°C)",
        cmap="coolwarm",
    ),
    MapSpec(
        column="hydro_sub_score",
        title="Hydro Risk Signal",
        legend_label="Hydro Sub-score",
        cmap="Blues",
        vmin=0.0,
        vmax=1.0,
    ),
    MapSpec(
        column="wildfire_intersection_area_ratio_of_grid",
        title="Observed Wildfire Grid Overlap",
        legend_label="Grid Area Overlap Ratio",
        cmap="YlOrRd",
        vmin=0.0,
        vmax=1.0,
    ),
    MapSpec(
        column="score_confidence",
        title="Risk Score Confidence",
        legend_label="Score Confidence",
        cmap="viridis",
        vmin=0.0,
        vmax=1.0,
    ),
]


class StaticMapError(ValueError):
    """Raised when GIS serving files cannot produce a valid map."""


def load_monthly_map_data(
    *,
    data_root: str | Path,
    reference_month: str,
) -> gpd.GeoDataFrame:
    """Load geometry and one monthly serving file."""

    data_root = Path(data_root)

    manifest_path = (
        data_root / "manifest.json"
    )

    geometry_path = (
        data_root / "grid_geometry.geojson"
    )

    month_path = (
        data_root
        / "months"
        / f"risk_{reference_month}.json"
    )

    if not manifest_path.exists():
        raise StaticMapError(
            f"Manifest does not exist: {manifest_path}"
        )

    if not geometry_path.exists():
        raise StaticMapError(
            f"Geometry does not exist: {geometry_path}"
        )

    if not month_path.exists():
        raise StaticMapError(
            f"Monthly data does not exist: {month_path}"
        )

    manifest = json.loads(
        manifest_path.read_text(
            encoding="utf-8"
        )
    )

    month_payload = json.loads(
        month_path.read_text(
            encoding="utf-8"
        )
    )

    monthly_columns = manifest[
        "monthly_data"
    ]["columns"]

    monthly = pd.DataFrame(
        month_payload["rows"],
        columns=monthly_columns,
    )

    geometry = gpd.read_file(
        geometry_path
    )

    if (
        geometry["grid_cell_key"]
        .duplicated()
        .any()
    ):
        raise StaticMapError(
            "Geometry contains duplicate grid_cell_key values."
        )

    if (
        monthly["grid_cell_key"]
        .duplicated()
        .any()
    ):
        raise StaticMapError(
            "Monthly data contains duplicate grid_cell_key values."
        )

    geometry_keys = set(
        geometry["grid_cell_key"]
    )

    monthly_keys = set(
        monthly["grid_cell_key"]
    )

    if geometry_keys != monthly_keys:
        raise StaticMapError(
            "Geometry and monthly grid keys do not match."
        )

    result = geometry.merge(
        monthly,
        on="grid_cell_key",
        how="left",
        validate="one_to_one",
    )

    return gpd.GeoDataFrame(
        result,
        geometry="geometry",
        crs=geometry.crs,
    )


def render_static_map(
    dataframe: gpd.GeoDataFrame,
    *,
    spec: MapSpec,
    reference_month: str,
    output_path: str | Path,
) -> dict[str, Any]:
    """Render one national GIS validation map."""

    if spec.column not in dataframe.columns:
        raise StaticMapError(
            f"Map column does not exist: {spec.column}"
        )

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    plot_data = dataframe.copy()

    plot_data[spec.column] = (
        pd.to_numeric(
            plot_data[spec.column],
            errors="coerce",
        )
    )

    valid_count = int(
        plot_data[spec.column]
        .notna()
        .sum()
    )

    null_count = int(
        plot_data[spec.column]
        .isna()
        .sum()
    )

    if valid_count == 0:
        raise StaticMapError(
            f"{spec.column} has no valid values."
        )

    province_boundary = (
        plot_data[
            [
                "province_key",
                "geometry",
            ]
        ]
        .dissolve(
            by="province_key"
        )
    )

    fig, ax = plt.subplots(
        figsize=(12, 9),
        constrained_layout=True,
    )

    plot_data.plot(
        column=spec.column,
        ax=ax,
        cmap=spec.cmap,
        legend=True,
        vmin=spec.vmin,
        vmax=spec.vmax,
        linewidth=0,
        missing_kwds={
            "color": "lightgrey",
            "label": "No data",
        },
        legend_kwds={
            "label": spec.legend_label,
            "shrink": 0.72,
        },
    )

    province_boundary.boundary.plot(
        ax=ax,
        color="black",
        linewidth=0.7,
    )

    ax.set_title(
        (
            f"{spec.title}\n"
            f"Alberta & British Columbia · "
            f"{reference_month}"
        ),
        fontsize=16,
        pad=14,
    )

    ax.set_axis_off()

    fig.savefig(
        output_path,
        dpi=180,
        bbox_inches="tight",
    )

    plt.close(fig)

    return {
        "column": spec.column,
        "reference_month": reference_month,
        "valid_count": valid_count,
        "null_count": null_count,
        "output_path": output_path.as_posix(),
    }


def render_national_static_maps(
    *,
    reference_month: str,
    data_root: str | Path = "dashboard/gis/data",
    output_root: str | Path = "dashboard/gis/maps",
) -> list[dict[str, Any]]:
    """Render the standard national static-map validation set."""

    dataframe = load_monthly_map_data(
        data_root=data_root,
        reference_month=reference_month,
    )

    output_root = (
        Path(output_root)
        / reference_month
    )

    results: list[dict[str, Any]] = []

    for spec in MAP_SPECS:
        output_path = (
            output_root
            / f"{spec.column}.png"
        )

        result = render_static_map(
            dataframe,
            spec=spec,
            reference_month=reference_month,
            output_path=output_path,
        )

        results.append(result)

    return results