from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import shapely
from pyproj import CRS, Transformer
from shapely.ops import transform as shapely_transform
from shapely.strtree import STRtree


TARGET_TABLE = "gold_grid_month_wildfire_perimeter_feature"
GRID_TABLE = "gold_grid_cell"
POLYGON_TABLE = "silver_wildfire_perimeter_polygon"

TARGET_GRID_SYSTEMS = {"ab_10km", "bc_10km"}
FEATURE_START_YEAR = 2016
FEATURE_END_YEAR = 2025
TARGET_CRS_EPSG = 3347

SOURCE_CRS_WKT = (
    'PROJCS["NAD_1983_Lambert_Conformal_Conic",'
    'GEOGCS["GCS_North_American_1983",'
    'DATUM["D_North_American_1983",'
    'SPHEROID["GRS_1980",6378137.0,298.257222101]],'
    'PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]],'
    'PROJECTION["Lambert_Conformal_Conic"],'
    'PARAMETER["False_Easting",0.0],'
    'PARAMETER["False_Northing",0.0],'
    'PARAMETER["Central_Meridian",-95.0],'
    'PARAMETER["Standard_Parallel_1",49.0],'
    'PARAMETER["Standard_Parallel_2",77.0],'
    'PARAMETER["Latitude_Of_Origin",49.0],'
    'UNIT["Meter",1.0]]'
)


class GoldWildfirePerimeterFeatureError(RuntimeError):
    pass


@dataclass(frozen=True)
class WildfirePerimeterGridFeatureSummary:
    input_polygon_count: int
    feature_window_polygon_count: int
    monthly_assignable_polygon_count: int
    missing_or_invalid_month_polygon_count: int
    monthly_assignable_rate: float
    intersecting_polygon_count: int
    grid_cell_count: int
    month_count: int
    output_row_count: int
    output_nonzero_grid_month_count: int
    total_intersection_area_ha: float
    crs_epsg: int


def build_gold_grid_month_wildfire_perimeter_feature(
    *,
    wildfire_perimeters: pd.DataFrame,
    grid: pd.DataFrame,
    start_year: int = FEATURE_START_YEAR,
    end_year: int = FEATURE_END_YEAR,
) -> tuple[pd.DataFrame, WildfirePerimeterGridFeatureSummary]:
    _require_columns(
        wildfire_perimeters,
        {
            "wildfire_perimeter_key",
            "province",
            "fire_year",
            "fire_month",
            "source_size_ha",
            "calculated_size_ha",
            "fire_cause",
            "prescribed",
            "geometry_wkt",
            "geometry_is_valid",
            "source_crs",
        },
        POLYGON_TABLE,
    )

    _require_columns(
        grid,
        {
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "analysis_area_sq_km",
            "analysis_geometry_wkt",
            "crs_epsg",
        },
        GRID_TABLE,
    )

    grid_for_feature = grid[grid["grid_system"].isin(TARGET_GRID_SYSTEMS)].copy()

    if grid_for_feature.empty:
        raise GoldWildfirePerimeterFeatureError("No AB/BC 10km grid cells are available.")

    invalid_grid_crs = grid_for_feature["crs_epsg"].ne(TARGET_CRS_EPSG).sum()

    if invalid_grid_crs:
        raise GoldWildfirePerimeterFeatureError(
            f"Gold grid contains {invalid_grid_crs} rows outside EPSG:{TARGET_CRS_EPSG}."
        )

    feature_window = wildfire_perimeters[
        wildfire_perimeters["fire_year"].between(start_year, end_year)
    ].copy()

    feature_window = feature_window[feature_window["geometry_is_valid"]].copy()

    usable_month = feature_window["fire_month"].between(1, 12)
    monthly_assignable = feature_window[usable_month].copy()
    missing_or_invalid_month_count = int((~usable_month).sum())

    monthly_assignable["reference_month"] = build_reference_month(
        monthly_assignable["fire_year"],
        monthly_assignable["fire_month"],
    )

    skeleton = build_grid_month_skeleton(
        grid=grid_for_feature,
        start_year=start_year,
        end_year=end_year,
    )

    intersections = intersect_wildfire_perimeters_with_grid(
        wildfire_perimeters=monthly_assignable,
        grid=grid_for_feature,
    )

    aggregated = aggregate_intersections(intersections)

    result = skeleton.merge(
        aggregated,
        on=[
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
        ],
        how="left",
        validate="one_to_one",
    )

    result = fill_zero_feature_columns(result)

    result["wildfire_grid_month_key"] = (
        result["grid_cell_key"].astype(str) + "__" + result["reference_month"].astype(str)
    )

    result = (
        result[
            [
                "wildfire_grid_month_key",
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
                "reference_month",
                "crs_epsg",
                "grid_analysis_area_sq_km",
                "wildfire_perimeter_count",
                "wildfire_intersection_area_sq_km",
                "wildfire_intersection_area_ha",
                "wildfire_intersection_area_ratio_of_grid",
                "wildfire_max_source_size_ha",
                "wildfire_max_calculated_size_ha",
                "wildfire_cause_n_polygon_count",
                "wildfire_cause_h_polygon_count",
                "wildfire_cause_u_polygon_count",
                "wildfire_cause_prescribed_burn_polygon_count",
                "wildfire_cause_other_polygon_count",
                "wildfire_has_observed_perimeter_overlap",
                "wildfire_temporal_assignment_method",
            ]
        ]
        .sort_values(["grid_cell_key", "reference_month"])
        .reset_index(drop=True)
    )

    output_nonzero_count = int(result["wildfire_has_observed_perimeter_overlap"].sum())
    total_intersection_area_ha = float(result["wildfire_intersection_area_ha"].sum())

    summary = WildfirePerimeterGridFeatureSummary(
        input_polygon_count=int(len(wildfire_perimeters)),
        feature_window_polygon_count=int(len(feature_window)),
        monthly_assignable_polygon_count=int(len(monthly_assignable)),
        missing_or_invalid_month_polygon_count=missing_or_invalid_month_count,
        monthly_assignable_rate=(
            round(len(monthly_assignable) / len(feature_window), 6) if len(feature_window) else 0.0
        ),
        intersecting_polygon_count=(
            int(intersections["wildfire_perimeter_key"].nunique()) if not intersections.empty else 0
        ),
        grid_cell_count=int(grid_for_feature["grid_cell_key"].nunique()),
        month_count=int(result["reference_month"].nunique()),
        output_row_count=int(len(result)),
        output_nonzero_grid_month_count=output_nonzero_count,
        total_intersection_area_ha=round(total_intersection_area_ha, 6),
        crs_epsg=TARGET_CRS_EPSG,
    )

    validate_feature_dataframe(result, summary)

    return result, summary


def build_reference_month(
    fire_year: pd.Series,
    fire_month: pd.Series,
) -> pd.Series:
    if fire_year.empty:
        return pd.Series(index=fire_year.index, dtype="object")

    year_text = fire_year.astype("int64").astype(str).str.zfill(4)
    month_text = fire_month.astype("int64").astype(str).str.zfill(2)

    return year_text + "-" + month_text


def build_grid_month_skeleton(
    *,
    grid: pd.DataFrame,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    months = pd.period_range(
        f"{start_year}-01",
        f"{end_year}-12",
        freq="M",
    ).astype(str)

    months_df = pd.DataFrame({"reference_month": months})

    grid_columns = [
        "grid_cell_key",
        "grid_system",
        "grid_level",
        "grid_version",
        "province_key",
        "analysis_area_sq_km",
        "crs_epsg",
    ]

    skeleton = grid[grid_columns].copy()
    skeleton = skeleton.rename(columns={"analysis_area_sq_km": "grid_analysis_area_sq_km"})
    skeleton["_join_key"] = 1
    months_df["_join_key"] = 1

    result = skeleton.merge(months_df, on="_join_key", how="inner").drop(columns=["_join_key"])

    return result


def intersect_wildfire_perimeters_with_grid(
    *,
    wildfire_perimeters: pd.DataFrame,
    grid: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    if wildfire_perimeters.empty:
        return empty_intersection_dataframe()

    transformer_cache: dict[str, Transformer | None] = {}

    for province_key, grid_group in grid.groupby("province_key", dropna=False):
        province_perimeters = wildfire_perimeters[
            wildfire_perimeters["province"].astype(str).str.upper() == str(province_key).upper()
        ].copy()

        if province_perimeters.empty:
            continue

        grid_lookup = grid_group.reset_index(drop=True)
        grid_geometries = shapely.from_wkt(
            grid_lookup["analysis_geometry_wkt"].astype(str).to_numpy()
        )
        tree = STRtree(grid_geometries)

        for perimeter in province_perimeters.itertuples(index=False):
            source_crs = str(perimeter.source_crs)
            transformer = transformer_cache.get(source_crs)

            if source_crs not in transformer_cache:
                transformer = build_source_to_target_transformer(source_crs)
                transformer_cache[source_crs] = transformer

            perimeter_geometry = shapely.from_wkt(str(perimeter.geometry_wkt))

            if transformer is not None:
                perimeter_geometry = shapely_transform(
                    transformer.transform,
                    perimeter_geometry,
                )

            if perimeter_geometry.is_empty:
                continue

            candidate_indices = tree.query(perimeter_geometry, predicate="intersects")

            for candidate_index in candidate_indices:
                candidate_index = int(candidate_index)
                grid_geometry = grid_geometries[candidate_index]
                intersection = perimeter_geometry.intersection(grid_geometry)

                if intersection.is_empty:
                    continue

                intersection_area_sq_m = float(intersection.area)

                if intersection_area_sq_m <= 0:
                    continue

                grid_row = grid_lookup.iloc[candidate_index]
                rows.append(
                    {
                        "grid_cell_key": grid_row["grid_cell_key"],
                        "grid_system": grid_row["grid_system"],
                        "grid_level": grid_row["grid_level"],
                        "grid_version": grid_row["grid_version"],
                        "province_key": grid_row["province_key"],
                        "reference_month": perimeter.reference_month,
                        "wildfire_perimeter_key": perimeter.wildfire_perimeter_key,
                        "wildfire_intersection_area_sq_km": intersection_area_sq_m / 1_000_000,
                        "wildfire_intersection_area_ha": intersection_area_sq_m / 10_000,
                        "source_size_ha": perimeter.source_size_ha,
                        "calculated_size_ha": perimeter.calculated_size_ha,
                        "fire_cause_category": normalize_cause_category(
                            perimeter.fire_cause,
                            perimeter.prescribed,
                        ),
                    }
                )

    if not rows:
        return empty_intersection_dataframe()

    return pd.DataFrame(rows)


def build_source_to_target_transformer(source_crs_name: str) -> Transformer | None:
    normalized = source_crs_name.strip().upper()

    if normalized in {f"EPSG:{TARGET_CRS_EPSG}", str(TARGET_CRS_EPSG)}:
        return None

    if normalized == "NAD_1983_LAMBERT_CONFORMAL_CONIC":
        source_crs = CRS.from_wkt(SOURCE_CRS_WKT)
    else:
        source_crs = CRS.from_user_input(source_crs_name)

    target_crs = CRS.from_epsg(TARGET_CRS_EPSG)

    return Transformer.from_crs(source_crs, target_crs, always_xy=True)


def aggregate_intersections(intersections: pd.DataFrame) -> pd.DataFrame:
    if intersections.empty:
        return empty_aggregated_dataframe()

    detail = intersections.copy()

    detail["wildfire_cause_n_polygon_count"] = (detail["fire_cause_category"] == "N").astype(int)
    detail["wildfire_cause_h_polygon_count"] = (detail["fire_cause_category"] == "H").astype(int)
    detail["wildfire_cause_u_polygon_count"] = (detail["fire_cause_category"] == "U").astype(int)
    detail["wildfire_cause_prescribed_burn_polygon_count"] = (
        detail["fire_cause_category"] == "PRESCRIBED_BURN"
    ).astype(int)
    detail["wildfire_cause_other_polygon_count"] = (
        detail["fire_cause_category"] == "OTHER"
    ).astype(int)

    grouped = (
        detail.groupby(
            [
                "grid_cell_key",
                "grid_system",
                "grid_level",
                "grid_version",
                "province_key",
                "reference_month",
            ],
            dropna=False,
        )
        .agg(
            wildfire_perimeter_count=("wildfire_perimeter_key", "nunique"),
            wildfire_intersection_area_sq_km=(
                "wildfire_intersection_area_sq_km",
                "sum",
            ),
            wildfire_intersection_area_ha=("wildfire_intersection_area_ha", "sum"),
            wildfire_max_source_size_ha=("source_size_ha", "max"),
            wildfire_max_calculated_size_ha=("calculated_size_ha", "max"),
            wildfire_cause_n_polygon_count=("wildfire_cause_n_polygon_count", "sum"),
            wildfire_cause_h_polygon_count=("wildfire_cause_h_polygon_count", "sum"),
            wildfire_cause_u_polygon_count=("wildfire_cause_u_polygon_count", "sum"),
            wildfire_cause_prescribed_burn_polygon_count=(
                "wildfire_cause_prescribed_burn_polygon_count",
                "sum",
            ),
            wildfire_cause_other_polygon_count=(
                "wildfire_cause_other_polygon_count",
                "sum",
            ),
        )
        .reset_index()
    )

    return grouped


def fill_zero_feature_columns(result: pd.DataFrame) -> pd.DataFrame:
    numeric_zero_columns = [
        "wildfire_perimeter_count",
        "wildfire_intersection_area_sq_km",
        "wildfire_intersection_area_ha",
        "wildfire_max_source_size_ha",
        "wildfire_max_calculated_size_ha",
        "wildfire_cause_n_polygon_count",
        "wildfire_cause_h_polygon_count",
        "wildfire_cause_u_polygon_count",
        "wildfire_cause_prescribed_burn_polygon_count",
        "wildfire_cause_other_polygon_count",
    ]

    for column in numeric_zero_columns:
        if column not in result.columns:
            result[column] = 0.0

        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    count_columns = [
        "wildfire_perimeter_count",
        "wildfire_cause_n_polygon_count",
        "wildfire_cause_h_polygon_count",
        "wildfire_cause_u_polygon_count",
        "wildfire_cause_prescribed_burn_polygon_count",
        "wildfire_cause_other_polygon_count",
    ]

    for column in count_columns:
        result[column] = result[column].astype("int64")

    denominator_ha = result["grid_analysis_area_sq_km"] * 100

    result["wildfire_intersection_area_ratio_of_grid"] = np.where(
        denominator_ha > 0,
        result["wildfire_intersection_area_ha"] / denominator_ha,
        0.0,
    )

    result["wildfire_has_observed_perimeter_overlap"] = result["wildfire_perimeter_count"] > 0

    result["wildfire_temporal_assignment_method"] = np.where(
        result["wildfire_has_observed_perimeter_overlap"],
        "polygon_fire_month",
        "no_observed_perimeter_overlap",
    )

    return result


def validate_feature_dataframe(
    result: pd.DataFrame,
    summary: WildfirePerimeterGridFeatureSummary,
) -> None:
    if result.empty:
        raise GoldWildfirePerimeterFeatureError("Wildfire perimeter Gold output is empty.")

    duplicate_key_count = int(result["wildfire_grid_month_key"].duplicated().sum())

    if duplicate_key_count:
        raise GoldWildfirePerimeterFeatureError(
            f"Duplicate wildfire_grid_month_key rows: {duplicate_key_count}"
        )

    if summary.month_count != 120:
        raise GoldWildfirePerimeterFeatureError(
            f"Expected 120 reference months, got {summary.month_count}."
        )

    expected_rows = summary.grid_cell_count * summary.month_count

    if summary.output_row_count != expected_rows:
        raise GoldWildfirePerimeterFeatureError(
            f"Expected {expected_rows} rows, got {summary.output_row_count}."
        )

    negative_metric_columns = [
        "wildfire_perimeter_count",
        "wildfire_intersection_area_sq_km",
        "wildfire_intersection_area_ha",
        "wildfire_intersection_area_ratio_of_grid",
    ]

    for column in negative_metric_columns:
        negative_count = int((result[column] < 0).sum())

        if negative_count:
            raise GoldWildfirePerimeterFeatureError(
                f"{column} contains negative values: {negative_count}"
            )


def empty_intersection_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
            "wildfire_perimeter_key",
            "wildfire_intersection_area_sq_km",
            "wildfire_intersection_area_ha",
            "source_size_ha",
            "calculated_size_ha",
            "fire_cause_category",
        ]
    )


def empty_aggregated_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "grid_cell_key",
            "grid_system",
            "grid_level",
            "grid_version",
            "province_key",
            "reference_month",
            "wildfire_perimeter_count",
            "wildfire_intersection_area_sq_km",
            "wildfire_intersection_area_ha",
            "wildfire_max_source_size_ha",
            "wildfire_max_calculated_size_ha",
            "wildfire_cause_n_polygon_count",
            "wildfire_cause_h_polygon_count",
            "wildfire_cause_u_polygon_count",
            "wildfire_cause_prescribed_burn_polygon_count",
            "wildfire_cause_other_polygon_count",
        ]
    )


def normalize_cause_category(
    fire_cause: Any,
    prescribed: Any,
) -> str:
    cause_text = "" if fire_cause is None else str(fire_cause).strip().upper()
    prescribed_text = "" if prescribed is None else str(prescribed).strip().upper()

    prescribed_values = {
        "Y",
        "YES",
        "TRUE",
        "T",
        "1",
        "PRESCRIBED",
        "PRESCRIBED BURN",
        "PRESCRIBED_BURN",
        "PB",
        "H-PB",
    }

    if cause_text in prescribed_values or prescribed_text in prescribed_values:
        return "PRESCRIBED_BURN"

    if cause_text in {"N", "H", "U"}:
        return cause_text

    return "OTHER"


def run_gold_grid_month_wildfire_perimeter_feature(
    *,
    lakehouse_root: str | Path = "lakehouse",
) -> dict[str, Any]:
    lakehouse_root = Path(lakehouse_root)
    gold_root = lakehouse_root / "gold"
    silver_root = lakehouse_root / "silver"

    grid_path = latest_table_path(gold_root / GRID_TABLE, GRID_TABLE)
    polygon_path = latest_table_path(silver_root / POLYGON_TABLE, POLYGON_TABLE)

    grid = pd.read_parquet(grid_path)
    wildfire_perimeters = pd.read_parquet(polygon_path)

    result, summary = build_gold_grid_month_wildfire_perimeter_feature(
        wildfire_perimeters=wildfire_perimeters,
        grid=grid,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()

    output_path = (
        gold_root
        / TARGET_TABLE
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / f"{TARGET_TABLE}.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)

    metadata = {
        "run_id": run_id,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "target_table": TARGET_TABLE,
        "output_path": output_path.as_posix(),
        "input_tables": {
            GRID_TABLE: grid_path.as_posix(),
            POLYGON_TABLE: polygon_path.as_posix(),
        },
        "summary": asdict(summary),
        "design_notes": {
            "grain": "grid_cell_key × reference_month",
            "scope": "AB/BC 10km grids, 2016-01 through 2025-12",
            "geometry_method": "polygon_intersection_epsg3347",
            "temporal_method": "polygon fire_year + fire_month; missing/invalid month polygons are excluded from monthly aggregation and counted in summary",
            "join_policy": "Does not depend on silver_wildfire_event point/polygon linkage.",
            "zero_semantics": "Zero wildfire metrics mean no observed NFDB polygon perimeter overlap for that grid-month, not zero physical wildfire risk.",
            "area_semantics": "wildfire_intersection_area_ha is additive across grids/months; wildfire_max_source_size_ha and wildfire_max_calculated_size_ha are non-additive reference metrics.",
        },
    }

    metadata_path = (
        gold_root
        / "_metadata"
        / TARGET_TABLE
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "metadata.json"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    manifest_path = gold_root / "_manifests" / "gold_runs.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    with manifest_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(metadata) + "\n")

    print(
        "[OK] wrote gold_grid_month_wildfire_perimeter_feature | "
        f"rows={summary.output_row_count} "
        f"grid_cells={summary.grid_cell_count} "
        f"months={summary.month_count} "
        f"monthly_assignable_polygons={summary.monthly_assignable_polygon_count} "
        f"nonzero_grid_months={summary.output_nonzero_grid_month_count} "
        f"run_id={run_id}"
    )

    return metadata


def latest_table_path(table_root: Path, table_name: str) -> Path:
    candidates = sorted(table_root.rglob(f"{table_name}.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No parquet files found for {table_name} under {table_root}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    table_name: str,
) -> None:
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise GoldWildfirePerimeterFeatureError(
            f"{table_name} is missing columns: {sorted(missing_columns)}"
        )
