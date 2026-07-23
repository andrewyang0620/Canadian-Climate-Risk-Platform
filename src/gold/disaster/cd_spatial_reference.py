from __future__ import annotations

import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


AB_BC_PRUID_TO_KEY = {
    "48": "AB",
    "59": "BC",
}

EXPECTED_EPSG = 3347


class GoldDisasterCDSpatialReferenceError(Exception):
    """Raised when Census Division spatial reference build fails."""


@dataclass(frozen=True)
class BoundaryInput:
    shp_path: Path
    prj_text: str | None


def build_gold_disaster_cd_spatial_reference(
    *,
    source_path: str | Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = Path(source_path)

    if not path.exists():
        raise GoldDisasterCDSpatialReferenceError(f"Source path does not exist: {path}")

    if path.suffix.lower() == ".zip":
        with tempfile.TemporaryDirectory() as temp_dir:
            boundary_input = _extract_boundary_zip(path, Path(temp_dir))
            return _build_from_shapefile(
                boundary_input=boundary_input,
                source_path=path,
            )

    boundary_input = _boundary_input_from_shapefile(path)
    return _build_from_shapefile(
        boundary_input=boundary_input,
        source_path=path,
    )


def _build_from_shapefile(
    *,
    boundary_input: BoundaryInput,
    source_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    shapefile = _import_shapefile()

    reader = shapefile.Reader(str(boundary_input.shp_path), encoding="latin1")

    rows: list[dict[str, Any]] = []

    try:
        fields = [
            field[0]
            for field in reader.fields
            if field[0].upper() != "DELETIONFLAG"
        ]

        for shape_record in reader.iterShapeRecords():
            attrs = {
                field.upper(): value
                for field, value in zip(fields, shape_record.record)
            }

            census_division_key = _as_str(_get_attr(attrs, "CDUID"))
            if census_division_key is None:
                continue

            census_division_key = census_division_key.zfill(4)

            province_uid = _as_str(_get_attr(attrs, "PRUID"))
            if province_uid is None:
                province_uid = census_division_key[:2]
            province_uid = province_uid.zfill(2)

            province_key = AB_BC_PRUID_TO_KEY.get(province_uid)

            if province_key not in {"AB", "BC"}:
                continue

            land_area_sq_km = _as_float(_get_attr(attrs, "LANDAREA"))

            geometry_wkt = _shape_to_wkt(shape_record.shape)

            rows.append(
                {
                    "census_division_key": census_division_key,
                    "census_division_name": _as_str(_get_attr(attrs, "CDNAME")),
                    "census_division_type": _as_str(_get_attr(attrs, "CDTYPE")),
                    "dguid": _as_str(_get_attr(attrs, "DGUID")),
                    "province_uid": province_uid,
                    "province_key": province_key,
                    "land_area_sq_km": land_area_sq_km,
                    "geometry_area_m2": (
                        land_area_sq_km * 1_000_000
                        if land_area_sq_km is not None
                        else None
                    ),
                    "geometry_crs_epsg": EXPECTED_EPSG,
                    "geometry_wkt": geometry_wkt,
                }
            )
    finally:
        try:
            reader.close()
        except Exception:
            pass

    result = pd.DataFrame(rows)

    if result.empty:
        raise GoldDisasterCDSpatialReferenceError(
            "No AB/BC Census Division records found."
        )

    result = result.sort_values(
        ["province_key", "census_division_key"]
    ).reset_index(drop=True)

    _validate_basic_result(result)

    summary = {
        "table_name": "gold_disaster_cd_spatial_reference",
        "source_path": str(source_path),
        "source_crs": boundary_input.prj_text,
        "source_epsg": EXPECTED_EPSG,
        "output_crs_epsg": EXPECTED_EPSG,
        "row_count": int(len(result)),
        "province_counts": _value_counts(result["province_key"]),
        "minimum_census_division_key": str(result["census_division_key"].min()),
        "maximum_census_division_key": str(result["census_division_key"].max()),
        "geometry_null_count": int(result["geometry_wkt"].isna().sum()),
        "geometry_area_positive_count": int((result["geometry_area_m2"] > 0).sum()),
        "census_division_keys": result["census_division_key"].tolist(),
    }

    return result, summary


def _extract_boundary_zip(source_path: Path, temp_dir: Path) -> BoundaryInput:
    with zipfile.ZipFile(source_path) as zip_file:
        zip_file.extractall(temp_dir)

    shp_files = list(temp_dir.rglob("*.shp"))

    if not shp_files:
        raise GoldDisasterCDSpatialReferenceError(
            f"No .shp file found inside zip: {source_path}"
        )

    shp_path = shp_files[0]
    prj_path = shp_path.with_suffix(".prj")
    prj_text = prj_path.read_text(encoding="utf-8", errors="ignore") if prj_path.exists() else None

    return BoundaryInput(
        shp_path=shp_path,
        prj_text=prj_text,
    )


def _boundary_input_from_shapefile(source_path: Path) -> BoundaryInput:
    if source_path.suffix.lower() != ".shp":
        raise GoldDisasterCDSpatialReferenceError(
            f"Expected .zip or .shp source path, got: {source_path}"
        )

    prj_path = source_path.with_suffix(".prj")
    prj_text = prj_path.read_text(encoding="utf-8", errors="ignore") if prj_path.exists() else None

    return BoundaryInput(
        shp_path=source_path,
        prj_text=prj_text,
    )


def _shape_to_wkt(shape: Any) -> str:
    geo = shape.__geo_interface__
    geometry_type = geo.get("type")
    coordinates = geo.get("coordinates")

    if geometry_type == "Polygon":
        return f"POLYGON {_polygon_to_wkt(coordinates)}"

    if geometry_type == "MultiPolygon":
        polygons = ", ".join(_polygon_to_wkt(polygon) for polygon in coordinates)
        return f"MULTIPOLYGON ({polygons})"

    raise GoldDisasterCDSpatialReferenceError(
        f"Unsupported geometry type: {geometry_type}"
    )


def _polygon_to_wkt(polygon_coordinates: Any) -> str:
    rings = ", ".join(_ring_to_wkt(ring) for ring in polygon_coordinates)
    return f"({rings})"


def _ring_to_wkt(ring_coordinates: Any) -> str:
    points = [(float(point[0]), float(point[1])) for point in ring_coordinates]

    if not points:
        raise GoldDisasterCDSpatialReferenceError("Encountered empty polygon ring.")

    if points[0] != points[-1]:
        points.append(points[0])

    point_text = ", ".join(f"{x:.6f} {y:.6f}" for x, y in points)
    return f"({point_text})"


def _get_attr(attrs: dict[str, Any], name: str) -> Any:
    return attrs.get(name.upper())


def _as_str(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


def _as_float(value: Any) -> float | None:
    if value is None:
        return None

    text = str(value).strip().replace(",", "")

    if text == "":
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _validate_basic_result(dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        raise GoldDisasterCDSpatialReferenceError("Output table is empty.")

    if dataframe["census_division_key"].isna().any():
        raise GoldDisasterCDSpatialReferenceError(
            "census_division_key contains nulls."
        )

    if dataframe["census_division_key"].duplicated().any():
        raise GoldDisasterCDSpatialReferenceError(
            "census_division_key contains duplicates."
        )

    invalid_provinces = (
        set(dataframe["province_key"].dropna().astype(str)) - {"AB", "BC"}
    )
    if invalid_provinces:
        raise GoldDisasterCDSpatialReferenceError(
            f"Invalid province_key values: {sorted(invalid_provinces)}"
        )

    if dataframe["geometry_wkt"].isna().any():
        raise GoldDisasterCDSpatialReferenceError("geometry_wkt contains nulls.")

    if not (dataframe["geometry_area_m2"] > 0).all():
        raise GoldDisasterCDSpatialReferenceError(
            "geometry_area_m2 must be positive."
        )


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in series.value_counts(dropna=False).to_dict().items()
    }


def _import_shapefile():
    try:
        import shapefile
    except ImportError as exc:
        raise GoldDisasterCDSpatialReferenceError(
            "pyshp is required to build gold_disaster_cd_spatial_reference. "
            "Install it with: python -m pip install pyshp"
        ) from exc

    return shapefile
