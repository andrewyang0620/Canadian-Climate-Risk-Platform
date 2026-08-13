from __future__ import annotations

from pathlib import Path

import geopandas as gpd


SOURCE_PATH = Path(
    "dashboard/gis/data/grid_geometry.geojson"
)

OUTPUT_PATH = Path(
    "dashboard/gis/data/grid_geometry.fgb"
)

EXPECTED_GRID_COUNT = 16_508
EXPECTED_CRS = 4326


class WebGeometryError(ValueError):
    """Raised when optimized web geometry violates the GIS contract."""


def build_web_geometry(
    source_path: str | Path = SOURCE_PATH,
    output_path: str | Path = OUTPUT_PATH,
) -> dict[str, object]:
    source_path = Path(source_path)
    output_path = Path(output_path)

    if not source_path.exists():
        raise WebGeometryError(
            f"Source geometry does not exist: {source_path}"
        )

    source = gpd.read_file(
        source_path
    )

    required_columns = {
        "grid_cell_key",
        "province_key",
        "geometry",
    }

    missing_columns = (
        required_columns
        - set(source.columns)
    )

    if missing_columns:
        raise WebGeometryError(
            "Source geometry is missing columns: "
            f"{sorted(missing_columns)}"
        )

    if len(source) != EXPECTED_GRID_COUNT:
        raise WebGeometryError(
            "Unexpected source grid count: "
            f"{len(source)}"
        )

    if (
        source.crs is None
        or source.crs.to_epsg()
        != EXPECTED_CRS
    ):
        raise WebGeometryError(
            f"Expected EPSG:{EXPECTED_CRS}, "
            f"got {source.crs}."
        )

    if source["grid_cell_key"].duplicated().any():
        raise WebGeometryError(
            "Source geometry contains duplicate grid keys."
        )

    if not source.geometry.is_valid.all():
        invalid_count = int(
            (~source.geometry.is_valid).sum()
        )

        raise WebGeometryError(
            "Source geometry contains "
            f"{invalid_count} invalid features."
        )

    serving = source[
        [
            "grid_cell_key",
            "province_key",
            "geometry",
        ]
    ].copy()

    if output_path.exists():
        output_path.unlink()

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    serving.to_file(
        output_path,
        driver="FlatGeobuf",
        engine="fiona",
        SPATIAL_INDEX="YES",
    )

    _validate_round_trip(
        source=serving,
        output_path=output_path,
    )

    return {
        "source_path": source_path.as_posix(),
        "output_path": output_path.as_posix(),
        "grid_cell_count": len(serving),
        "crs": "EPSG:4326",
        "source_size_mb": round(
            source_path.stat().st_size
            / 1024
            / 1024,
            2,
        ),
        "output_size_mb": round(
            output_path.stat().st_size
            / 1024
            / 1024,
            2,
        ),
        "geometry_precision": "unchanged",
        "status": "passed",
    }


def _validate_round_trip(
    *,
    source: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    round_trip = gpd.read_file(
        output_path
    )

    if len(round_trip) != len(source):
        raise WebGeometryError(
            "FlatGeobuf row count differs from GeoJSON."
        )

    if (
        round_trip.crs is None
        or round_trip.crs.to_epsg()
        != EXPECTED_CRS
    ):
        raise WebGeometryError(
            "FlatGeobuf CRS differs from source."
        )

    source_sorted = (
        source
        .sort_values(
            "grid_cell_key"
        )
        .reset_index(drop=True)
    )

    round_trip_sorted = (
        round_trip
        .sort_values(
            "grid_cell_key"
        )
        .reset_index(drop=True)
    )

    if not source_sorted[
        "grid_cell_key"
    ].equals(
        round_trip_sorted[
            "grid_cell_key"
        ]
    ):
        raise WebGeometryError(
            "FlatGeobuf grid keys differ from source."
        )

    if not source_sorted[
        "province_key"
    ].equals(
        round_trip_sorted[
            "province_key"
        ]
    ):
        raise WebGeometryError(
            "FlatGeobuf province keys differ from source."
        )

    mismatches: list[str] = []

    for source_row, output_row in zip(
        source_sorted.itertuples(),
        round_trip_sorted.itertuples(),
    ):
        source_geometry = (
            source_row.geometry.normalize()
        )

        output_geometry = (
            output_row.geometry.normalize()
        )

        if not source_geometry.equals_exact(
            output_geometry,
            tolerance=0.0,
        ):
            mismatches.append(
                source_row.grid_cell_key
            )

            if len(mismatches) >= 10:
                break

    if mismatches:
        raise WebGeometryError(
            "FlatGeobuf geometry differs from source "
            "at exact coordinate precision. "
            f"Examples: {mismatches}"
        )

    if not round_trip.geometry.is_valid.all():
        raise WebGeometryError(
            "FlatGeobuf contains invalid geometry."
        )


if __name__ == "__main__":
    result = build_web_geometry()

    print(
        "[OK] web geometry build complete"
    )

    for key, value in result.items():
        print(
            f"{key}={value}"
        )