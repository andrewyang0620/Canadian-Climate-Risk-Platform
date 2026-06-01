from __future__ import annotations

import json
import math
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from src.silver.common import (
    SilverRunResult,
    append_jsonl,
    file_sha256,
    latest_successful_bronze_raw_path,
    utc_now_iso,
    utc_today,
    write_json,
    write_parquet,
)


CALGARY_SOURCE_NAME = "calgary_building_permits"
VANCOUVER_SOURCE_NAME = "vancouver_building_permits"


def run_municipal_building_permit_silver(
    *,
    bronze_manifest_path: str | Path = "lakehouse/bronze/_manifests/bronze_runs.jsonl",
    output_root: str | Path = "lakehouse/silver",
    silver_manifest_path: str | Path = "lakehouse/silver/_manifests/silver_runs.jsonl",
) -> SilverRunResult:
    source_name = "municipal_building_permit"

    calgary_raw_path = latest_successful_bronze_raw_path(
        source_name=CALGARY_SOURCE_NAME,
        manifest_path=bronze_manifest_path,
    )
    vancouver_raw_path = latest_successful_bronze_raw_path(
        source_name=VANCOUVER_SOURCE_NAME,
        manifest_path=bronze_manifest_path,
    )

    run_id = str(uuid.uuid4())
    extract_date = utc_today()
    extract_timestamp = utc_now_iso()
    output_root = Path(output_root)

    calgary_df = standardize_calgary_building_permits(calgary_raw_path)
    vancouver_df = standardize_vancouver_building_permits(vancouver_raw_path)

    dataframe = pd.concat([calgary_df, vancouver_df], ignore_index=True)

    if dataframe.empty:
        raise RuntimeError("Municipal building permit Silver produced zero rows.")

    dataframe["estimated_project_cost"] = dataframe["estimated_project_cost"].map(
        safe_non_negative_float
    )

    dataframe = clean_building_permit_coordinates(dataframe)

    dataframe = deduplicate_building_permits(dataframe)
    dataframe = dataframe.sort_values(
        ["city", "issue_date", "building_permit_key"],
        na_position="last",
    ).reset_index(drop=True)

    output_path = (
        output_root
        / "silver_building_permit"
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "silver_building_permit.parquet"
    )

    write_parquet(output_path, dataframe)

    output_tables = [
        table_output_metadata(
            table_name="silver_building_permit",
            path=output_path,
            dataframe=dataframe,
            source_raw_files=[calgary_raw_path, vancouver_raw_path],
        )
    ]

    metadata = {
        "run_id": run_id,
        "source_name": source_name,
        "extract_date": extract_date,
        "extract_timestamp": extract_timestamp,
        "silver_layer": "municipal_building_permit_standardization",
        "load_status": "success",
        "target_tables": ["silver_building_permit"],
        "output_tables": output_tables,
        "source_inputs": [
            {
                "source_name": CALGARY_SOURCE_NAME,
                "city": "calgary",
                "raw_file_path": calgary_raw_path.as_posix(),
                "raw_file_checksum": file_sha256(calgary_raw_path),
                "standardized_row_count": int(len(calgary_df)),
            },
            {
                "source_name": VANCOUVER_SOURCE_NAME,
                "city": "vancouver",
                "raw_file_path": vancouver_raw_path.as_posix(),
                "raw_file_checksum": file_sha256(vancouver_raw_path),
                "standardized_row_count": int(len(vancouver_df)),
            },
        ],
        "row_count": int(len(dataframe)),
        "cities": sorted(dataframe["city"].dropna().unique().tolist()),
        "issue_year_min": safe_int(dataframe["issue_year"].min()),
        "issue_year_max": safe_int(dataframe["issue_year"].max()),
        "standardization_notes": {
            "grain": "One row per municipal building permit record.",
            "identity": "building_permit_key is based on city + source permit id.",
            "vancouver_csv": "Vancouver source is semicolon-delimited and includes multiline quoted text fields.",
            "geometry": "Point WKT is retained where available; latitude and longitude are standardized to numeric columns.",
        },
    }

    metadata_path = (
        output_root
        / "_metadata"
        / source_name
        / f"extract_date={extract_date}"
        / f"run_id={run_id}"
        / "metadata.json"
    )

    write_json(metadata_path, metadata)

    manifest_record = {
        **metadata,
        "metadata_path": metadata_path.as_posix(),
        "manifest_record_created_at": utc_now_iso(),
    }

    append_jsonl(silver_manifest_path, manifest_record)

    print(
        "[OK] wrote municipal building permit Silver outputs | "
        f"rows={len(dataframe)} cities={metadata['cities']} "
        f"years={metadata['issue_year_min']}-{metadata['issue_year_max']} "
        f"run_id={run_id}"
    )

    return SilverRunResult(
        source_name=source_name,
        run_id=run_id,
        extract_date=extract_date,
        output_tables=output_tables,
        metadata_path=metadata_path.as_posix(),
    )


def standardize_calgary_building_permits(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype=object, low_memory=False)
    raw.columns = [normalize_name(column) for column in raw.columns]

    required = {"permitnum", "applieddate", "originaladdress", "latitude", "longitude"}
    missing = required - set(raw.columns)

    if missing:
        raise ValueError(f"Calgary building permits missing columns: {sorted(missing)}")

    dataframe = pd.DataFrame(
        {
            "building_permit_key": "calgary_" + raw["permitnum"].astype(str),
            "city": "calgary",
            "source_permit_id": raw["permitnum"].map(clean_str),
            "permit_number": raw["permitnum"].map(clean_str),
            "permit_status": (
                raw.get("statuscurrent").map(clean_str) if "statuscurrent" in raw.columns else None
            ),
            "permit_type": (
                raw.get("permittype").map(clean_str) if "permittype" in raw.columns else None
            ),
            "permit_type_mapped": (
                raw.get("permittypemapped").map(clean_str)
                if "permittypemapped" in raw.columns
                else None
            ),
            "permit_class": (
                raw.get("permitclass").map(clean_str) if "permitclass" in raw.columns else None
            ),
            "permit_class_group": (
                raw.get("permitclassgroup").map(clean_str)
                if "permitclassgroup" in raw.columns
                else None
            ),
            "permit_class_mapped": (
                raw.get("permitclassmapped").map(clean_str)
                if "permitclassmapped" in raw.columns
                else None
            ),
            "work_class": (
                raw.get("workclass").map(clean_str) if "workclass" in raw.columns else None
            ),
            "work_class_group": (
                raw.get("workclassgroup").map(clean_str)
                if "workclassgroup" in raw.columns
                else None
            ),
            "work_class_mapped": (
                raw.get("workclassmapped").map(clean_str)
                if "workclassmapped" in raw.columns
                else None
            ),
            "application_date": raw["applieddate"].map(parse_date),
            "issue_date": (
                raw.get("issueddate").map(parse_date) if "issueddate" in raw.columns else None
            ),
            "completed_date": (
                raw.get("completeddate").map(parse_date) if "completeddate" in raw.columns else None
            ),
            "issue_year": (
                raw.get("issueddate").map(parse_year) if "issueddate" in raw.columns else None
            ),
            "year_month": (
                raw.get("issueddate").map(parse_year_month) if "issueddate" in raw.columns else None
            ),
            "address_text": raw["originaladdress"].map(clean_str),
            "project_description": (
                raw.get("description").map(clean_str) if "description" in raw.columns else None
            ),
            "applicant_name": (
                raw.get("applicantname").map(clean_str) if "applicantname" in raw.columns else None
            ),
            "contractor_name": (
                raw.get("contractorname").map(clean_str)
                if "contractorname" in raw.columns
                else None
            ),
            "housing_units": (
                raw.get("housingunits").map(safe_int) if "housingunits" in raw.columns else None
            ),
            "estimated_project_cost": (
                raw.get("estprojectcost").map(safe_float)
                if "estprojectcost" in raw.columns
                else None
            ),
            "total_sqft": (
                raw.get("totalsqft").map(safe_float) if "totalsqft" in raw.columns else None
            ),
            "neighbourhood_code": (
                raw.get("communitycode").map(clean_str) if "communitycode" in raw.columns else None
            ),
            "neighbourhood_name": (
                raw.get("communityname").map(clean_str) if "communityname" in raw.columns else None
            ),
            "latitude": raw["latitude"].map(safe_float),
            "longitude": raw["longitude"].map(safe_float),
            "geometry_type": "Point",
            "geometry_wkt": raw.get("point").map(clean_str) if "point" in raw.columns else None,
            "source_locations_wkt": (
                raw.get("locationswkt").map(clean_str) if "locationswkt" in raw.columns else None
            ),
            "source_name": CALGARY_SOURCE_NAME,
        }
    )

    dataframe["geometry_wkt"] = dataframe.apply(
        lambda row: row["geometry_wkt"] or point_wkt(row["longitude"], row["latitude"]),
        axis=1,
    )

    return dataframe


def standardize_vancouver_building_permits(path: str | Path) -> pd.DataFrame:
    raw = read_vancouver_building_permits_csv(path)
    raw.columns = [normalize_name(column) for column in raw.columns]

    required = {"permitnumber", "permitnumbercreateddate", "address"}
    missing = required - set(raw.columns)

    if missing:
        raise ValueError(f"Vancouver building permits missing columns: {sorted(missing)}")

    lat_lon = (
        raw.get("geo_point_2d").map(parse_vancouver_geo_point)
        if "geo_point_2d" in raw.columns
        else None
    )

    dataframe = pd.DataFrame(
        {
            "building_permit_key": "vancouver_" + raw["permitnumber"].astype(str),
            "city": "vancouver",
            "source_permit_id": raw["permitnumber"].map(clean_str),
            "permit_number": raw["permitnumber"].map(clean_str),
            "permit_status": None,
            "permit_type": (
                raw.get("typeofwork").map(clean_str) if "typeofwork" in raw.columns else None
            ),
            "permit_type_mapped": (
                raw.get("typeofwork").map(clean_str) if "typeofwork" in raw.columns else None
            ),
            "permit_class": (
                raw.get("permitcategory").map(clean_str)
                if "permitcategory" in raw.columns
                else None
            ),
            "permit_class_group": (
                raw.get("propertyuse").map(clean_str) if "propertyuse" in raw.columns else None
            ),
            "permit_class_mapped": (
                raw.get("specificusecategory").map(clean_str)
                if "specificusecategory" in raw.columns
                else None
            ),
            "work_class": (
                raw.get("typeofwork").map(clean_str) if "typeofwork" in raw.columns else None
            ),
            "work_class_group": (
                raw.get("permitcategory").map(clean_str)
                if "permitcategory" in raw.columns
                else None
            ),
            "work_class_mapped": (
                raw.get("permitcategory").map(clean_str)
                if "permitcategory" in raw.columns
                else None
            ),
            "application_date": raw["permitnumbercreateddate"].map(parse_date),
            "issue_date": (
                raw.get("issuedate").map(parse_date) if "issuedate" in raw.columns else None
            ),
            "completed_date": None,
            "issue_year": (
                raw.get("issueyear").map(safe_int) if "issueyear" in raw.columns else None
            ),
            "year_month": (
                raw.get("yearmonth").map(clean_str) if "yearmonth" in raw.columns else None
            ),
            "address_text": raw["address"].map(clean_str),
            "project_description": (
                raw.get("projectdescription").map(clean_str)
                if "projectdescription" in raw.columns
                else None
            ),
            "applicant_name": (
                raw.get("applicant").map(clean_str) if "applicant" in raw.columns else None
            ),
            "contractor_name": (
                raw.get("buildingcontractor").map(clean_str)
                if "buildingcontractor" in raw.columns
                else None
            ),
            "housing_units": None,
            "estimated_project_cost": (
                raw.get("projectvalue").map(safe_float) if "projectvalue" in raw.columns else None
            ),
            "total_sqft": None,
            "neighbourhood_code": None,
            "neighbourhood_name": (
                raw.get("geolocalarea").map(clean_str) if "geolocalarea" in raw.columns else None
            ),
            "latitude": (
                lat_lon.map(lambda value: value[0] if value else None)
                if lat_lon is not None
                else None
            ),
            "longitude": (
                lat_lon.map(lambda value: value[1] if value else None)
                if lat_lon is not None
                else None
            ),
            "geometry_type": "Point",
            "geometry_wkt": (
                raw.get("geom").map(geom_json_to_point_wkt) if "geom" in raw.columns else None
            ),
            "source_locations_wkt": None,
            "source_name": VANCOUVER_SOURCE_NAME,
        }
    )

    dataframe["geometry_wkt"] = dataframe.apply(
        lambda row: row["geometry_wkt"] or point_wkt(row["longitude"], row["latitude"]),
        axis=1,
    )

    return dataframe


def read_vancouver_building_permits_csv(path: str | Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            sep=";",
            dtype=object,
            encoding="utf-8-sig",
            low_memory=False,
        )
    except Exception:
        return pd.read_csv(
            path,
            sep=";",
            dtype=object,
            encoding="utf-8-sig",
            engine="python",
        )


CITY_COORDINATE_BOUNDS = {
    "calgary": {
        "latitude_min": 50.8,
        "latitude_max": 51.3,
        "longitude_min": -114.4,
        "longitude_max": -113.7,
    },
    "vancouver": {
        "latitude_min": 49.0,
        "latitude_max": 49.4,
        "longitude_min": -123.4,
        "longitude_max": -122.8,
    },
}


def clean_building_permit_coordinates(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Null out coordinates outside expected city envelopes.

    The permit row is retained because it is still useful for non-spatial
    permit activity features. Invalid coordinates should not enter spatial joins.
    """
    working = dataframe.copy()

    for city, bounds in CITY_COORDINATE_BOUNDS.items():
        city_mask = working["city"] == city
        has_coordinates = working["latitude"].notna() & working["longitude"].notna()

        valid_coordinates = (
            (working["latitude"] >= bounds["latitude_min"])
            & (working["latitude"] <= bounds["latitude_max"])
            & (working["longitude"] >= bounds["longitude_min"])
            & (working["longitude"] <= bounds["longitude_max"])
        )

        invalid_mask = city_mask & has_coordinates & ~valid_coordinates

        working.loc[invalid_mask, "latitude"] = None
        working.loc[invalid_mask, "longitude"] = None
        working.loc[invalid_mask, "geometry_wkt"] = None

    return working


def deduplicate_building_permits(dataframe: pd.DataFrame) -> pd.DataFrame:
    working = dataframe.copy()

    working["_source_record_count"] = working.groupby("building_permit_key")[
        "building_permit_key"
    ].transform("size")

    working["_quality_score"] = (
        working["issue_date"].notna().astype(int)
        + working["address_text"].notna().astype(int)
        + working["geometry_wkt"].notna().astype(int)
        + working["estimated_project_cost"].notna().astype(int)
    )

    working = working.sort_values(
        ["building_permit_key", "_quality_score"],
        ascending=[True, False],
        na_position="last",
    )

    deduped = working.drop_duplicates(
        subset=["building_permit_key"],
        keep="first",
    ).copy()

    deduped["source_record_count"] = deduped["_source_record_count"].astype(int)

    return deduped.drop(columns=["_source_record_count", "_quality_score"])


def parse_vancouver_geo_point(value: Any) -> tuple[float, float] | None:
    text = clean_str(value)

    if text is None or "," not in text:
        return None

    parts = [part.strip() for part in text.split(",")]

    if len(parts) != 2:
        return None

    latitude = safe_float(parts[0])
    longitude = safe_float(parts[1])

    if latitude is None or longitude is None:
        return None

    return latitude, longitude


def geom_json_to_point_wkt(value: Any) -> str | None:
    text = clean_str(value)

    if text is None:
        return None

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None

    if payload.get("type") != "Point":
        return None

    coordinates = payload.get("coordinates") or []

    if len(coordinates) < 2:
        return None

    longitude = safe_float(coordinates[0])
    latitude = safe_float(coordinates[1])

    return point_wkt(longitude, latitude)


def point_wkt(longitude: Any, latitude: Any) -> str | None:
    lon = safe_float(longitude)
    lat = safe_float(latitude)

    if lon is None or lat is None:
        return None

    return f"POINT ({lon} {lat})"


def parse_date(value: Any) -> str | None:
    text = clean_str(value)

    if text is None:
        return None

    parsed = pd.to_datetime(text, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date().isoformat()


def parse_year(value: Any) -> int | None:
    date = parse_date(value)

    if date is None:
        return None

    return int(date[:4])


def parse_year_month(value: Any) -> str | None:
    date = parse_date(value)

    if date is None:
        return None

    return date[:7]


def normalize_name(value: str) -> str:
    return str(value).strip().lower()


def clean_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()

    if text == "":
        return None

    return text


def safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None

    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None

    if not math.isfinite(number):
        return None

    return number


def safe_non_negative_float(value: Any) -> float | None:
    number = safe_float(value)

    if number is None:
        return None

    if number < 0:
        return None

    return number


def safe_int(value: Any) -> int | None:
    number = safe_float(value)

    if number is None:
        return None

    return int(number)


def table_output_metadata(
    *,
    table_name: str,
    path: Path,
    dataframe: pd.DataFrame,
    source_raw_files: list[Path],
) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "file_path": path.as_posix(),
        "file_name": path.name,
        "file_size_bytes": path.stat().st_size,
        "file_checksum": file_sha256(path),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": list(dataframe.columns),
        "source_raw_files": [
            {
                "path": source_raw_file.as_posix(),
                "checksum": file_sha256(source_raw_file),
            }
            for source_raw_file in source_raw_files
        ],
    }
