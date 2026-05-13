import json

from src.profiling.source_profiler import BronzeSourceProfiler, check_source_contracts


def make_manifest_record(source_name, raw_file_path, metadata_path):
    return {
        "run_id": f"{source_name}-run-1",
        "source_name": source_name,
        "source_group": "municipal",
        "provider": "Test Provider",
        "extract_timestamp": "2026-04-29T10:00:00+00:00",
        "extract_date": "2026-04-29",
        "raw_file_path": str(raw_file_path),
        "metadata_path": str(metadata_path),
        "file_name": raw_file_path.name,
        "file_size_bytes": raw_file_path.stat().st_size,
        "file_checksum": "abc123",
        "checksum_algorithm": "sha256",
        "ingestion_method": "test",
        "row_count": None,
        "target_bronze_table": f"bronze_{source_name}",
        "target_silver_table": f"silver_{source_name}",
        "load_status": "success",
        "manifest_record_created_at": "2026-04-29T10:01:00+00:00",
    }


def write_jsonl(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def test_profiler_profiles_csv_and_detects_join_contract(tmp_path):
    raw_file = tmp_path / "source.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    raw_file.write_text(
        "pid,total_value,property_tax_year\n123,1000000,2025\n",
        encoding="utf-8",
    )
    metadata_file.write_text("{}", encoding="utf-8")

    write_jsonl(
        manifest_file,
        [make_manifest_record("vancouver_property_tax", raw_file, metadata_file)],
    )

    source_config = {
        "sources": {
            "vancouver_property_tax": {
                "source_group": "municipal",
                "required_fields": ["property_tax_year", "total_value"],
                "join_contract": {
                    "candidate_join_keys": ["pid", "parcel_id"],
                },
            }
        }
    }

    profiler = BronzeSourceProfiler(
        manifest_path=manifest_file,
        source_config=source_config,
        count_rows=True,
    )

    report = profiler.run()
    item = report["sources"][0]

    assert item["status"] == "profiled"
    assert item["profile"]["file_type"] == "csv"
    assert item["profile"]["row_count_exact"] == 1
    assert item["contract_checks"]["required_fields"]["passed"] is True
    assert item["contract_checks"]["join_contract"]["passed"] is True
    assert item["contract_checks"]["join_contract"]["found"] == ["pid"]


def test_profiler_profiles_geojson_and_detects_identity_contract(tmp_path):
    raw_file = tmp_path / "parcels.geojson"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    raw_file.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "pid": "123",
                            "legal_description": "LOT 1",
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    metadata_file.write_text("{}", encoding="utf-8")

    write_jsonl(
        manifest_file,
        [make_manifest_record("vancouver_property_parcels", raw_file, metadata_file)],
    )

    source_config = {
        "sources": {
            "vancouver_property_parcels": {
                "source_group": "municipal",
                "required_fields": ["geometry"],
                "identity_contract": {
                    "candidate_id_fields": ["parcel_id", "pid"],
                },
            }
        }
    }

    profiler = BronzeSourceProfiler(
        manifest_path=manifest_file,
        source_config=source_config,
    )

    report = profiler.run()
    item = report["sources"][0]

    assert item["status"] == "profiled"
    assert item["profile"]["file_type"] == "geojson"
    assert item["profile"]["feature_count"] == 1
    assert item["contract_checks"]["required_fields"]["passed"] is True
    assert item["contract_checks"]["identity_contract"]["passed"] is True
    assert item["contract_checks"]["identity_contract"]["found"] == ["pid"]


def test_profiler_marks_missing_bronze_run(tmp_path):
    manifest_file = tmp_path / "bronze_runs.jsonl"
    write_jsonl(manifest_file, [])

    source_config = {
        "sources": {
            "missing_source": {
                "source_group": "municipal",
                "required_fields": ["id"],
            }
        }
    }

    profiler = BronzeSourceProfiler(
        manifest_path=manifest_file,
        source_config=source_config,
    )

    report = profiler.run()

    assert report["missing_bronze_run_count"] == 1
    assert report["sources"][0]["status"] == "missing_bronze_run"


def test_check_source_contracts_detects_coordinate_contract():
    file_profile = {
        "columns": ["ADDRESS", "LATITUDE", "LONGITUDE"],
    }

    source_metadata = {
        "required_fields": ["ADDRESS"],
        "coordinate_contract": {
            "candidate_latitude_fields": ["latitude", "lat"],
            "candidate_longitude_fields": ["longitude", "lon"],
            "candidate_geometry_fields": ["location", "geometry"],
        },
    }

    checks = check_source_contracts(
        source_metadata=source_metadata,
        file_profile=file_profile,
    )

    assert checks["required_fields"]["passed"] is True
    assert checks["coordinate_contract"]["passed"] is True
    assert checks["coordinate_contract"]["latitude"]["found"] == ["latitude"]
    assert checks["coordinate_contract"]["longitude"]["found"] == ["longitude"]


def test_check_source_contracts_detects_climate_measurement_contract():
    file_profile = {
        "columns": ["station_id", "date", "MAX_TEMP", "TOTAL_PRECIP"],
    }

    source_metadata = {
        "required_fields": ["station_id", "date"],
        "climate_measurement_contract": {
            "candidate_raw_fields": {
                "temperature": ["MAX_TEMP", "MIN_TEMP"],
                "precipitation": ["TOTAL_PRECIP"],
            }
        },
    }

    checks = check_source_contracts(
        source_metadata=source_metadata,
        file_profile=file_profile,
    )

    assert checks["required_fields"]["passed"] is True
    assert checks["climate_measurement_contract"]["passed"] is True
    assert checks["climate_measurement_contract"]["groups"]["temperature"]["found"] == ["MAX_TEMP"]
    assert checks["climate_measurement_contract"]["groups"]["precipitation"]["found"] == [
        "TOTAL_PRECIP"
    ]


def test_measurement_contract_without_raw_candidates_is_marked_post_silver_only():
    file_profile = {
        "columns": ["station_id", "date"],
    }

    source_metadata = {
        "required_fields": ["station_id", "date"],
        "measurement_contract": {
            "required_measurements": ["discharge", "water_level"],
            "validation_rule": "at_least_one_measurement_present_after_silver_standardization",
        },
    }

    checks = check_source_contracts(
        source_metadata=source_metadata,
        file_profile=file_profile,
    )

    assert checks["required_fields"]["passed"] is True
    assert checks["measurement_contract"]["passed"] is None


def test_profiler_detects_semicolon_delimited_csv(tmp_path):
    raw_file = tmp_path / "semicolon.csv"
    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "bronze_runs.jsonl"

    raw_file.write_text(
        "pid;total_value;property_tax_year\n123;1000000;2025\n",
        encoding="utf-8",
    )
    metadata_file.write_text("{}", encoding="utf-8")

    write_jsonl(
        manifest_file,
        [make_manifest_record("vancouver_property_tax", raw_file, metadata_file)],
    )

    source_config = {
        "sources": {
            "vancouver_property_tax": {
                "source_group": "municipal",
                "required_fields": ["property_tax_year", "total_value"],
                "join_contract": {
                    "candidate_join_keys": ["pid", "parcel_id"],
                },
            }
        }
    }

    profiler = BronzeSourceProfiler(
        manifest_path=manifest_file,
        source_config=source_config,
        count_rows=True,
    )

    report = profiler.run()
    item = report["sources"][0]

    assert item["status"] == "profiled"
    assert item["profile"]["delimiter"] == ";"
    assert item["profile"]["column_count"] == 3
    assert item["contract_checks"]["required_fields"]["passed"] is True
    assert item["contract_checks"]["join_contract"]["passed"] is True
