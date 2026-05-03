from src.utils.config import load_project_config


def _sources():
    return load_project_config("source_config.yml")["sources"]


def test_source_level_silver_targets_do_not_point_to_overlay_or_mapping_outputs():
    sources = _sources()

    forbidden_fragments = [
        "_overlay",
        "_map",
        "_mapping",
        "_property_map",
        "_flood_overlay",
        "_parcel_flood_overlay",
    ]

    for source_name, metadata in sources.items():
        target = metadata["target_silver_table"]

        for fragment in forbidden_fragments:
            assert fragment not in target, (
                f"{source_name} target_silver_table points to a downstream/intermediate "
                f"product instead of a source-level Silver table: {target}"
            )


def test_vancouver_floodplain_targets_source_level_silver_table():
    sources = _sources()

    assert sources["vancouver_floodplain"]["target_silver_table"] == "silver_vancouver_floodplain"


def test_vancouver_building_permits_targets_source_level_silver_table():
    sources = _sources()

    assert (
        sources["vancouver_building_permits"]["target_silver_table"]
        == "silver_vancouver_building_permits"
    )


def test_calgary_permit_sources_have_separate_source_level_silver_tables():
    sources = _sources()

    assert (
        sources["calgary_building_permits"]["target_silver_table"]
        == "silver_calgary_building_permits"
    )
    assert (
        sources["calgary_development_permits"]["target_silver_table"]
        == "silver_calgary_development_permits"
    )


def test_census_boundaries_contract_covers_province_and_municipality_outputs():
    sources = _sources()
    census = sources["census_boundaries"]

    assert census["required_fields"] == ["geometry"]
    assert census["target_silver_table"] == "silver_boundary_municipality"
    assert "silver_boundary_province" in census["additional_silver_tables"]

    outputs = set(census["boundary_contract"]["required_boundary_outputs"])
    assert "silver_boundary_province" in outputs
    assert "silver_boundary_municipality" in outputs

    standard_fields = census["boundary_contract"]["required_standard_fields"]
    assert "province_key" in standard_fields["silver_boundary_province"]
    assert "municipality_key" in standard_fields["silver_boundary_municipality"]


def test_boundary_required_fields_do_not_use_silver_only_standard_names():
    sources = _sources()
    census_required = set(sources["census_boundaries"]["required_fields"])

    silver_only_names = {
        "municipality_key",
        "municipality_name",
        "province_key",
        "province_name",
    }

    assert census_required.isdisjoint(silver_only_names)


def test_hydrometric_sources_have_measurement_contracts():
    sources = _sources()

    for source_name in ["eccc_hydrometric_realtime", "hydat_archive"]:
        contract = sources[source_name]["measurement_contract"]

        required = set(contract["required_measurements"])
        assert "discharge" in required
        assert "water_level" in required

        standard_names = contract["downstream_standard_names"]
        assert standard_names["discharge"] == "discharge_cms"
        assert standard_names["water_level"] == "water_level_m"


def test_climate_source_has_climate_measurement_contract():
    sources = _sources()
    contract = sources["eccc_historical_climate"]["climate_measurement_contract"]

    required = set(contract["required_measurements"])
    assert "temperature" in required
    assert "precipitation" in required

    standard_names = contract["downstream_standard_names"]
    assert standard_names["max_temperature"] == "max_temp_c"
    assert standard_names["total_precipitation"] == "total_precip_mm"

    assert (
        "climate_measurement_presence"
        in sources["eccc_historical_climate"]["post_silver_validation_checks"]
    )


def test_statcan_building_permits_has_municipality_mapping_contract():
    sources = _sources()
    source = sources["statcan_building_permits"]

    contract = source["municipality_mapping_contract"]

    assert contract["intended_join_target"] == "census_boundaries"
    assert contract["intended_join_target_silver_table"] == "silver_boundary_municipality"
    assert contract["join_required_for_downstream"] is True
    assert contract["candidate_geography_fields"]
    assert "municipality" in contract["candidate_geography_fields"]
    assert "municipality_boundary_join_coverage" in source["post_silver_validation_checks"]


def test_disaster_database_has_location_mapping_contract():
    sources = _sources()
    contract = sources["canadian_disaster_database"]["location_mapping_contract"]

    assert contract["required_for_downstream_validation"] is True
    assert contract["minimum_geographic_grain"] == "province_plus_location_text"
    assert contract["candidate_location_fields"]
    assert "municipality" in contract["candidate_location_fields"]
    assert "location" in contract["candidate_location_fields"]


def test_vancouver_property_sources_have_join_or_identity_contracts():
    sources = _sources()

    parcel_contract = sources["vancouver_property_parcels"]["identity_contract"]
    assert parcel_contract["primary_entity"] == "parcel"
    assert parcel_contract["stable_id_required"] is True
    assert parcel_contract["candidate_id_fields"]
    assert "parcel_id" not in sources["vancouver_property_parcels"]["required_fields"]

    tax_contract = sources["vancouver_property_tax"]["join_contract"]
    assert tax_contract["intended_join_target"] == "vancouver_property_parcels"
    assert tax_contract["join_required_for_downstream"] is True
    assert tax_contract["candidate_join_keys"]


def test_calgary_property_assessment_has_coordinate_contract():
    sources = _sources()
    source = sources["calgary_property_assessment"]

    assert "coordinate_range" in source["validation_checks"]

    contract = source["coordinate_contract"]
    assert contract["required_for_spatial_overlay"] is True
    assert contract["candidate_latitude_fields"]
    assert contract["candidate_longitude_fields"]
    assert contract["candidate_geometry_fields"]


def test_wildfire_csv_or_geojson_uses_post_silver_geometry_validation():
    sources = _sources()
    wildfire = sources["wildfire_history"]

    assert wildfire["file_format"] == "geojson_or_csv"
    assert "geometry_validity" not in wildfire["validation_checks"]
    assert "geometry_constructed_from_coordinates" in wildfire["post_silver_validation_checks"]
    assert "geometry_validity" in wildfire["post_silver_validation_checks"]


def test_contract_sources_have_post_silver_validation_checks_where_needed():
    sources = _sources()

    expected = [
        "eccc_historical_climate",
        "eccc_hydrometric_realtime",
        "hydat_archive",
        "wildfire_history",
        "statcan_building_permits",
        "census_boundaries",
        "canadian_disaster_database",
        "vancouver_property_parcels",
        "vancouver_property_tax",
        "vancouver_building_permits",
        "calgary_property_assessment",
        "calgary_building_permits",
        "calgary_development_permits",
    ]

    for source_name in expected:
        assert "post_silver_validation_checks" in sources[source_name], (
            f"{source_name} has a contract requiring downstream validation but no "
            "post_silver_validation_checks."
        )
        assert sources[source_name]["post_silver_validation_checks"]
