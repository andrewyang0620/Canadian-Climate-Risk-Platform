from src.ingestion.source_registry import SourceRegistry


EXPECTED_PRIMARY_SILVER_OUTPUTS = {
    "calgary_building_permits": "silver_building_permit",
    "calgary_development_permits": "silver_development_permit",
    "calgary_flood_hazard": "silver_flood_hazard_zone",
    "calgary_property_assessment": "silver_property_assessment",
    "canadian_disaster_database": "silver_disaster_event_month",
    "census_boundaries": "silver_boundary_municipality",
    "eccc_historical_climate": "silver_climate_daily",
    "eccc_hydrometric_realtime": "silver_hydro_realtime_observation",
    "hydat_archive": "silver_hydro_daily",
    "statcan_building_permits": "silver_permit_monthly",
    "vancouver_building_permits": "silver_building_permit",
    "vancouver_floodplain": "silver_flood_hazard_zone",
    "vancouver_property_parcels": "silver_property_parcel",
    "vancouver_property_tax": "silver_property_tax_assessment",
    "wildfire_history": "silver_wildfire_event",
}


EXPECTED_SILVER_OUTPUTS = {
    "calgary_building_permits": {
        "silver_building_permit",
    },
    "calgary_development_permits": {
        "silver_development_permit",
    },
    "calgary_flood_hazard": {
        "silver_flood_hazard_zone",
    },
    "calgary_property_assessment": {
        "silver_property_assessment",
    },
    "canadian_disaster_database": {
        "silver_disaster_event_month",
    },
    "census_boundaries": {
        "silver_boundary_province",
        "silver_boundary_municipality",
    },
    "eccc_historical_climate": {
        "silver_climate_daily",
    },
    "eccc_hydrometric_realtime": {
        "silver_hydro_realtime_observation",
    },
    "hydat_archive": {
        "silver_hydro_daily",
        "silver_hydro_station",
    },
    "statcan_building_permits": {
        "silver_permit_monthly",
    },
    "vancouver_building_permits": {
        "silver_building_permit",
    },
    "vancouver_floodplain": {
        "silver_flood_hazard_zone",
    },
    "vancouver_property_parcels": {
        "silver_property_parcel",
    },
    "vancouver_property_tax": {
        "silver_property_tax_assessment",
    },
    "wildfire_history": {
        "silver_wildfire_event",
    },
}


def test_all_configured_sources_have_explicit_contract_expectations():
    registry = SourceRegistry()

    assert set(registry.list_sources()) == set(EXPECTED_PRIMARY_SILVER_OUTPUTS)
    assert set(registry.list_sources()) == set(EXPECTED_SILVER_OUTPUTS)


def test_primary_silver_outputs_match_real_implementation():
    registry = SourceRegistry()

    for source_name, expected_primary in EXPECTED_PRIMARY_SILVER_OUTPUTS.items():
        source = registry.get_source(source_name)

        assert source.target_silver_table == expected_primary


def test_complete_silver_outputs_match_real_implementation():
    registry = SourceRegistry()

    for source_name, expected_outputs in EXPECTED_SILVER_OUTPUTS.items():
        source = registry.get_source(source_name)

        assert set(source.target_silver_tables) == expected_outputs


def test_primary_output_is_in_complete_output_contract():
    registry = SourceRegistry()

    for source in registry.sources.values():
        assert source.target_silver_table in source.target_silver_tables


def test_shared_municipal_sources_declare_shared_tables():
    registry = SourceRegistry()

    assert registry.silver_outputs_for("calgary_building_permits") == ["silver_building_permit"]

    assert registry.silver_outputs_for("vancouver_building_permits") == ["silver_building_permit"]

    assert registry.silver_outputs_for("calgary_flood_hazard") == ["silver_flood_hazard_zone"]

    assert registry.silver_outputs_for("vancouver_floodplain") == ["silver_flood_hazard_zone"]


def test_hydat_primary_output_is_daily_fact_table():
    registry = SourceRegistry()
    source = registry.get_source("hydat_archive")

    assert source.target_silver_table == "silver_hydro_daily"
    assert source.target_silver_tables == [
        "silver_hydro_daily",
        "silver_hydro_station",
    ]


def test_registry_includes_all_declared_silver_outputs():
    registry = SourceRegistry()

    expected_tables = {
        table_name for outputs in EXPECTED_SILVER_OUTPUTS.values() for table_name in outputs
    }

    assert registry.silver_tables() == expected_tables
