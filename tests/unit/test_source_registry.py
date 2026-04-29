import pytest

from src.ingestion.source_registry import SourceRegistry
from src.utils.config import ConfigError, load_project_config


def test_project_scope_config_loads():
    config = load_project_config("project_scope.yml")

    assert config["project"]["name"] == "canadian_climate_property_risk_platform"
    assert config["geographic_scope"]["provinces"]
    assert config["spatial_units"]["province_grid"]["resolution_m"] == 10000


def test_source_registry_loads_sources():
    registry = SourceRegistry()

    sources = registry.list_sources()

    assert sources
    assert "eccc_historical_climate" in sources
    assert "canadian_disaster_database" in sources
    assert "vancouver_property_parcels" in sources
    assert "calgary_flood_hazard" in sources


def test_source_registry_required_source_fields():
    registry = SourceRegistry()

    for source_name in registry.list_sources():
        source = registry.get_source(source_name)

        assert source.name == source_name
        assert source.display_name
        assert source.provider
        assert source.source_url.startswith("http")
        assert source.target_bronze_table.startswith("bronze_")
        assert source.target_silver_table.startswith("silver_")
        assert source.downstream_use
        assert source.required_fields
        assert source.validation_checks


def test_source_registry_filters_by_group():
    registry = SourceRegistry()

    national_sources = registry.filter_by_group("national")
    municipal_sources = registry.filter_by_group("municipal")

    assert national_sources
    assert municipal_sources
    assert all(source.source_group == "national" for source in national_sources)
    assert all(source.source_group == "municipal" for source in municipal_sources)


def test_bronze_and_silver_tables_are_registered():
    registry = SourceRegistry()

    bronze_tables = registry.bronze_tables()
    silver_tables = registry.silver_tables()

    assert "bronze_climate" in bronze_tables
    assert "bronze_disaster_events" in bronze_tables
    assert "bronze_vancouver_property_parcels" in bronze_tables
    assert "bronze_calgary_flood_hazard" in bronze_tables

    assert "silver_climate_daily" in silver_tables
    assert "silver_disaster_event_month" in silver_tables
    assert "silver_vancouver_parcel" in silver_tables
    assert "silver_calgary_flood_hazard" in silver_tables


def test_spatial_config_loads_required_crs():
    config = load_project_config("spatial_config.yml")

    assert config["crs"]["processing"]["epsg"] == 3347
    assert config["crs"]["serving"]["epsg"] == 4326
    assert config["grids"]["province_grid_10km"]["resolution_m"] == 10000
    assert config["grids"]["city_grid_1km"]["resolution_m"] == 1000


def test_risk_score_config_score_range():
    config = load_project_config("risk_score_config.yml")

    grid_score = config["grid_priority_score"]
    assert grid_score["score_range"]["min"] == 0
    assert grid_score["score_range"]["max"] == 100
    assert "coverage_confidence" in config


def test_dq_thresholds_load():
    config = load_project_config("dq_thresholds.yml")

    assert config["geometry"]["minimum_valid_rate"] >= 0.0
    assert config["crs"]["required_processing_epsg"] == 3347
    assert config["values"]["score"]["min"] == 0
    assert config["values"]["score"]["max"] == 100


def test_unknown_source_raises_key_error():
    registry = SourceRegistry()

    with pytest.raises(KeyError):
        registry.get_source("not_a_real_source")


def test_empty_or_invalid_config_path_raises_error():
    with pytest.raises(ConfigError):
        load_project_config("not_a_real_config.yml")
