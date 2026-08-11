from src.utils.config import load_project_config


def test_platform_config_uses_azure_and_snowflake():
    config = load_project_config("platform_config.yml")

    assert config["platform"]["primary_cloud_provider"] == "azure"
    assert config["platform"]["warehouse"] == "snowflake"
    assert config["platform"]["orchestration"] == "azure_data_factory"

    assert config["storage"]["default_backend"] == "local"

    azure = config["storage"]["azure"]

    assert azure["account_name_env"] == "AZURE_STORAGE_ACCOUNT_NAME"

    assert (
        azure["file_system_envs"]["bronze"]
        == "AZURE_STORAGE_FILE_SYSTEM_BRONZE"
    )

    assert (
        azure["file_system_envs"]["silver"]
        == "AZURE_STORAGE_FILE_SYSTEM_SILVER"
    )

    assert (
        azure["file_system_envs"]["gold"]
        == "AZURE_STORAGE_FILE_SYSTEM_GOLD"
    )


def test_platform_config_defines_required_warehouse_schemas():
    config = load_project_config("platform_config.yml")

    schemas = config["warehouse"]["schemas"]

    assert "core" in schemas
    assert "staging" in schemas
    assert "intermediate" in schemas
    assert "marts" in schemas
    assert "audit" in schemas

    assert schemas["core"]["name"] == "CORE"
    assert schemas["marts"]["name"] == "MARTS"


def test_platform_config_keeps_local_and_cloud_modes_separate():
    config = load_project_config("platform_config.yml")

    local = config["deployment_modes"]["local_dev"]
    cloud = config["deployment_modes"]["cloud_target"]

    assert local["bronze_storage"] == "local_filesystem"
    assert local["silver_storage"] == "local_filesystem"

    assert cloud["bronze_storage"] == "azure_adls_gen2"
    assert cloud["silver_storage"] == "azure_adls_gen2"
    assert cloud["gold_storage"] == "azure_adls_gen2"

    assert cloud["warehouse"] == "snowflake"
    assert cloud["orchestration"] == "azure_data_factory"


def test_platform_config_defines_medallion_storage_zones():
    config = load_project_config("platform_config.yml")

    zones = config["storage"]["zones"]

    assert zones["bronze"]["azure_file_system"] == "bronze"
    assert zones["silver"]["azure_file_system"] == "silver"
    assert zones["gold"]["azure_file_system"] == "gold"
    assert zones["audit"]["azure_file_system"] == "audit"
    assert zones["profiles"]["azure_file_system"] == "profiles"
    assert zones["exports"]["azure_file_system"] == "exports"


def test_dbt_profile_name_matches_platform_config():
    config = load_project_config("platform_config.yml")

    assert config["dbt"]["profile_name"] == "climate_risk_snowflake"
    assert config["dbt"]["profiles_dir"] == "dbt"