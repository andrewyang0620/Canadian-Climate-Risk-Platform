from src.utils.config import load_project_config


def test_platform_config_uses_aws_and_snowflake():
    config = load_project_config("platform_config.yml")

    assert config["platform"]["primary_cloud_provider"] == "aws"
    assert config["platform"]["warehouse"] == "snowflake"
    assert config["storage"]["default_backend"] == "local"
    assert config["storage"]["s3"]["raw_bucket_env"] == "AWS_S3_BUCKET_RAW"
    assert config["storage"]["s3"]["processed_bucket_env"] == "AWS_S3_BUCKET_PROCESSED"
    assert config["storage"]["s3"]["bronze_prefix"] == "bronze"
    assert config["storage"]["s3"]["silver_prefix"] == "silver"
    assert config["storage"]["silver"]["s3_uri_env"] == "S3_SILVER_URI"


def test_platform_config_defines_required_warehouse_schemas():
    config = load_project_config("platform_config.yml")

    schemas = config["warehouse"]["schemas"]

    assert "bronze_external" in schemas
    assert "silver" in schemas
    assert "gold" in schemas
    assert "audit" in schemas
    assert schemas["gold"]["name"] == "GOLD"


def test_platform_config_keeps_local_and_cloud_modes_separate():
    config = load_project_config("platform_config.yml")

    assert config["deployment_modes"]["local_dev"]["bronze_storage"] == "local_filesystem"
    assert config["deployment_modes"]["cloud_target"]["bronze_storage"] == "aws_s3"
    assert config["deployment_modes"]["cloud_target"]["warehouse"] == "snowflake"


def test_dbt_profile_name_matches_platform_config():
    config = load_project_config("platform_config.yml")

    assert config["dbt"]["profile_name"] == "climate_risk_snowflake"
    assert config["dbt"]["profiles_dir"] == "dbt"
