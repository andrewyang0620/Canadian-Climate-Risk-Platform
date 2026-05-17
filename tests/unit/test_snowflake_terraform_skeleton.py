from pathlib import Path


TERRAFORM_DIR = Path("infra/terraform/snowflake")


def test_snowflake_terraform_files_exist():
    expected_files = {
        "variables.tf",
        "outputs.tf",
        "terraform.tfvars.example",
    }

    actual_files = {path.name for path in TERRAFORM_DIR.iterdir() if path.is_file()}

    assert expected_files <= actual_files


def test_snowflake_terraform_does_not_default_to_accountadmin():
    combined = "\n".join(path.read_text(encoding="utf-8") for path in TERRAFORM_DIR.glob("*.tf"))

    assert 'default     = "ACCOUNTADMIN"' not in combined
    assert 'default = "ACCOUNTADMIN"' not in combined
    assert "SYSADMIN" in combined


def test_snowflake_terraform_outputs_core_names():
    outputs_tf = (TERRAFORM_DIR / "outputs.tf").read_text(encoding="utf-8")

    assert 'output "warehouse_name"' in outputs_tf
    assert 'output "database_name"' in outputs_tf
    assert 'output "schema_names"' in outputs_tf
    assert 'output "bronze_external_schema_name"' in outputs_tf
    assert 'output "silver_schema_name"' in outputs_tf
    assert 'output "gold_schema_name"' in outputs_tf
    assert 'output "audit_schema_name"' in outputs_tf


def test_snowflake_tfvars_example_documents_required_values():
    tfvars_example = (TERRAFORM_DIR / "terraform.tfvars.example").read_text(encoding="utf-8")

    assert "snowflake_account" in tfvars_example
    assert "snowflake_user" in tfvars_example
    assert "snowflake_role" in tfvars_example
    assert "warehouse_name" in tfvars_example
    assert "database_name" in tfvars_example
    assert "schema_names" in tfvars_example
    assert "BRONZE_EXTERNAL" in tfvars_example
    assert "GOLD" in tfvars_example


def test_snowflake_provider_uses_organization_and_account_name():
    versions_tf = (TERRAFORM_DIR / "versions.tf").read_text(encoding="utf-8")
    variables_tf = (TERRAFORM_DIR / "variables.tf").read_text(encoding="utf-8")
    tfvars_example = (TERRAFORM_DIR / "terraform.tfvars.example").read_text(encoding="utf-8")

    assert "organization_name" in versions_tf
    assert "account_name" in versions_tf
    assert 'variable "snowflake_organization_name"' in variables_tf
    assert "snowflake_organization_name" in tfvars_example
