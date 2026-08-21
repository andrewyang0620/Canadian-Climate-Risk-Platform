resource "snowflake_stage_external_azure" "adls_gold" {
  name     = "ADLS_GOLD_STAGE"
  database = snowflake_database.main.name
  schema   = snowflake_schema.core.name

  url = "azure://${var.adls_storage_account_name}.blob.core.windows.net/${var.adls_gold_file_system}/"

  storage_integration = snowflake_storage_integration_azure.adls_gold.name

  comment = "External stage for canonical ADLS Gold analytical sources."
}