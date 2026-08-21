resource "snowflake_storage_integration_azure" "adls_gold" {
  name    = "ADLS_GOLD_INTEGRATION"
  enabled = true

  azure_tenant_id = var.azure_tenant_id

  storage_allowed_locations = [
    "azure://${var.adls_storage_account_name}.blob.core.windows.net/${var.adls_gold_file_system}/"
  ]

  comment = "Read-only Snowflake integration for canonical ADLS Gold analytical sources."
}