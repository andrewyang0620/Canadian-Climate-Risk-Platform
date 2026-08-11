data "azurerm_client_config" "current" {}


resource "random_string" "storage_suffix" {
  length  = 6
  upper   = false
  lower   = true
  numeric = true
  special = false
}


resource "azurerm_storage_account" "lake" {
  name = substr(
    "${local.storage_account_base}${random_string.storage_suffix.result}",
    0,
    24,
  )

  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type

  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false

  public_network_access_enabled   = true
  default_to_oauth_authentication = true

  cross_tenant_replication_enabled = false
  local_user_enabled               = false

  # Kept enabled during the C1 bootstrap so Terraform can reliably
  # provision the ADLS file systems.
  #
  # Application code still authenticates through Microsoft Entra ID
  # via DefaultAzureCredential.
  #
  # This will be hardened in Phase G when managed identities,
  # Key Vault, and private networking are introduced.
  shared_access_key_enabled = true

  tags = local.common_tags
}


resource "azurerm_storage_data_lake_gen2_filesystem" "zone" {
  for_each = var.file_systems

  name               = each.value
  storage_account_id = azurerm_storage_account.lake.id

  depends_on = [
    azurerm_role_assignment.current_principal_storage_data
  ]
}


resource "azurerm_role_assignment" "current_principal_storage_data" {
  count = (
    var.grant_current_principal_data_access
    ? 1
    : 0
  )

  scope = azurerm_storage_account.lake.id

  role_definition_name = "Storage Blob Data Contributor"

  principal_id = data.azurerm_client_config.current.object_id
}