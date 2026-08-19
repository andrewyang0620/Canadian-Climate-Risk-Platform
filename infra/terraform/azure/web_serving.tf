resource "azurerm_static_web_app" "explorer" {
  name = "${var.project_code}-${var.environment}-national-gis"

  resource_group_name = azurerm_resource_group.platform.name
  location            = var.static_web_app_location

  sku_tier = "Free"
  sku_size = "Free"

  public_network_access_enabled = true
  preview_environments_enabled  = false

  tags = local.common_tags
}


resource "azurerm_storage_account" "gis_serving" {
  name = substr(
    "${local.gis_storage_account_base}${random_string.storage_suffix.result}",
    0,
    24,
  )

  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location

  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"

  is_hns_enabled = false

  min_tls_version            = "TLS1_2"
  https_traffic_only_enabled = true

  public_network_access_enabled   = true
  allow_nested_items_to_be_public = true

  default_to_oauth_authentication = true

  # Required by the current Terraform storage-container
  # provisioning path. Browser reads remain anonymous
  # only for the explicitly public GIS container.
  shared_access_key_enabled = true

  cross_tenant_replication_enabled = false
  local_user_enabled               = false

  blob_properties {
    cors_rule {
      allowed_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://${azurerm_static_web_app.explorer.default_host_name}",
      ]

      allowed_methods = [
        "GET",
        "HEAD",
        "OPTIONS",
      ]

      allowed_headers = [
        "*",
      ]

      exposed_headers = [
        "Accept-Ranges",
        "Content-Length",
        "Content-Range",
        "ETag",
        "Last-Modified",
      ]

      max_age_in_seconds = 3600
    }
  }

  tags = local.common_tags
}


resource "azurerm_storage_container" "gis_serving" {
  name = var.gis_serving_container_name

  storage_account_id = azurerm_storage_account.gis_serving.id

  # Anonymous clients may read known blob URLs,
  # but cannot enumerate the container.
  container_access_type = "blob"
}


resource "azurerm_role_assignment" "current_principal_gis_storage_data" {
  count = (
    var.grant_current_principal_data_access
    ? 1
    : 0
  )

  scope = azurerm_storage_account.gis_serving.id

  role_definition_name = "Storage Blob Data Contributor"

  principal_id = data.azurerm_client_config.current.object_id
}