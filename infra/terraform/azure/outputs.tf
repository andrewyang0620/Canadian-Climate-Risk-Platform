output "resource_group_name" {
  description = "Azure resource group containing the platform foundation."

  value = azurerm_resource_group.platform.name
}


output "storage_account_name" {
  description = "ADLS Gen2 storage account name."

  value = azurerm_storage_account.lake.name
}


output "storage_account_id" {
  description = "Azure resource ID of the ADLS Gen2 storage account."

  value = azurerm_storage_account.lake.id
}


output "storage_account_dfs_endpoint" {
  description = "DFS endpoint for the ADLS Gen2 account."

  value = azurerm_storage_account.lake.primary_dfs_endpoint
}


output "file_system_names" {
  description = "ADLS Gen2 file systems provisioned for the platform."

  value = sort(
    tolist(
      var.file_systems
    )
  )
}


output "abfss_uris" {
  description = "ABFSS root URI for each platform file system."

  value = {
    for name in var.file_systems :
    name => ("abfss://${name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
    )
  }
}