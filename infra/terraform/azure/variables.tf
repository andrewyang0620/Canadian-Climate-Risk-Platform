variable "subscription_id" {
  description = "Azure subscription ID used for the deployment."
  type        = string
}


variable "location" {
  description = "Azure region for the platform foundation."
  type        = string
  default     = "canadacentral"
}


variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition = contains(
      ["dev", "test", "prod"],
      var.environment,
    )

    error_message = "environment must be dev, test, or prod."
  }
}


variable "project_code" {
  description = "Short project identifier used in Azure resource names."
  type        = string
  default     = "ccrisk"
}


variable "resource_group_name" {
  description = "Optional explicit Azure resource group name."
  type        = string
  default     = null
  nullable    = true
}


variable "storage_account_prefix" {
  description = "Prefix used for the globally unique ADLS Gen2 storage account."
  type        = string
  default     = "ccrisk"
}


variable "storage_replication_type" {
  description = "Storage replication type for the ADLS Gen2 account."
  type        = string
  default     = "LRS"

  validation {
    condition = contains(
      ["LRS", "ZRS", "GRS", "RAGRS"],
      var.storage_replication_type,
    )

    error_message = "Unsupported storage replication type."
  }
}


variable "file_systems" {
  description = "ADLS Gen2 file systems used by the platform."

  type = set(string)

  default = [
    "bronze",
    "silver",
    "gold",
    "audit",
    "exports",
    "profiles",
  ]
}


variable "grant_current_principal_data_access" {
  description = "Grant the Terraform caller Storage Blob Data Contributor access."
  type        = bool
  default     = true
}


variable "tags" {
  description = "Additional Azure resource tags."
  type        = map(string)
  default     = {}
}

variable "static_web_app_location" {
  description = "Azure region used by the Static Web App resource."
  type        = string
  default     = "eastus2"
}


variable "gis_serving_container_name" {
  description = "Public blob container containing browser-facing GIS artifacts."
  type        = string
  default     = "gis"
}