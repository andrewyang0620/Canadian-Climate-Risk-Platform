variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}

variable "snowflake_role" {
  type    = string
  default = "SYSADMIN"
}

variable "database_name" {
  type    = string
  default = "CLIMATE_RISK"
}

variable "warehouse_name" {
  type    = string
  default = "CLIMATE_RISK_WH"
}

variable "warehouse_size" {
  type    = string
  default = "XSMALL"
}

variable "schema_names" {
  description = "Snowflake schemas used by the data platform."
  type = object({
    core      = string
    analytics = string
    audit     = string
  })

  default = {
    core      = "CORE"
    analytics = "ANALYTICS"
    audit     = "AUDIT"
  }
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name. This is required by the snowflakedb/snowflake provider."
  type        = string
}

variable "azure_tenant_id" {
  description = "Microsoft Entra tenant ID containing the ADLS storage account."
  type        = string
}

variable "adls_storage_account_name" {
  description = "Canonical ADLS Gen2 storage account."
  type        = string
  default     = "ccriskdevibu7j0"
}

variable "adls_gold_file_system" {
  description = "Canonical ADLS Gold filesystem."
  type        = string
  default     = "gold"
}
