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
    bronze_external = string
    bronze          = string
    silver          = string
    staging         = string
    intermediate    = string
    marts           = string
    gold            = string
    audit           = string
  })

  default = {
    bronze_external = "BRONZE_EXTERNAL"
    bronze          = "BRONZE"
    silver          = "SILVER"
    staging         = "STAGING"
    intermediate    = "INTERMEDIATE"
    marts           = "MARTS"
    gold            = "GOLD"
    audit           = "AUDIT"
  }
}

