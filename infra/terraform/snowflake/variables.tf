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
  default = "ACCOUNTADMIN"
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
