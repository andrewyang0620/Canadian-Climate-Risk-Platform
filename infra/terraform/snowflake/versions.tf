terraform {
  required_version = ">= 1.6.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0.0"
    }
  }
}

provider "snowflake" {
  account_name = var.snowflake_account
  user         = var.snowflake_user
  password     = var.snowflake_password
  role         = var.snowflake_role
}
