resource "snowflake_warehouse" "transform" {
  name                = var.warehouse_name
  warehouse_size      = var.warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_database" "climate_risk" {
  name = var.database_name
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.climate_risk.name
  name     = "BRONZE"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.climate_risk.name
  name     = "SILVER"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.climate_risk.name
  name     = "GOLD"
}

resource "snowflake_schema" "audit" {
  database = snowflake_database.climate_risk.name
  name     = "AUDIT"
}
