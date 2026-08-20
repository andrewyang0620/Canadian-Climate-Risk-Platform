resource "snowflake_schema" "core" {
  database = snowflake_database.main.name
  name     = var.schema_names.core
  comment  = "Curated analytical source tables loaded from canonical ADLS Gold outputs."
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.main.name
  name     = var.schema_names.analytics
  comment  = "Business-facing analytical models build by dbt."
}

resource "snowflake_schema" "audit" {
  database = snowflake_database.main.name
  name     = var.schema_names.audit
  comment  = "Warehouse load audit, validation, and operational metadata."
}