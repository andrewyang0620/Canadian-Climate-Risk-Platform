resource "snowflake_schema" "bronze_external" {
  database = snowflake_database.main.name
  name     = var.schema_names.bronze_external
  comment  = "External access layer for raw Bronze objects stored in S3."
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.main.name
  name     = var.schema_names.bronze
  comment  = "Optional raw landing tables in Snowflake."
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.main.name
  name     = var.schema_names.silver
  comment  = "Standardized processed tables loaded from S3 Silver outputs."
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.main.name
  name     = var.schema_names.staging
  comment  = "dbt staging models."
}

resource "snowflake_schema" "intermediate" {
  database = snowflake_database.main.name
  name     = var.schema_names.intermediate
  comment  = "dbt intermediate transformation models."
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.main.name
  name     = var.schema_names.marts
  comment  = "Business-facing dbt mart tables."
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.main.name
  name     = var.schema_names.gold
  comment  = "Final analytics-ready warehouse layer in Snowflake."
}

resource "snowflake_schema" "audit" {
  database = snowflake_database.main.name
  name     = var.schema_names.audit
  comment  = "Warehouse audit tables and pipeline validation outputs."
}
