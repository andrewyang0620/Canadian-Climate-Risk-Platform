output "warehouse_name" {
  description = "Snowflake warehouse name."
  value       = snowflake_warehouse.main.name
}

output "database_name" {
  description = "Snowflake database name."
  value       = snowflake_database.main.name
}

output "schema_names" {
  description = "Snowflake schema names used by the platform."
  value = {
    bronze_external = snowflake_schema.bronze_external.name
    bronze          = snowflake_schema.bronze.name
    silver          = snowflake_schema.silver.name
    staging         = snowflake_schema.staging.name
    intermediate    = snowflake_schema.intermediate.name
    marts           = snowflake_schema.marts.name
    gold            = snowflake_schema.gold.name
    audit           = snowflake_schema.audit.name
  }
}

output "bronze_external_schema_name" {
  description = "Snowflake external Bronze schema name."
  value       = snowflake_schema.bronze_external.name
}

output "bronze_schema_name" {
  description = "Snowflake Bronze schema name."
  value       = snowflake_schema.bronze.name
}

output "silver_schema_name" {
  description = "Snowflake Silver schema name."
  value       = snowflake_schema.silver.name
}

output "staging_schema_name" {
  description = "Snowflake staging schema name."
  value       = snowflake_schema.staging.name
}

output "intermediate_schema_name" {
  description = "Snowflake intermediate schema name."
  value       = snowflake_schema.intermediate.name
}

output "marts_schema_name" {
  description = "Snowflake marts schema name."
  value       = snowflake_schema.marts.name
}

output "gold_schema_name" {
  description = "Snowflake Gold schema name."
  value       = snowflake_schema.gold.name
}

output "audit_schema_name" {
  description = "Snowflake audit schema name."
  value       = snowflake_schema.audit.name
}
