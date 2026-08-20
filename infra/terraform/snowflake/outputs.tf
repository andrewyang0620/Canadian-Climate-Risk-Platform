output "warehouse_name" {
  description = "Snowflake warehouse name."
  value       = snowflake_warehouse.main.name
}

output "database_name" {
  description = "Snowflake database name."
  value       = snowflake_database.main.name
}

output "schema_names" {
  description = "Snowflake schema names."
  value = {
    core      = snowflake_schema.core.name
    analytics = snowflake_schema.analytics.name
    audit     = snowflake_schema.audit.name
  }
}

output "core_schema_name" {
  description = "CORE schema name."
  value       = snowflake_schema.core.name
}

output "analytics_schema_name" {
  description = "ANALYTICS schema name."
  value       = snowflake_schema.analytics.name
}

output "audit_schema_name" {
  description = "AUDIT schema name."
  value       = snowflake_schema.audit.name
}