output "warehouse_name" {
  description = "Snowflake warehouse name."
  value       = var.warehouse_name
}

output "database_name" {
  description = "Snowflake database name."
  value       = var.database_name
}

output "schema_names" {
  description = "Snowflake schema names used by the platform."
  value       = var.schema_names
}

output "bronze_external_schema_name" {
  description = "Snowflake external Bronze schema name."
  value       = var.schema_names.bronze_external
}

output "bronze_schema_name" {
  description = "Snowflake Bronze schema name."
  value       = var.schema_names.bronze
}

output "silver_schema_name" {
  description = "Snowflake Silver schema name."
  value       = var.schema_names.silver
}

output "staging_schema_name" {
  description = "Snowflake staging schema name."
  value       = var.schema_names.staging
}

output "intermediate_schema_name" {
  description = "Snowflake intermediate schema name."
  value       = var.schema_names.intermediate
}

output "marts_schema_name" {
  description = "Snowflake marts schema name."
  value       = var.schema_names.marts
}

output "gold_schema_name" {
  description = "Snowflake Gold schema name."
  value       = var.schema_names.gold
}

output "audit_schema_name" {
  description = "Snowflake audit schema name."
  value       = var.schema_names.audit
}
