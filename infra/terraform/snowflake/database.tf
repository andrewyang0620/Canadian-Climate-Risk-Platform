resource "snowflake_database" "main" {
  name    = var.database_name
  comment = "Database for Canadian Climate Risk data platform."
}
