resource "snowflake_warehouse" "main" {
  name           = var.warehouse_name
  warehouse_size = var.warehouse_size

  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  comment = "Warehouse for Canadian Climate Risk data platform development workloads."
}
