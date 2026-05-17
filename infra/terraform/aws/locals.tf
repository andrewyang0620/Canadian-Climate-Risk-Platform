locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = "data-engineering"
  }

  bronze_prefix   = "bronze"
  silver_prefix   = "silver"
  audit_prefix    = "audit"
  profiles_prefix = "profiles"
}
