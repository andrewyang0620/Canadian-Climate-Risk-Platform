locals {
  resource_group_name = coalesce(
    var.resource_group_name,
    "${var.project_code}-${var.environment}-rg",
  )

  storage_account_base = lower(
    replace(
      "${var.storage_account_prefix}${var.environment}",
      "-",
      "",
    )
  )

  gis_storage_account_base = lower(
    replace(
      "${var.storage_account_prefix}${var.environment}gis",
      "-",
      "",
    )
  )

  common_tags = merge(
    {
      project     = "canadian-climate-risk-platform"
      environment = var.environment
      managed_by  = "terraform"
      repository  = "Canadian-Climate-Risk-Platform"
    },
    var.tags,
  )
}