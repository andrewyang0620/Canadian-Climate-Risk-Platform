variable "aws_region" {
  description = "AWS region for project resources."
  type        = string
  default     = "ca-central-1"
}

variable "project_name" {
  description = "Project name used for tagging."
  type        = string
  default     = "canadian-climate-risk-platform"
}

variable "data_lake_bucket_name" {
  description = "Globally unique S3 bucket name for the data lake."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}
