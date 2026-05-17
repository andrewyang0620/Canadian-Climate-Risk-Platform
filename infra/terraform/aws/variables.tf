variable "project_name" {
  description = "Project name used for tagging and naming."
  type        = string
  default     = "canadian-climate-risk-platform"
}

variable "environment" {
  description = "Deployment environment, such as dev, staging, or prod."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for S3 resources."
  type        = string
  default     = "ca-central-1"
}

variable "raw_bucket_name" {
  description = "Globally unique S3 bucket name for raw / Bronze data."
  type        = string
}

variable "processed_bucket_name" {
  description = "Globally unique S3 bucket name for processed / Silver data."
  type        = string
}

variable "force_destroy_buckets" {
  description = "Whether Terraform can destroy buckets containing objects. Keep false for safer environments."
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  description = "Days before noncurrent object versions expire."
  type        = number
  default     = 90
}

variable "incomplete_multipart_upload_days" {
  description = "Days before incomplete multipart uploads are aborted."
  type        = number
  default     = 7
}

variable "audit_noncurrent_version_expiration_days" {
  description = "Days before noncurrent audit/profile object versions expire."
  type        = number
  default     = 30
}

variable "audit_object_expiration_days" {
  description = "Days before current audit/profile objects expire."
  type        = number
  default     = 180
}

