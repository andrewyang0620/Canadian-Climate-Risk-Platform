output "raw_bucket_name" {
  description = "S3 bucket name for raw / Bronze data."
  value       = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  description = "S3 bucket name for processed / Silver, audit, and profiling data."
  value       = aws_s3_bucket.processed.bucket
}

output "raw_bucket_uri" {
  description = "S3 URI for raw bucket."
  value       = "s3://${aws_s3_bucket.raw.bucket}"
}

output "processed_bucket_uri" {
  description = "S3 URI for processed bucket."
  value       = "s3://${aws_s3_bucket.processed.bucket}"
}

output "bronze_prefix_uri" {
  description = "S3 URI for Bronze prefix."
  value       = "s3://${aws_s3_bucket.raw.bucket}/${local.bronze_prefix}"
}

output "silver_prefix_uri" {
  description = "S3 URI for Silver prefix."
  value       = "s3://${aws_s3_bucket.processed.bucket}/${local.silver_prefix}"
}

output "audit_prefix_uri" {
  description = "S3 URI for audit outputs."
  value       = "s3://${aws_s3_bucket.processed.bucket}/${local.audit_prefix}"
}

output "profiles_prefix_uri" {
  description = "S3 URI for profiling outputs."
  value       = "s3://${aws_s3_bucket.processed.bucket}/${local.profiles_prefix}"
}

output "s3_data_platform_policy_arn" {
  description = "IAM policy ARN for data platform S3 access."
  value       = aws_iam_policy.s3_data_platform_access.arn
}
