output "data_lake_bucket_name" {
  value = aws_s3_bucket.data_lake.bucket
}

output "bronze_uri" {
  value = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/"
}

output "silver_uri" {
  value = "s3://${aws_s3_bucket.data_lake.bucket}/silver/"
}
