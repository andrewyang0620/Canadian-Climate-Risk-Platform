data "aws_iam_policy_document" "s3_data_platform_access" {
  statement {
    sid    = "ListProjectBuckets"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]

    resources = [
      aws_s3_bucket.raw.arn,
      aws_s3_bucket.processed.arn
    ]
  }

  statement {
    sid    = "ReadWriteRawBronzeObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]

    resources = [
      "${aws_s3_bucket.raw.arn}/${local.bronze_prefix}/*"
    ]
  }

  statement {
    sid    = "ReadWriteProcessedSilverAuditProfileObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.processed.arn}/${local.silver_prefix}/*",
      "${aws_s3_bucket.processed.arn}/${local.audit_prefix}/*",
      "${aws_s3_bucket.processed.arn}/${local.profiles_prefix}/*"
    ]
  }
}

resource "aws_iam_policy" "s3_data_platform_access" {
  name        = "${var.project_name}-${var.environment}-s3-data-platform-access"
  description = "Least-privilege S3 access policy for Canadian Climate Risk data platform raw and processed zones."
  policy      = data.aws_iam_policy_document.s3_data_platform_access.json
}
