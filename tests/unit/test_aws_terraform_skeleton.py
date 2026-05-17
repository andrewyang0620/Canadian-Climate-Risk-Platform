from pathlib import Path


TERRAFORM_DIR = Path("infra/terraform/aws")


def test_aws_terraform_skeleton_files_exist():
    expected_files = {
        "versions.tf",
        "variables.tf",
        "locals.tf",
        "s3.tf",
        "iam.tf",
        "outputs.tf",
        "terraform.tfvars.example",
        "backend.tf.example",
        "README.md",
    }

    actual_files = {path.name for path in TERRAFORM_DIR.iterdir() if path.is_file()}

    assert expected_files <= actual_files


def test_aws_terraform_defines_raw_and_processed_buckets():
    s3_tf = (TERRAFORM_DIR / "s3.tf").read_text(encoding="utf-8")

    assert 'resource "aws_s3_bucket" "raw"' in s3_tf
    assert 'resource "aws_s3_bucket" "processed"' in s3_tf
    assert "aws_s3_bucket_public_access_block" in s3_tf
    assert "aws_s3_bucket_versioning" in s3_tf
    assert "aws_s3_bucket_server_side_encryption_configuration" in s3_tf


def test_aws_terraform_tfvars_example_documents_lifecycle_values():
    tfvars_example = (TERRAFORM_DIR / "terraform.tfvars.example").read_text(encoding="utf-8")

    assert "your-suffix" in tfvars_example
    assert "raw_bucket_name" in tfvars_example
    assert "processed_bucket_name" in tfvars_example
    assert "noncurrent_version_expiration_days" in tfvars_example
    assert "incomplete_multipart_upload_days" in tfvars_example
    assert "audit_noncurrent_version_expiration_days" in tfvars_example
    assert "audit_object_expiration_days" in tfvars_example


def test_aws_terraform_bronze_iam_does_not_allow_delete_object():
    iam_tf = (TERRAFORM_DIR / "iam.tf").read_text(encoding="utf-8")

    raw_statement_start = iam_tf.index('sid    = "ReadWriteRawBronzeObjects"')
    processed_statement_start = iam_tf.index(
        'sid    = "ReadWriteProcessedSilverAuditProfileObjects"'
    )
    raw_statement = iam_tf[raw_statement_start:processed_statement_start]

    assert "s3:GetObject" in raw_statement
    assert "s3:PutObject" in raw_statement
    assert "s3:DeleteObject" not in raw_statement


def test_aws_terraform_processed_iam_authorizes_silver_audit_and_profiles():
    iam_tf = (TERRAFORM_DIR / "iam.tf").read_text(encoding="utf-8")

    assert "${aws_s3_bucket.processed.arn}/${local.silver_prefix}/*" in iam_tf
    assert "${aws_s3_bucket.processed.arn}/${local.audit_prefix}/*" in iam_tf
    assert "${aws_s3_bucket.processed.arn}/${local.profiles_prefix}/*" in iam_tf


def test_aws_terraform_processed_bucket_has_audit_and_profiles_prefixes():
    locals_tf = (TERRAFORM_DIR / "locals.tf").read_text(encoding="utf-8")
    s3_tf = (TERRAFORM_DIR / "s3.tf").read_text(encoding="utf-8")
    outputs_tf = (TERRAFORM_DIR / "outputs.tf").read_text(encoding="utf-8")

    assert "bronze_prefix" in locals_tf
    assert "silver_prefix" in locals_tf
    assert "audit_prefix" in locals_tf
    assert "profiles_prefix" in locals_tf
    assert "gold_prefix" not in locals_tf

    assert "processed-silver-lifecycle" in s3_tf
    assert "processed-audit-lifecycle" in s3_tf
    assert "processed-profiles-lifecycle" in s3_tf
    assert 'prefix = ""' not in s3_tf

    assert 'output "bronze_prefix_uri"' in outputs_tf
    assert 'output "silver_prefix_uri"' in outputs_tf
    assert 'output "audit_prefix_uri"' in outputs_tf
    assert 'output "profiles_prefix_uri"' in outputs_tf
    assert 'output "gold_prefix_uri"' not in outputs_tf
