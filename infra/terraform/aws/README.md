# AWS Terraform — S3 Storage Foundation

This directory defines the AWS S3 storage foundation for the Canadian Climate Risk Data Platform.

Gold layer analytics are in Snowflake (see `infra/terraform/snowflake/`). This module manages S3 only.

## Storage Layout

```text
raw S3 bucket
└── bronze/          ← raw source snapshots (append/overwrite only, no deletes)

processed S3 bucket
├── silver/          ← cleaned, validated Parquet / GeoParquet
├── audit/           ← data quality check outputs, pipeline audit records
└── profiles/        ← source profiling outputs (feature/07-source-profiling)
```

## Resources Created

- `raw` S3 bucket — Bronze zone
- `processed` S3 bucket — Silver, audit, and profiling zones
- Public access blocks on both buckets
- AES256 server-side encryption with bucket key
- Bucket versioning
- Lifecycle rules per prefix (silver, audit, profiles use separate retention periods)
- Least-privilege IAM policy (`ReadWrite` on silver/audit/profiles; `Read/Write` only — no delete — on bronze)

## Files

| File | Purpose |
|---|---|
| `versions.tf` | Terraform and AWS provider version constraints |
| `variables.tf` | All input variables with defaults |
| `locals.tf` | Common tags and S3 prefix names |
| `s3.tf` | Bucket resources, encryption, versioning, lifecycle rules |
| `iam.tf` | IAM policy document and policy resource |
| `outputs.tf` | Bucket names, URIs, prefix URIs, policy ARN |
| `terraform.tfvars.example` | Template — copy to `terraform.tfvars` and fill in |
| `backend.tf.example` | Remote state backend template (inactive) |

## Setup

Copy the example variables file:

```powershell
Copy-Item terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and replace bucket names with globally unique values.

## Validate Locally

```powershell
terraform fmt -recursive
terraform init -backend=false
terraform validate
```

## Plan

Only run after configuring AWS credentials:

```powershell
terraform plan -var-file="terraform.tfvars"
```

## Apply

Do not apply until the project is ready to create real AWS resources.

```powershell
terraform apply -var-file="terraform.tfvars"
```

## Notes

- `terraform.tfvars` must not be committed (covered by `.gitignore`).
- `backend.tf.example` is intentionally not active; configure remote state before first `apply`.
- Bronze objects must not be deleted by pipeline code — the IAM policy enforces this by omitting `s3:DeleteObject` from the raw bucket statement.
