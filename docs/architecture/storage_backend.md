# Storage Backend Design
 
## Purpose
 
This project uses a storage backend abstraction so the same ingestion and processing logic can write to either:
 
- local lakehouse paths during development
- AWS S3 paths for cloud-oriented execution
This keeps local development reproducible while aligning the architecture with an AWS S3 + Snowflake ELT platform.
 
---
 
## Backend Modes
 
### Local Mode
 
```text
STORAGE_BACKEND=local
LOCAL_LAKEHOUSE_ROOT=lakehouse
```
 
Example output:
 
```text
lakehouse/bronze/<source_name>/extract_date=<date>/run_id=<run_id>/raw/...
```
 
### S3 Mode
 
```text
STORAGE_BACKEND=s3
AWS_REGION=ca-central-1
AWS_S3_BUCKET_RAW=<raw-bucket>
AWS_S3_BUCKET_PROCESSED=<processed-bucket>
AWS_S3_BRONZE_PREFIX=bronze
AWS_S3_SILVER_PREFIX=silver
```
 
Example output:
 
```text
s3://<raw-bucket>/bronze/<source_name>/extract_date=<date>/run_id=<run_id>/raw/...
```
 
---
 
## Design Principle
 
The storage backend is intentionally small:
 
```text
put_bytes
put_text
upload_file
exists
uri
```
 
This is enough for ingestion, audit metadata, manifest files, and later Silver outputs.
 
---
 
## Current Scope
 
This feature introduces the abstraction and tests. Existing ingestion flows still write locally by default. Later features will integrate the backend into Bronze writers and Silver outputs.
## Bronze Sync Utility

The project includes a Bronze sync utility that copies local Bronze files to the configured storage backend while preserving the Bronze relative layout.

Dry run example:

```powershell
python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze `
  --source eccc_historical_climate `
  --dry-run

Sync all implemented Bronze sources to the configured backend:

python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze

The sync utility preserves paths such as:

eccc_historical_climate/extract_date=2026-05-11/run_id=<run_id>/raw/eccc_climate_daily_bc_ab_2016.jsonl.gz

When STORAGE_BACKEND=s3, this becomes:

s3://<raw-bucket>/bronze/eccc_historical_climate/extract_date=2026-05-11/run_id=<run_id>/raw/eccc_climate_daily_bc_ab_2016.jsonl.gz

This allows the current local Bronze ingestion pipeline to remain reproducible while enabling cloud storage migration.

### Recommended S3 Sync Mode

For cloud upload, use manifest-aware filtering instead of syncing every local development run:

```powershell
python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze `
  --source eccc_historical_climate `
  --latest-successful-only `
  --exclude-smoke-tests `
  --dry-run

This mode uses bronze_runs.jsonl to select the latest successful non-smoke-test run for each source. It avoids uploading old development runs and smoke-test artifacts.
