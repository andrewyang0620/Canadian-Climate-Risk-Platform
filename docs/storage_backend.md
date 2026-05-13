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