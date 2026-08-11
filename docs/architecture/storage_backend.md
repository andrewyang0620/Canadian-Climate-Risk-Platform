# Storage Backend Design

## Purpose

The project uses a storage backend abstraction so the same ingestion and processing logic can write to either:

- Local lakehouse paths during development
- Azure Data Lake Storage Gen2 for cloud-oriented execution

Local development remains the default workflow. ADLS Gen2 is the production-oriented cloud storage target.

---

## Backend Modes

### Local Mode

```env
STORAGE_BACKEND=local
LOCAL_LAKEHOUSE_ROOT=lakehouse
```

Example output:
```
lakehouse/bronze/<source_name>/extract_date=<date>/run_id=<run_id>/raw/...
```

---

### Azure Data Lake Storage Gen2 Mode

```env
STORAGE_BACKEND=azure
AZURE_STORAGE_ACCOUNT_NAME=<storage-account>
AZURE_STORAGE_FILE_SYSTEM_BRONZE=bronze
AZURE_STORAGE_FILE_SYSTEM_SILVER=silver
AZURE_STORAGE_FILE_SYSTEM_GOLD=gold
AZURE_STORAGE_FILE_SYSTEM_AUDIT=audit
AZURE_STORAGE_FILE_SYSTEM_EXPORTS=exports
AZURE_STORAGE_FILE_SYSTEM_PROFILES=profiles
```

Example Bronze URI:
```
abfss://bronze@<storage-account>.dfs.core.windows.net/<source_name>/extract_date=<date>/run_id=<run_id>/raw/...
```

Authentication uses Azure Identity credentials through `DefaultAzureCredential`.

| Environment | Authentication Method |
|---|---|
| Local development | `az login` (Azure CLI) |
| Cloud workloads | Managed identity or service principal |

---

## Storage Layout

The ADLS Gen2 storage account uses separate file systems for each platform zone.

| File System | Purpose |
|---|---|
| `bronze` | Immutable raw source snapshots |
| `silver` | Standardized processed datasets |
| `gold` | Validated analytical domain and risk-score outputs |
| `audit` | Run metadata, validation, and backtesting outputs |
| `exports` | GIS and BI delivery datasets |
| `profiles` | Schema and data-quality profiling outputs |

---

## Design Principle

The storage backend interface remains intentionally small:

```
put_bytes
put_text
upload_file
exists
uri
```

Business logic does not depend directly on Azure SDK APIs. The backend abstraction keeps local development reproducible while allowing cloud storage to change independently from ingestion and transformation code.

---

## Bronze Sync Utility

The project includes a Bronze sync utility that copies local Bronze files to the configured storage backend while preserving the Bronze relative layout.

**Dry run:**
```powershell
python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze `
  --source eccc_historical_climate `
  --dry-run
```

**Sync all implemented Bronze sources:**
```powershell
python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze
```

**Path mapping example:**

Local:
```
eccc_historical_climate/extract_date=2026-05-11/run_id=<run_id>/raw/eccc_climate_daily_bc_ab_2016.jsonl.gz
```

ADLS Gen2:
```
abfss://bronze@<storage-account>.dfs.core.windows.net/eccc_historical_climate/extract_date=2026-05-11/run_id=<run_id>/raw/eccc_climate_daily_bc_ab_2016.jsonl.gz
```

**Manifest-aware filtering** (recommended for cloud uploads):
```powershell
python -m src.storage.sync_bronze `
  --bronze-root lakehouse/bronze `
  --source eccc_historical_climate `
  --latest-successful-only `
  --exclude-smoke-tests `
  --dry-run
```

This preserves the existing local ingestion workflow while providing a clean migration path to ADLS Gen2.