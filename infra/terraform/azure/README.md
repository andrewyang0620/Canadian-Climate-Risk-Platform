# Azure Terraform Foundation

This Terraform configuration provisions the minimum Azure foundation for the Canadian Climate Risk Platform.

---

## Resources

- Azure Resource Group
- ADLS Gen2 StorageV2 account with hierarchical namespace
- Bronze file system
- Silver file system
- Gold file system
- Audit file system
- Exports file system
- Profiles file system
- Storage Blob Data Contributor access for the deploying principal

---

## Authentication

Authenticate locally with Azure CLI:

```powershell
az login
az account show
```

Copy the example variables:

```powershell
Copy-Item terraform.tfvars.example terraform.tfvars
```

Set the Azure subscription ID in `terraform.tfvars`.

---

## Validate

```powershell
terraform init
terraform fmt -recursive
terraform validate
terraform plan
```

---

## Apply

```powershell
terraform apply
```

After provisioning, export the storage account name to the project environment:

```env
STORAGE_BACKEND=azure
AZURE_STORAGE_ACCOUNT_NAME=<terraform output>
```

The Python storage backend authenticates through `DefaultAzureCredential`.

---

## Scope

This foundation intentionally does not provision:

- Azure Data Factory
- Azure Databricks
- Key Vault
- Private endpoints
- Azure Static Web Apps

Those services are introduced in later project phases after the Azure data-lake foundation is validated.