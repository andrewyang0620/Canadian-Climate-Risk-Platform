# GIS Deployment Runbook

## Production Resources

| Resource | Value |
|---|---|
| Static Web App | `ccrisk-dev-national-gis` |
| Resource group | `ccrisk-dev-rg` |
| Primary URL | `https://gis.climate-risk.andrewjingtaoyang.dev` |
| Azure hostname | `https://icy-ground-0cf004a0f.7.azurestaticapps.net` |
| GIS storage | `ccriskdevgisibu7j0` |
| Container | `gis` |
| GIS base URL | `https://ccriskdevgisibu7j0.blob.core.windows.net/gis` |
| DNS | Cloudflare |

Frontend and GIS data deploy separately:

```text
React/Vite dist -> Azure Static Web Apps
FGB / JSON      -> Azure Blob Storage
```

## Frontend Environment

Required Vite variables:

```text
VITE_MAPTILER_KEY
VITE_MAP_STYLE_URL
VITE_CITY_MAP_STYLE_URL
VITE_GIS_DATA_BASE_URL
```

Production GIS base URL:

```text
https://ccriskdevgisibu7j0.blob.core.windows.net/gis
```

Do not commit API keys or deployment tokens.

## Build

```powershell
cd dashboard/gis/web
npm run build
```

Output:

```text
dashboard/gis/web/dist/
```

GIS data is not copied into `dist`.

## Local Production Preview

Use port 5173 because it is in the Blob CORS allowlist:

```powershell
npm run preview -- --port 5173
```

Validate National, Vancouver, and Calgary. FGB requests should return `206 Partial Content`.

## Deploy Frontend

Verify Azure session and target:

```powershell
az account show --query "{subscription:name, user:user.name}" -o table

az staticwebapp list `
  --query "[?name=='ccrisk-dev-national-gis'].{Name:name,ResourceGroup:resourceGroup,Hostname:defaultHostname}" `
  -o table
```

Load the existing deployment token into the current PowerShell session:

```powershell
$env:SWA_CLI_DEPLOYMENT_TOKEN = az staticwebapp secrets list `
  --name ccrisk-dev-national-gis `
  --resource-group ccrisk-dev-rg `
  --query "properties.apiKey" `
  -o tsv
```

Deploy the current `dist`:

```powershell
cd dashboard/gis/web

npx --yes -p @azure/static-web-apps-cli@latest swa deploy .\dist `
  --env production `
  --app-name ccrisk-dev-national-gis `
  --resource-group ccrisk-dev-rg
```

Do not create a new Static Web App if the CLI prompts unexpectedly.

## GIS Data Layout

```text
gis/
├── manifest.json
├── grid_metadata.json
├── grid_geometry.fgb
├── months/
└── cities/
    ├── manifest.json
    ├── vancouver/
    │   ├── parcels.fgb
    │   └── building_permits.fgb
    └── calgary/
        ├── properties.fgb
        ├── building_permits.fgb
        ├── development_permits.fgb
        └── development_permit_property_links.json
```

Frontend-only changes do not require GIS data uploads.

Example listing:

```powershell
az storage blob list `
  --account-name ccriskdevgisibu7j0 `
  --container-name gis `
  --auth-mode login `
  --output table
```

## Domain Structure

```text
andrewjingtaoyang.dev
    -> future personal portfolio

climate-risk.andrewjingtaoyang.dev
    -> reserved project page

gis.climate-risk.andrewjingtaoyang.dev
    -> production GIS
```

Cloudflare DNS points the GIS hostname to Azure Static Web Apps. Azure manages HTTPS for the custom domain.

## Blob CORS

Required origins:

```text
http://localhost:5173
https://icy-ground-0cf004a0f.7.azurestaticapps.net
https://gis.climate-risk.andrewjingtaoyang.dev
```

Validate Range and CORS:

```powershell
curl.exe -sS -D - -o NUL `
  -H "Origin: https://gis.climate-risk.andrewjingtaoyang.dev" `
  -H "Range: bytes=0-1023" `
  "https://ccriskdevgisibu7j0.blob.core.windows.net/gis/cities/calgary/properties.fgb"
```

Expected:

```text
HTTP/1.1 206 Partial Content
Access-Control-Allow-Origin: https://gis.climate-risk.andrewjingtaoyang.dev
```

The custom-domain CORS origin should also exist in Terraform before an authoritative `terraform apply`.

## MapTiler

If the production API key restricts HTTP origins, allow:

```text
http://localhost:5173
https://icy-ground-0cf004a0f.7.azurestaticapps.net
https://gis.climate-risk.andrewjingtaoyang.dev
```

## Production Acceptance

Verify:

1. The custom domain returns the current frontend.
2. National, Vancouver, and Calgary scopes work.
3. Property, flood, BP, and DP layers load as expected.
4. BP to property and Calgary DP to property interactions work.
5. FGB requests use `206 Partial Content`.
6. Browser Console has no blocking errors.

## Rollback

Frontend rollback:

- Redeploy the last known-good `dist` to the same Static Web App.

GIS data rollback:

- Restore the previous presentation artifacts in the `gis` container.
