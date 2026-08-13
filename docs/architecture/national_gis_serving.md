# National GIS Serving Architecture

## Status

Production serving architecture for the National Climate Risk Explorer.

**Production application:** <https://icy-ground-0cf004a0f.7.azurestaticapps.net>

## Architecture

The National GIS explorer separates frontend hosting from GIS data delivery.

```
Browser
  │
  ├── Azure Static Web Apps
  │     React + Vite
  │     MapLibre GL
  │     deck.gl
  │
  └── Azure Blob Storage
        manifest.json
        grid_metadata.json
        grid_geometry.fgb
        months/risk_YYYY-MM.json
```

### Frontend Hosting

Azure Static Web Apps hosts only the compiled frontend application.

| Resource | Value |
|----------|-------|
| Static Web App | `ccrisk-dev-national-gis` |
| Production URL | `https://icy-ground-0cf004a0f.7.azurestaticapps.net` |

The production Vite build does not copy GIS serving data into `dist`.

### GIS Serving Storage

Browser-facing GIS artifacts are hosted separately from the analytical ADLS lake.

| Resource | Value |
|----------|-------|
| Storage account | `ccriskdevgisibu7j0` |
| Container | `gis` |
| Base URL | `https://ccriskdevgisibu7j0.blob.core.windows.net/gis` |

Published artifacts:

```
gis/
├── manifest.json
├── grid_metadata.json
├── grid_geometry.fgb
└── months/
    ├── risk_2016-01.json
    ├── ...
    └── risk_2025-12.json
```

The analytical Bronze, Silver, and Gold zones remain private and are not exposed directly to the browser.

### Geometry Delivery

`grid_geometry.geojson` remains the canonical exported presentation geometry. The browser-facing geometry is encoded as FlatGeobuf:

| Property | Value |
|----------|-------|
| Source GeoJSON size | 117.09 MB |
| FlatGeobuf size | 48.88 MB |
| Grid cells | 16,508 |
| CRS | EPSG:4326 |
| Coordinate precision | Unchanged |

FlatGeobuf is used because it preserves the exported geometry while providing a spatial index and HTTP range-based access. The frontend requests geometry for the current padded map viewport rather than loading the complete national geometry for every view.

### Monthly Data Delivery

Monthly data is stored separately from geometry:

```
months/risk_YYYY-MM.json
```

| Property | Value |
|----------|-------|
| First month | 2016-01 |
| Last month | 2025-12 |
| Total months | 120 |
| Grid cells per month | 16,508 |

Static attributes are stored in `grid_metadata.json`. The browser joins geometry and monthly attributes using `grid_cell_key`.

## Production Data Configuration

The frontend resolves GIS data through:

```
VITE_GIS_DATA_BASE_URL
```

Local development can use the Vite public directory backed by `dashboard/gis/data`. Production points to:

```
https://ccriskdevgisibu7j0.blob.core.windows.net/gis
```

### CORS

Azure Blob Storage allows browser access from:

- `http://localhost:5173`
- `http://127.0.0.1:5173`
- `https://icy-ground-0cf004a0f.7.azurestaticapps.net`

Relevant range-response headers are exposed to the browser.

## Production Validation

Production validation confirmed:

- Static Web App returns HTTP 200
- Manifest returns HTTP 200
- Monthly JSON returns HTTP 200
- FlatGeobuf byte-range request returns HTTP 206
- `Content-Range` is returned correctly
- Production CORS origin is accepted
- Viewport geometry loading works
- MapTiler basemap loads
- Layer switching works
- Month switching works
- AB / BC filtering works
- Hover interaction works
- Grid detail sheet works
- URL state survives reload
- Browser console contains no blocking runtime errors

## Infrastructure Management

Azure resources are provisioned with Terraform under:

```
infra/terraform/azure/
```

Terraform manages:

- Analytical ADLS Gen2 storage
- Public GIS serving storage
- GIS Blob container
- GIS storage RBAC
- Azure Static Web App

After deployment:

```
terraform plan
```

returns no infrastructure drift.
