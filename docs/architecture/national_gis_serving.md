# Unified National and City GIS Serving Architecture

## Production

**GIS:** <https://gis.climate-risk.andrewjingtaoyang.dev>

One React/Vite application serves:

```text
National | Vancouver | Calgary
```

## Serving Architecture

```text
Private Bronze / Silver / Gold
        ↓
GIS export products
        ↓
Azure Blob Storage
FlatGeobuf + JSON
        ↓
React / Vite
MapLibre + deck.gl
MapTiler basemap and 3D buildings
        ↓
Azure Static Web Apps
        ↓
gis.climate-risk.andrewjingtaoyang.dev
```

Analytical tables are not exposed directly to the browser.

## National Contract

```text
gis/
├── manifest.json
├── grid_metadata.json
├── grid_geometry.fgb
└── months/risk_YYYY-MM.json
```

- 16,508 national 10 km cells.
- 120 monthly attribute files from 2016-01 to 2025-12.
- Join key: `grid_cell_key`.
- FlatGeobuf geometry loads by viewport with HTTP Range requests.
- Monthly JSON serves Composite, Climate, Hydro, Wildfire, confidence, ranking, and supporting fields.

National climate mapping semantics remain unchanged:

- 150 km station search radius.
- Existing IDW implementation.
- Existing minimum-station logic.
- Existing mapping semantics.

## City Contract

```text
gis/cities/
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

### Vancouver

- `parcels.fgb`: 99,726 parcels.
- `building_permits.fgb`: 50,239 spatial features from 50,610 source permits.

### Calgary

- `properties.fgb`: 410,049 property locations.
- `building_permits.fgb`: 488,369 spatial features from 489,276 source permits.
- `development_permits.fgb`: 189,745 spatial features from 190,399 source permits.
- `development_permit_property_links.json`: 210,659 DP to property links.

## Spatial Semantics

Canonical property grains:

```text
Vancouver -> property_parcel_key
Calgary   -> source_parcel_id
```

Each property receives one primary national 10 km grid using maximum-area overlap against full national cell geometry.

The national signal is contextual only. It is not parcel-level risk precision.

### Flood

Vancouver scenarios:

- Designated Floodplain
- Fraser Risk Today
- Still Creek Floodplain
- Wave Effect Zone

Calgary regulatory exposure:

- Flood Fringe
- Floodplain
- Floodway
- Overland Flow

`Normal River Channel` is context only.

### Permit relationships

Building Permits and Development Permits remain event-grain point layers.

```text
BP point -> mapped property when available
DP point -> zero, one, or many mapped properties
```

No nearest-property guessing is used.

Calgary DP logic:

- Single-property mappings use `single_source_parcel_id`.
- Multi-property mappings load `development_permit_property_links.json` on demand.
- The relationship JSON is cached in memory after first load.

## Rendering

```text
ground
-> property or flood polygon
-> MapTiler 3D buildings
-> BP / DP points
-> labels
```

3D buildings are physical context only.

Level-of-detail gates:

```text
property polygons -> zoom >= 12
activity points   -> zoom >= 13.5
```

City layers share one interleaved deck.gl overlay.

## City Layer Modes

```text
property
flood
building_permits
development_permits   # Calgary only
none
```

URL state preserves scope, date, national layer and region, and city layer mode.

## Production Resources

```text
Static Web App: ccrisk-dev-national-gis
Azure hostname: icy-ground-0cf004a0f.7.azurestaticapps.net
Custom domain: gis.climate-risk.andrewjingtaoyang.dev

Storage account: ccriskdevgisibu7j0
Container: gis
Base URL: https://ccriskdevgisibu7j0.blob.core.windows.net/gis
```

MapTiler hosts the National and City basemap styles.

## Validation

Production acceptance requires:

- National, Vancouver, and Calgary scopes load correctly.
- FGB requests return `206 Partial Content`.
- Blob CORS allows the production custom domain.
- National and city detail panels work.
- BP to property and Calgary DP to property interactions work.
- Browser Console has no blocking errors.

## IaC Note

The custom-domain Blob CORS origin must be represented in Terraform before an authoritative `terraform apply` so manual production configuration is not overwritten.
