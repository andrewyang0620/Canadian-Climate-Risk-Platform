# Data Sources

This document tracks all national, provincial, and municipal public datasets used by the platform, including source URL, update frequency, file format, spatial grain, ingestion method, and downstream mart usage.

## Registered Polygon/Basin Source Upgrades

Grid-level modeling should prefer area-based spatial footprints over point-only allocation when reliable footprints are available. The following two source upgrades are now registered and profiled.

| Source ID | Source | Spatial role | Bronze output | Silver output |
|---|---|---|---|---|
| `national_hydrometric_basin_polygons` | National Hydrometric Network Basin Polygons | Hydrometric drainage basin polygons, pour points, and station points | `bronze_hydro_basin_polygon` | `silver_hydro_basin_polygon`, `silver_hydro_basin_pour_point`, `silver_hydro_basin_station_point` |
| `wildfire_perimeter_polygons` | Canadian National Fire Database `NFDB_poly` | Wildfire perimeter / burned-area polygons | `bronze_wildfire_perimeter_polygon` | `silver_wildfire_perimeter_polygon` |

### National Hydrometric Network Basin Polygons

The National Hydrometric Network Basin Polygons source was added as a Hydro spatial-footprint source for grid-level hydro allocation.

This source does not replace HYDAT daily flow/level observations. HYDAT remains the measurement source. The basin polygon package provides spatial drainage-basin, pour-point, and station-point geometry keyed by `StationNum`, which is standardized to `station_id`.

Project-scope chunks:

- MDA_ADP_05.zip
- MDA_ADP_06.zip
- MDA_ADP_07.zip
- MDA_ADP_08.zip
- MDA_ADP_09.zip
- MDA_ADP_10.zip
- MDA_ADP_11.zip

Silver outputs:

- `silver_hydro_basin_polygon`
- `silver_hydro_basin_pour_point`
- `silver_hydro_basin_station_point`

Validated full project-scope Silver output:

- polygon rows: 5,071
- pour point rows: 5,071
- station point rows: 5,071
- existing `silver_hydro_station` match count: 3,212 / 3,428
- match rate: 93.70%
- AB match rate: 87.14%
- BC match rate: 96.82%

Geometry handling:

- source CRS: EPSG:4326
- downstream processing CRS: EPSG:3347
- original invalid polygon count: 1,573
- repaired polygon count: 1,573
- final valid geometry count: 5,071
- final geometry types: Polygon and MultiPolygon

Interpretation: unmatched hydro stations are retained in existing Hydro Silver tables. Downstream Gold hydro logic must use basin polygons where available and a documented fallback strategy for stations without basin polygon coverage.

### Wildfire perimeter polygons - NFDB_poly

Source: Canadian National Fire Database fire polygon data.

This source is registered separately from the existing NFDB point wildfire history source.

Bronze source:

- source name: `wildfire_perimeter_polygons`
- raw package: `NFDB_poly.zip`
- target Bronze table: `bronze_wildfire_perimeter_polygon`

Silver output:

- target Silver table: `silver_wildfire_perimeter_polygon`
- Silver grain: one NFDB wildfire perimeter polygon record for BC/AB
- Silver geographic scope: BC and AB
- Silver historical scope: full available BC/AB source history, 1972-2024
- Gold/modeling window: downstream feature layers may filter to 2016-2025

Latest source profile:

- raw NFDB polygon rows: 48,571
- BC/AB Silver rows: 14,527
- AB rows: 4,805
- BC rows: 9,722
- BC/AB rows in 2016-2025 feature window: 5,054
- source files:
  - `NFDB_poly_1972to2020_20250630.shp`: 11,722 BC/AB Silver rows
  - `NFDB_poly_2021to2024_20250630.shp`: 2,805 BC/AB Silver rows

Design note:
The wildfire perimeter polygon source does not replace `silver_wildfire_event`. The existing point source remains the wildfire event occurrence source. The polygon source provides burned-area/perimeter geometry for future grid-intersection and exposure features.
