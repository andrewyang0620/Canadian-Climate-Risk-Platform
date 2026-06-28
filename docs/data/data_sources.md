# Data Sources

This document tracks all national, provincial, and municipal public datasets used by the platform, including source URL, update frequency, file format, spatial grain, ingestion method, and downstream mart usage.

## Planned Polygon/Basin Source Upgrade

Two source upgrades are planned because grid-level modeling should prefer area-based spatial footprints over point-only allocation when available.

### Hydro basin polygons

Candidate source:

- National Hydrometric Network Basin Polygons

Purpose:

- provide drainage basin polygons for hydrometric stations
- support basin-to-grid intersection for future hydro Gold logic
- keep HYDAT station and daily observations as the measurement source

Branch 19 decision:

- do not register this source in `configs/source_config.yml` yet
- do not assume station key field names yet
- do not set match-rate thresholds yet

Required future audit in the Hydro basin Bronze/Silver branch:

- inspect actual raw schema
- confirm station identifier field
- standardize geometry and CRS
- measure join rate against `silver_hydro_station`
- measure join rate against `silver_hydro_daily`
- measure AB/BC grid intersection coverage

### Wildfire perimeter polygons

Candidate source:

- NFDB fire perimeter polygons

Purpose:

- provide fire perimeter polygons for burned-area grid features
- replace point-count / nearest-fire allocation in future Gold wildfire logic if quality is sufficient
- keep existing `silver_wildfire_event` as a legacy point/event reference source until polygon quality and join rate are validated

Branch 19 decision:

- do not register this source in `configs/source_config.yml` yet
- do not assume polygon-to-point join keys yet
- do not assume `CFS_REF_ID` is equivalent to `NFDBFIREID` / `nfdb_fire_id`
- do not set match-rate thresholds yet

Required future audit in the Wildfire polygon Bronze/Silver branch:

- inspect actual polygon schema
- confirm available fire identity fields
- confirm date/year fields
- confirm size/area fields
- standardize geometry and CRS
- measure AB/BC polygon coverage
- measure join rate against `silver_wildfire_event`
- report unmatched polygon and point/event records separately

## National Hydrometric Network Basin Polygons

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
