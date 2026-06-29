# Source Schema Profile Summary

Generated at: `2026-05-12T00:12:42+00:00`

This summary is generated from local Bronze raw files. It verifies raw schema, candidate join keys, coordinate fields, measurement fields, and downstream source contracts before Silver standardization.

## Summary

- Profiled sources: `15`
- Missing Bronze runs: `0`
- Raw files missing: `0`
- Profile failures: `0`

## Sources

| Source | Status | File Type | Rows / Features | Columns | Contract Notes |
|---|---:|---:|---:|---:|---|
| `eccc_historical_climate` | `profiled` | `jsonl_gzip` | `171108` | `40` | `required_fields:True` `coordinate_contract:True` `climate_measurement_contract:True` |
| `hydat_archive` | `profiled` | `zip_archive` | `` | `1` | `required_fields:True` `measurement_contract:None` |
| `wildfire_history` | `profiled` | `zip_archive` | `` | `1` | `required_fields:True` |
| `census_boundaries` | `profiled` | `zip_archive` | `` | `1` | `required_fields:True` |
| `canadian_disaster_database` | `profiled` | `excel` | `1490` | `25` | `required_fields:True` `location_mapping_contract:True` |
| `vancouver_property_parcels` | `profiled` | `geojson` | `99726` | `6` | `required_fields:True` `identity_contract:True` |
| `vancouver_property_tax` | `profiled` | `csv` | `1552486` | `30` | `required_fields:True` `join_contract:True` |
| `vancouver_building_permits` | `profiled` | `csv` | `50610` | `20` | `required_fields:True` `join_contract:True` `coordinate_contract:True` |
| `vancouver_floodplain` | `profiled` | `geojson` | `8` | `7` | `required_fields:True` |
| `calgary_property_assessment` | `profiled` | `csv` | `604955` | `22` | `required_fields:True` `identity_contract:True` `coordinate_contract:True` |
| `calgary_flood_hazard` | `profiled` | `geojson` | `1144` | `4` | `required_fields:True` |
| `calgary_building_permits` | `profiled` | `csv` | `490102` | `30` | `required_fields:True` `join_contract:True` `coordinate_contract:True` |
| `calgary_development_permits` | `profiled` | `csv` | `190399` | `35` | `required_fields:True` `join_contract:True` `coordinate_contract:True` |
| `national_hydrometric_basin_polygons` | `profiled` | `zip_geojson_package` | `5071 per layer` | `varies` | `required_fields:True` `geometry_contract:True` `layer_alignment:True` `station_join_contract:True` |
| `wildfire_perimeter_polygons` | `profiled` | `zip_shapefile` | `48571` | `varies` | `required_fields:True` `geometry_contract:True` `bc_ab_filter:True` `key_contract:True` |

## Notes

- `required_fields` are checked against profiled raw columns.
- Candidate contracts are checked by case-insensitive normalized field matching.
- Large CSV row counts are exact only when profiling is run with `--count-rows`.
- This file should be reviewed before implementing Silver standardization logic.

## Polygon/Basin Profiling Notes

The two area-based source upgrades are now registered and profiled. Remaining work is downstream allocation and join-policy validation, not raw-schema discovery.

Hydro basin polygons use `StationNum` as the source station identifier and standardize it to `station_id`. The profile confirms layer alignment across basin polygons, pour points, and station points, plus measured coverage against existing Hydro Silver station records.

Wildfire perimeter polygons preserve `CFS_REF_ID` as a natural source identifier and use a lineage-safe Silver primary key. Do not assume that `CFS_REF_ID`, `FIRE_ID`, `SOURCE_KEY`, `NFDBFIREID`, or current `silver_wildfire_event.nfdb_fire_id` values are equivalent without a separate match-rate audit.

## National Hydrometric Network Basin Polygons profile

Schema probe confirmed that each project-scope MDA_ADP GeoJSON chunk contains three aligned layers:

1. `DrainageBasin_BassinDeDrainage`
   - geometry: Polygon / MultiPolygon after repair
   - target Silver table: `silver_hydro_basin_polygon`
   - key field: `StationNum`
   - standardized key: `station_id`
   - area fields: `Area_km2`, `Aire_km2`
   - revision fields: `Version`, `Date_rev`

2. `PourPoint_PointExutoire`
   - geometry: Point
   - target Silver table: `silver_hydro_basin_pour_point`
   - key field: `StationNum`
   - province field: `ProvTerr`

3. `Station`
   - geometry: Point
   - target Silver table: `silver_hydro_basin_station_point`
   - key field: `StationNum`
   - province field: `ProvTerr`
   - HYDAT version field: `HYDAT_ver`

Full project-scope profile:

- total station IDs across each layer: 5,071
- layer alignment: polygon, pour point, and station point station ID sets are identical
- MDA_ADP region counts:
  - 05: 1,740
  - 06: 164
  - 07: 476
  - 08: 2,180
  - 09: 87
  - 10: 258
  - 11: 166
- existing `silver_hydro_station` match count: 3,212
- existing `silver_hydro_station` unmatched count: 216

Station ID note: the source includes standard WSC station IDs such as `11AA001` and extended/test or auxiliary IDs such as `08HDX03` and `08HDX05`. Silver validation accepts both forms while preserving the source station identifier.

## Wildfire perimeter polygon source profile - NFDB_poly

Source package:

- `NFDB_poly.zip`
- raw size observed in Bronze: 778,498,701 bytes
- source shapefiles:
  - `NFDB_poly_1972to2020_20250630.shp`
  - `NFDB_poly_2021to2024_20250630.shp`

Raw profile:

- total raw polygon rows: 48,571
- 1972-2020 shapefile rows: 41,210
- 2021-2024 shapefile rows: 7,361

BC/AB Silver profile:

- total Silver rows: 14,527
- AB rows: 4,805
- BC rows: 9,722
- year range: 1972-2024
- 2016-2025 downstream feature-window rows: 5,054

Source-file contribution after BC/AB Silver filter:

- `NFDB_poly_1972to2020_20250630.shp`: 11,722 rows
- `NFDB_poly_2021to2024_20250630.shp`: 2,805 rows

Silver fields preserve:

- source identifiers: `cfs_ref_id`, `source_fire_id`, `source_key`
- temporal fields: `fire_year`, `fire_month`, `fire_day`, `report_date`, `out_date`, `polygon_date`, `acquired_date`
- size fields: `source_size_ha`, `calculated_size_ha`
- cause and mapping fields: `fire_cause`, `prescribed`, `map_source`, `map_method`
- geometry audit fields: `geometry_original_is_valid`, `geometry_was_repaired`, `geometry_is_valid`
- lineage fields: `source_file`, `source_record_number`, `source_name`, `source_layer`

Validation result:

- required columns: passed
- non-empty table: passed
- primary key uniqueness: 14,527 / 14,527
- null or blank `cfs_ref_id`: 0
- province filter: AB/BC only
- fire year presence: passed
- geometry repaired count: 143
- invalid geometry after repair: 0
- geometry types: Polygon, MultiPolygon
- source CRS: `NAD_1983_Lambert_Conformal_Conic`

Key policy:
`wildfire_perimeter_key` is a lineage-safe Silver primary key. `CFS_REF_ID` is preserved as a natural source identifier, but polygon keys must not be assumed equivalent to existing `silver_wildfire_event` point-event keys without separate join profiling.
