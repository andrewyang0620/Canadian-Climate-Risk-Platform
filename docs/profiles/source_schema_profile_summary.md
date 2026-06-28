# Source Schema Profile Summary

Generated at: `2026-05-12T00:12:42+00:00`

This summary is generated from local Bronze raw files. It verifies raw schema, candidate join keys, coordinate fields, measurement fields, and downstream source contracts before Silver standardization.

## Summary

- Profiled sources: `13`
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

## Notes

- `required_fields` are checked against profiled raw columns.
- Candidate contracts are checked by case-insensitive normalized field matching.
- Large CSV row counts are exact only when profiling is run with `--count-rows`.
- This file should be reviewed before implementing Silver standardization logic.

## Pending Polygon/Basin Profiling Requirements

The current source profiling covers the existing registered sources. Two planned area-based source upgrades still require dedicated profiling before they can be registered in `configs/source_config.yml`.

### Hydro basin polygon profiling requirements

The Hydro basin polygon source must be profiled before implementation decisions are made.

Do not assume raw field names for station identity. Candidate field names must come from actual downloaded source schema inspection, not from guesswork.

The profiling output must report:

- raw file format and layer names
- raw column names and dtypes
- geometry type distribution
- source CRS
- geometry null count
- geometry validity rate
- station identifier candidate fields
- duplicate station identifier count
- join rate against `silver_hydro_station`
- join rate against `silver_hydro_daily`
- AB/BC intersection coverage

### Wildfire perimeter polygon profiling requirements

The NFDB perimeter polygon source must be profiled before implementation decisions are made.

Do not assume that `CFS_REF_ID` is equivalent to `NFDBFIREID` or the current `silver_wildfire_event.nfdb_fire_id`. Any join key must be proven through data profiling and match-rate audit.

The profiling output must report:

- raw file format and layer names
- raw column names and dtypes
- geometry type distribution
- source CRS
- geometry null count
- geometry validity rate
- fire identity candidate fields
- year/date candidate fields
- size/area candidate fields
- duplicate identity candidate counts
- join rate against `silver_wildfire_event`
- AB/BC intersection coverage
- unmatched polygon count
- unmatched point/event count
