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
