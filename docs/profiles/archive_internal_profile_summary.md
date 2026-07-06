# Archive Internal Profile Summary

Generated at: `2026-05-12T00:10:41+00:00`

This summary inspects internal schemas inside complex Bronze archives such as nested shapefile packages and SQLite archives.

## Summary

- Profiled sources: `3`
- Missing Bronze runs: `0`
- Raw files missing: `0`
- Profile failures: `0`

## Sources

| Source | Status | Archive Type | Internal Objects | Key Contract Checks |
|---|---:|---:|---:|---|
| `census_boundaries` | `profiled` | `shapefile_zip` | `layers=2` | `province_layer_detected:True` `csd_layer_detected:True` `required_boundary_outputs:True` |
| `wildfire_history` | `profiled` | `shapefile_zip` | `layers=1` | `main_layer_detected:True` `event_id_field:True` `year_field:True` `date_field:True` `size_field:True` |
| `hydat_archive` | `profiled` | `sqlite_zip` | `tables=33` | `candidate_tables:True` `stations_table_detected:True` `daily_flow_or_level_detected:True` |

## Detail Notes

- Census boundaries are checked for province and CSD layers.
- Wildfire history is checked for event ID, year/date, and size candidate fields.
- HYDAT is checked for core hydrometric tables such as STATIONS, DLY_FLOWS, and DLY_LEVELS.
- This file validates that downloaded archives are usable for Silver standardization.
