# Source Schema Profile Summary

Generated at: `2026-05-11T20:26:41+00:00`

This summary is generated from local Bronze raw files. It verifies raw schema, candidate join keys, coordinate fields, measurement fields, and downstream source contracts before Silver standardization.

## Summary

- Profiled sources: `1`
- Missing Bronze runs: `0`
- Raw files missing: `0`
- Profile failures: `0`

## Sources

| Source | Status | File Type | Rows / Features | Columns | Contract Notes |
|---|---:|---:|---:|---:|---|
| `hydat_archive` | `profiled` | `zip_archive` | `` | `1` | `required_fields:True` `measurement_contract:None` |

## Notes

- `required_fields` are checked against profiled raw columns.
- Candidate contracts are checked by case-insensitive normalized field matching.
- Large CSV row counts are exact only when profiling is run with `--count-rows`.
- This file should be reviewed before implementing Silver standardization logic.
