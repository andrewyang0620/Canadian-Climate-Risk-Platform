# Gold Wildfire Perimeter Grid-Month Feature Contract
 
## Table
 
`gold_grid_month_wildfire_perimeter_feature`
 
## Purpose
 
This Gold table converts NFDB wildfire perimeter polygons into monthly 10km grid-level
burned-area exposure features.
 
It uses the polygon source as the authoritative geometry source for observed wildfire
perimeter overlap. It does not depend on the legacy NFDB point wildfire event source.
 
## Grain
 
One row per:
 
{fence}text
grid_cell_key x reference_month
{fence}
 
## Scope
 
**Spatial scope:**
 
- `grid_system in ('ab_10km', 'bc_10km')`
 
**Temporal scope:**
 
- 2016-01 through 2025-12
 
**Expected skeleton:**
 
- 16,508 grid cells x 120 months = 1,980,960 rows
 
## Inputs
 
### Gold grid
 
`gold_grid_cell`
 
Required fields include:
 
- `grid_cell_key`
- `grid_system`
- `grid_level`
- `grid_version`
- `province_key`
- `analysis_area_sq_km`
- `analysis_geometry_wkt`
- `crs_epsg`
 
Only `ab_10km` and `bc_10km` are used.
 
### Silver wildfire perimeter polygons
 
`silver_wildfire_perimeter_polygon`
 
Required fields include:
 
- `wildfire_perimeter_key`
- `province`
- `fire_year`
- `fire_month`
- `source_size_ha`
- `calculated_size_ha`
- `fire_cause`
- `prescribed`
- `geometry_wkt`
- `geometry_is_valid`
- `source_crs`
 
Silver keeps BC/AB full source history. Gold filters to the 2016-2025 feature window.
 
## Geometry method
 
All intersection calculations are performed in EPSG:3347.
 
The grid uses `analysis_geometry_wkt`, not `full_cell_geometry_wkt`, so edge cells are
clipped to the province boundary.
 
Wildfire perimeter geometries are transformed from their source CRS to EPSG:3347 before
intersection.
 
## Temporal assignment
 
Monthly assignment uses polygon-native `fire_year` and `fire_month`.
 
A polygon enters monthly aggregation only when:
 
- `2016 <= fire_year <= 2025`
- `fire_month` between 1 and 12
 
Observed source profile:
 
- feature-window polygons: 5,054
- monthly-assignable polygons: 4,939
- missing/invalid `fire_month` polygons: 115
- monthly-assignable rate: 97.7246%
 
The 115 polygons with missing or invalid month are not forced into a monthly bucket.
They remain available in Silver but are excluded from this monthly Gold table.
 
## Output fields
 
**Identity fields:**
 
- `wildfire_grid_month_key`
- `grid_cell_key`
- `grid_system`
- `grid_level`
- `grid_version`
- `province_key`
- `reference_month`
- `crs_epsg`
- `grid_analysis_area_sq_km`
 
**Wildfire exposure fields:**
 
- `wildfire_perimeter_count`
- `wildfire_intersection_area_sq_km`
- `wildfire_intersection_area_ha`
- `wildfire_intersection_area_ratio_of_grid`
 
**Non-additive reference fields:**
 
- `wildfire_max_source_size_ha`
- `wildfire_max_calculated_size_ha`
 
**Cause bucket fields:**
 
- `wildfire_cause_n_polygon_count`
- `wildfire_cause_h_polygon_count`
- `wildfire_cause_u_polygon_count`
- `wildfire_cause_prescribed_burn_polygon_count`
- `wildfire_cause_other_polygon_count`
 
**Flag and method fields:**
 
- `wildfire_has_observed_perimeter_overlap`
- `wildfire_temporal_assignment_method`
 
## Zero semantics
 
This table uses a complete grid-month skeleton.
 
For wildfire perimeter metrics:
 
- `0` = no observed NFDB polygon perimeter overlap for this grid-month
 
This does not mean zero physical wildfire risk. It only means no observed perimeter
overlap in the available NFDB polygon source.
 
This differs from some climate or hydro feature tables where null can mean missing
measurement coverage.
 
## Additivity rules
 
**Safe to aggregate across grid cells and months:**
 
- `wildfire_intersection_area_ha`
- `wildfire_intersection_area_sq_km`
- `wildfire_perimeter_count`
- `wildfire_cause_*_polygon_count`
 
Use caution with `wildfire_perimeter_count`: one wildfire polygon can intersect multiple
grid cells, so cross-grid sums count grid-polygon intersections, not unique fire events.
 
**Not additive across grid cells:**
 
- `wildfire_max_source_size_ha`
- `wildfire_max_calculated_size_ha`
 
These are reference metrics for the largest original fire polygon intersecting the
grid-month. They must not be summed across grid cells.
 
**Intentionally omitted fields:**
 
- `wildfire_total_source_size_ha`
- `wildfire_total_calculated_size_ha`
 
They are excluded because source fire area would be repeated across every intersecting
grid cell and could cause double counting.
 
## Cause semantics
 
Polygon-native `fire_cause` is used directly.
 
Observed feature-window polygon cause profile:
 
- `N` = 3,053
- `H` = 1,601
- `U` = 400
 
Gold intersection-level cause totals can exceed original polygon counts because one
polygon can intersect multiple grid cells.
 
Prescribed burn is handled separately using the `prescribed` field and special cause
values such as `H-PB`.
 
## Point wildfire source relationship
 
This Gold table does not join to `silver_wildfire_event`.
 
The legacy point source remains useful for event-occurrence features, such as event
counts, proximity, or density. The polygon source is used for burned-area/perimeter
overlap.
 
Measured audit result in the 2016-2025 feature window:
 
- point rows: 25,680
- polygon rows: 5,054
- best linkage: `poly_fire_id_to_point_source_fire_id`
- best linkage match rate: 99.25%
 
Despite the strong match rate, this perimeter Gold feature does not require point
enrichment because polygon-native fields are sufficient for geometry, month assignment,
and cause bucketing.
 
## Latest validation result
 
Latest successful run:
 
- rows: 1,980,960
- grid cells: 16,508
- months: 120
- nonzero grid-months: 8,242
- total intersection area ha: 11,625,500.8334
- max intersection area ha: 10,000.0
- max ratio of grid: 1.0
- rows ratio > 1: 0
 
Validation checks passed:
 
- required columns
- complete grid-month skeleton
- key uniqueness
- grid scope
- reference month range
- CRS
- metric quality
- cause count consistency
- overlap flag consistency
- zero semantics