# Gold Spatial Grid Contract

## Purpose

The analytical grid is the stable spatial backbone for all downstream
climate-risk features, overlays, scores, and Gold marts.

## Coordinate systems

- Analysis CRS: EPSG:3347, NAD83 / Statistics Canada Lambert
- Display coordinates: EPSG:4326
- Grid generation and area calculations must occur in EPSG:3347
- All area values are stored in square kilometres

## Grid systems

| Grid system | Boundary | Cell size |
|---|---|---:|
| `bc_10km` | British Columbia | 10,000 m |
| `ab_10km` | Alberta | 10,000 m |
| `vancouver_1km` | Vancouver, BC | 1,000 m |
| `calgary_1km` | Calgary, AB | 1,000 m |

## Stable alignment

All grids use a global EPSG:3347 origin of `(0, 0)`.

Grid indices are calculated as:

- `grid_x_index = floor(easting / cell_size_m)`
- `grid_y_index = floor(northing / cell_size_m)`

The stable key format is:

`{grid_system}_x{grid_x_index}_y{grid_y_index}`

Keys must not depend on DataFrame order or run ID.

## Grain

`gold_grid_cell` has one row per analytical grid cell.

## Geometry contract

Each row stores:

- `full_cell_geometry_wkt`: complete square grid cell
- `analysis_geometry_wkt`: cell clipped to the province or city boundary
- `full_cell_area_sq_km`
- `analysis_area_sq_km`
- `boundary_coverage_ratio`
- `is_boundary_edge_cell`

Downstream area-based overlays must use `analysis_geometry_wkt`.

## Administrative identifiers

- `province_key`: canonical abbreviation, such as `AB` or `BC`
- `province_code`: Statistics Canada numeric code, such as `48` or `59`
- City boundaries are selected using exact municipality name plus
  `province_key`
- Vancouver: `municipality_name = Vancouver`, `province_key = BC`
- Calgary: `municipality_name = Calgary`, `province_key = AB`

## Geometry repair

Invalid polygonal boundaries are repaired with Shapely `make_valid`.

Repair status must be recorded. Non-polygonal fragments produced during
repair are excluded from area-based analysis.

## Versioning

The initial grid version is `v1`.

Any future change to cell alignment, cell size, clipping rules, CRS, or
identifier construction requires a new grid version.
