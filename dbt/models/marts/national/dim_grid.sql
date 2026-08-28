with grid as (
    select *
    from {{ ref('stg_grid_cell') }}
)
select
    grid_cell_key,

    -- grid identity
    grid_system,
    grid_level,
    grid_version,

    -- province
    province_key,
    province_code,
    province_name,

    -- grid definition
    boundary_year,
    cell_size_m,
    grid_x_index,
    grid_y_index,

    -- projected bounds / centroid
    cell_min_x,
    cell_min_y,
    cell_max_x,
    cell_max_y,
    centroid_x,
    centroid_y,

    -- display coordinates
    centroid_longitude,
    centroid_latitude,

    -- area / boundary context
    full_cell_area_sq_km,
    analysis_area_sq_km,
    boundary_coverage_ratio,
    is_boundary_edge_cell,

    -- geometry metadata
    analysis_geometry_type,
    analysis_geometry_wkt,
    crs_epsg,

    -- source metadata
    source_boundary_key,
    source_boundary_type,
    source_boundary_geometry_repaired

from grid