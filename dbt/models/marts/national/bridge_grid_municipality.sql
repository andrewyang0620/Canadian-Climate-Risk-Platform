with bridge as (
    select *
    from {{ ref('stg_grid_municipality_bridge') }}
)
select
    grid_municipality_bridge_key,

    -- relationship keys
    grid_cell_key,
    municipality_key,

    -- municipality attributes
    municipality_name,
    municipality_type,
    municipality_province_code,
    municipality_province_name,
    municipality_boundary_year,

    -- spatial relationship
    grid_analysis_area_sq_km,
    municipality_area_sq_km,
    intersection_area_sq_km,
    grid_coverage_ratio,
    municipality_coverage_ratio,

    -- relationship semantics
    is_primary_municipality,
    municipality_match_count,

    -- spatial quality / lineage
    municipality_geometry_repaired,
    spatial_join_method,
    crs_epsg
from bridge