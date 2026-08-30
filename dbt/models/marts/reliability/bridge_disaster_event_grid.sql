with scope as (
    select * 
    from {{ ref('stg_disaster_event_grid_scope') }}
)
select
    -- relationship identity
    event_grid_scope_key,
    disaster_event_reference_key,
    source_disaster_event_key,

    -- event context
    reference_month,
    event_year,
    event_month_number,
    province_key,
    disaster_domain,
    location_text,
    location_tier,

    -- grid identity
    grid_cell_key,
    grid_system,
    grid_province_key,

    -- grid geometry measures
    grid_analysis_area_sq_km,
    grid_geometry_area_sq_km,

    -- upstream CD / mapping lineage
    matched_census_division_keys_json,
    matched_census_division_count,
    source_event_cd_scope_keys_json,
    source_mapped_geo_levels_json,
    resolution_methods_json,
    mapping_confidences_json,
    mapping_methods_json,

    -- affected-area relationship
    affected_overlap_area_sq_km,
    affected_grid_coverage_ratio,
    maximum_single_cd_coverage_ratio,

    -- mapping / eligibility semantics
    is_csd_to_cd_approximation,
    is_backtest_window,
    is_ab_bc_scope,
    is_domain_relevant,
    is_grid_backtest_eligible
from scope