with observation as (
    select *
    from {{ ref('stg_grid_month_disaster_event_label') }}
)
select 
    -- grain / dimensions
    grid_month_disaster_label_key,
    grid_cell_key,
    reference_month,

    -- observation semantics
    label_is_observed,
    disaster_event_occurred,

    -- event counts
    disaster_event_count,
    wildfire_event_count,
    flood_event_count,
    storm_or_climate_event_count,
    climate_extreme_event_count,

    -- event lineage
    disaster_event_types,
    disaster_event_reference_keys_json,

    -- spatial-resolution lineage
    direct_cd_resolution_event_count,
    csd_parent_cd_event_count,
    cd_scope_event_count,
    cd_group_scope_event_count,
    csd_scope_event_count,

    -- approximation / quality
    approximate_event_count,
    low_overlap_event_count,
    has_csd_parent_cd_approximation,
    has_low_overlap_event,

    -- event-to-grid coverage quality
    minimum_event_grid_coverage_ratio,
    mean_event_grid_coverage_ratio,
    maximum_event_grid_coverage_ratio
from observation