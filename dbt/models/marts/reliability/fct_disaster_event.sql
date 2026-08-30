with event as (
    select *
    from {{ ref('stg_disaster_event_reference') }}
)
select
    -- identity
    disaster_event_reference_key,
    source_disaster_event_key,
    source_row_number,
    source_name,

    -- event description / source context
    description,
    source_province_value,

    -- time
    reference_month,
    event_year,
    event_month_number,

    -- geography
    province_key,

    -- disaster classification
    normalized_event_type,
    normalized_event_subtype,
    disaster_domain,

    is_wildfire_domain_relevant,
    is_flood_domain_relevant,
    is_climate_domain_relevant,
    is_domain_relevant,

    -- location / mapping context
    location_text,
    location_text_normalized,
    location_tier,

    mapped_geo_level,
    mapped_geo_codes_json,
    mapping_method,
    mapping_confidence,

    -- validation eligibility
    is_grid_backtest_eligible,
    is_province_month_backtest_eligible,

    is_backtest_window,
    is_ab_bc_scope,
    is_backtest_eligible,

    -- impact measures
    estimated_total_cost_cad,
    normalized_total_cost_cad,
    fatalities_total,
    injured_total,
    evacuated_total,
    affected_total
from event