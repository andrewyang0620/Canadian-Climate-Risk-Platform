with permit as (
    select *
    from {{ ref('stg_calgary_development_permit_context') }}
)
select 
    -- permit identity
    development_permit_key,
    source_permit_id,
    permit_number,

    -- planning / development context
    category,
    description,
    proposed_use_code,
    proposed_use_description,
    permitted_discretionary,

    land_use_district,
    land_use_district_description,

    status_current,
    decision,
    decision_by,

    -- dates
    applied_date::date as applied_date,
    decision_date::date as decision_date,
    release_date::date as release_date,
    must_commence_date::date as must_commence_date,
    canceled_refused_date::date as canceled_refused_date,

    applied_year::integer as applied_year,
    decision_year::integer as decision_year,

    -- appeal context
    sdab_number,
    sdab_hearing_date::date as sdab_hearing_date,
    sdab_decision,
    sdab_decision_date::date as sdab_decision_date,

    -- location
    address_text,
    community_code,
    community_name,
    ward,
    quadrant,
    latitude,
    longitude,

    -- source / spatial mapping quality
    source_location_count,
    source_titled_parcel_count,
    unique_source_point_count,

    exact_point_match_count,
    ambiguous_point_match_count,
    unmatched_point_count,

    mapped_property_location_count,
    location_mapping_status,
    has_partial_spatial_mapping,

    single_source_parcel_id,

    -- mapped assessment context
    mapped_assessed_value_total_sum,
    mapped_assessed_value_residential_sum,
    mapped_assessed_value_non_residential_sum,
    mapped_assessed_value_farmland_sum,

    -- mapped flood context
    flood_exposed_property_location_count,
    regulatory_property_location_count,
    normal_river_channel_property_location_count,

    is_flood_exposed,
    intersects_regulatory_flood_layer,
    intersects_normal_river_channel
from permit