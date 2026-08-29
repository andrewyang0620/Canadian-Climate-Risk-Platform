with property as (
    select *
    from {{ ref('stg_calgary_property_risk_context') }}
)
select 
    property_location_assessment_key,  -- property id
    source_parcel_id,
    assessment_year,  -- assignment context
    assessment_record_count,
    has_multiple_assessment_records,
    distinct_property_count,
    distinct_source_unique_key_count,
    assessed_value_total_sum,
    assessed_value_residential_sum,
    assessed_value_non_residential_sum,
    assessed_value_farmland_sum,
    assessment_class,
    assessment_class_count,
    has_multiple_assessment_classes,
    community_code,  -- local property context
    community_name,
    community_count,
    has_multiple_communities,
    land_use_designation,
    land_use_designation_count,
    has_multiple_land_use_designations,
    property_type,
    property_type_count,
    has_multiple_property_types,
    year_of_construction_min,
    year_of_construction_max,
    intersects_regulatory_flood_layer,  -- flood exposure
    is_flood_exposed,
    intersects_normal_river_channel,
    flood_zone_membership_count,
    flood_fringe_flag,
    flood_fringe_overlap_ratio,
    floodplain_flag,
    floodplain_overlap_ratio,
    floodway_flag,
    floodway_overlap_ratio,
    normal_river_channel_flag,
    normal_river_channel_overlap_ratio,
    overland_flow_flag,
    overland_flow_overlap_ratio,
    national_grid_cell_key,  -- national grid assignment
    national_grid_candidate_count,
    national_grid_overlap_ratio,
    has_national_grid_assignment
from property