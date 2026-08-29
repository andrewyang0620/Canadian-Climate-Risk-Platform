with permit as (

    select *
    from {{ ref('stg_calgary_building_permit_context') }}

)

select

    -- permit identity
    building_permit_key,
    city,
    source_permit_id,
    permit_number,

    -- permit classification
    permit_status,
    permit_type,
    permit_type_mapped,
    permit_class,
    permit_class_group,
    permit_class_mapped,
    work_class,
    work_class_group,
    work_class_mapped,

    -- time
    try_to_date(application_date) as application_date,
    try_to_date(issue_date) as issue_date,
    try_to_date(completed_date) as completed_date,

    issue_year::integer as issue_year,

    case
        when year_month is not null
        then try_to_date(year_month || '-01')
    end as issue_month,

    -- project context
    address_text,
    project_description,
    applicant_name,
    contractor_name,

    -- development / economic measures
    housing_units,
    housing_units_reported,
    housing_units_anomaly_flag,
    new_housing_units_created,
    creates_new_housing_units,
    estimated_project_cost,
    total_sqft,

    -- housing classification
    is_residential_permit,
    is_housing_related,
    housing_activity_type,

    -- location
    neighbourhood_code,
    neighbourhood_name,
    latitude,
    longitude,
    has_spatial_geometry,

    -- property mapping
    location_match_count,
    location_mapping_status,
    source_parcel_id,

    -- mapped assessment context
    assessment_year::integer as assessment_year,
    assessment_record_count,
    assessed_value_total_sum,
    assessed_value_residential_sum,
    assessed_value_non_residential_sum,
    assessed_value_farmland_sum,

    community_code,
    community_name,
    land_use_designation,
    property_type,

    year_of_construction_min::integer as year_of_construction_min,

    year_of_construction_max::integer as year_of_construction_max,

    -- flood context
    intersects_regulatory_flood_layer,
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
    overland_flow_overlap_ratio

from permit