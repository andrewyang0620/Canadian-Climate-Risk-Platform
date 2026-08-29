with permit as (
    select *
    from {{ ref('stg_vancouver_building_permit_context') }}
)
select
    building_permit_key,  -- permit id
    permit_number,
    try_to_date(issue_date) as issue_date,  -- time
    try_to_date(year_month || '-01') as issue_month,
    issue_year::integer as issue_year,
    address_text,  -- permit context
    project_description,
    permit_type_mapped,
    permit_class_group,
    permit_class_mapped,
    work_class_mapped,
    is_housing_related,  -- housing activity
    housing_activity_type,
    is_new_housing_building_permit,
    is_housing_renovation_permit,
    is_housing_demolition_permit,
    is_housing_salvage_abatement_permit,
    estimated_project_cost,  -- economic context
    neighbourhood_name,  -- local geography
    latitude,
    longitude,
    has_spatial_geometry,
    parcel_match_count,  -- parcel mapping
    parcel_mapping_status,
    property_parcel_key,
    is_flood_exposed,  -- flood exposure
    scenario_count,
    designated_floodplain_flag,
    designated_floodplain_overlap_ratio,
    fraser_risk_today_flag,
    fraser_risk_today_overlap_ratio,
    still_creek_floodplain_flag,
    still_creek_floodplain_overlap_ratio,
    wave_effect_zone_flag,
    wave_effect_zone_overlap_ratio,
    has_latest_assessment,  -- assessment mapping context
    assessment_mapping_ambiguous,
    assessment_mapping_exact_1_to_1
from permit