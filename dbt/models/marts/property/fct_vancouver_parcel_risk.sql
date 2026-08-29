with parcel as (
    select *
    from {{ ref('stg_vancouver_parcel_risk_context') }}
)
select
    property_parcel_key,  -- parcel id
    city,
    province,
    source_parcel_id,
    source_tax_coord,
    address_text,
    is_flood_exposed,  -- floor exposure
    scenario_count,
    designated_floodplain_flag,
    designated_floodplain_overlap_ratio,
    fraser_risk_today_flag,
    fraser_risk_today_overlap_ratio,
    still_creek_floodplain_flag,
    still_creek_floodplain_overlap_ratio,
    wave_effect_zone_flag,
    wave_effect_zone_overlap_ratio,
    has_latest_assessment,  -- assessment context
    assessment_mapping_ambiguous,
    assessment_mapping_exact_1_to_1,
    report_year,
    land_coordinate_current_land_value,
    land_coordinate_current_improvement_value,
    land_coordinate_current_total_assessed_value,
    exact_mapped_current_land_value,
    exact_mapped_current_improvement_value,
    exact_mapped_current_total_assessed_value,
    zoning_district,  -- local property context
    zoning_classification,
    neighbourhood_code,
    national_grid_cell_key,  -- national grids
    national_grid_candidate_count,
    national_grid_overlap_ratio,
    national_grid_cell_key is not null as has_national_grid_assignment
from parcel